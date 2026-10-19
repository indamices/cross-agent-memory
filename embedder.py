"""Embedding wrapper supporting Ollama and local sentence-transformers backends."""

import hashlib
import json
import logging
import time
import urllib.request
from collections import OrderedDict
from typing import Optional

from config import EMBEDDING

logger = logging.getLogger("memory-service.embedder")

# Ollama 永远是本地服务,绝不能走代理。
# cron / launchd 上下文常残留 HTTP_PROXY 却没有 NO_PROXY=localhost,
# 会导致 urllib 把 localhost:11434 也塞进代理 -> HTTP 502 Bad Gateway。
# 用一个显式禁用代理的 opener,使本地调用不依赖环境变量是否设对。
_DIRECT_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))

_cache: OrderedDict[str, list[float]] = OrderedDict()
_cache_hits: int = 0
_cache_misses: int = 0

_local_model = None


def _get_local_model():
    global _local_model
    if _local_model is None:
        from sentence_transformers import SentenceTransformer
        logger.info(f"Loading local model: {EMBEDDING.local_model}")
        _local_model = SentenceTransformer(EMBEDDING.local_model)
        logger.info(
            f"Local model loaded. dim={_local_model.get_sentence_embedding_dimension()}"
        )
    return _local_model


def _cache_key(text: str, model: str) -> str:
    text_hash = hashlib.md5(text.encode()).hexdigest()[:16]
    return f"{model}:{text_hash}"


def _store_in_cache(key: str, embedding: list[float]) -> None:
    global _cache_hits, _cache_misses
    _cache[key] = embedding
    if len(_cache) > EMBEDDING.cache_max_size:
        _cache.popitem(last=False)


def _choose_model(text: str) -> str:
    if EMBEDDING.backend == "local":
        return EMBEDDING.local_model
    if len(text) > EMBEDDING.long_threshold:
        return EMBEDDING.long_text_model
    return EMBEDDING.short_text_model


def _local_embed_single(text: str) -> list[float]:
    model = _get_local_model()
    embedding = model.encode(text, normalize_embeddings=True)
    return embedding.tolist()


def _local_embed_batch(texts: list[str]) -> list[list[float]]:
    model = _get_local_model()
    embeddings = model.encode(
        texts,
        normalize_embeddings=True,
        batch_size=EMBEDDING.local_batch_size,
        show_progress_bar=False,
    )
    return [e.tolist() for e in embeddings]


def _ollama_embed_single(text: str, model: str) -> list[float]:
    payload = json.dumps({"model": model, "input": [text]}).encode("utf-8")
    req = urllib.request.Request(
        f"{EMBEDDING.ollama_url}/api/embed",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    with _DIRECT_OPENER.open(req, timeout=EMBEDDING.timeout) as resp:
        data = json.loads(resp.read().decode("utf-8"))
        return data["embeddings"][0]


def _ollama_embed_batch(texts: list[str], model: str) -> list[list[float]]:
    payload = json.dumps({"model": model, "input": texts}).encode("utf-8")
    req = urllib.request.Request(
        f"{EMBEDDING.ollama_url}/api/embed",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    with _DIRECT_OPENER.open(req, timeout=EMBEDDING.timeout * 3) as resp:
        data = json.loads(resp.read().decode("utf-8"))
        return data["embeddings"]


def embed_single(text: str, model: Optional[str] = None) -> list[float]:
    global _cache_hits, _cache_misses

    model = model or _choose_model(text)
    key = _cache_key(text, model)

    if key in _cache:
        _cache.move_to_end(key)
        _cache_hits += 1
        logger.debug(f"Cache hit for key {key[:24]}...")
        return _cache[key]

    _cache_misses += 1

    try:
        if EMBEDDING.backend == "local":
            result = _local_embed_single(text)
        else:
            result = _ollama_embed_single(text, model)
        _store_in_cache(key, result)
        return result
    except Exception as e:
        logger.error(f"Embedding failed (model={model}): {e}")
        raise


def embed_batch(texts: list[str], model: Optional[str] = None) -> list[list[float]]:
    global _cache_hits, _cache_misses

    model = model or _choose_model(texts[0] if texts else EMBEDDING.local_model)

    results: list[list[float] | None] = [None] * len(texts)
    miss_indices: list[int] = []
    miss_texts: list[str] = []

    for i, text in enumerate(texts):
        key = _cache_key(text, model)
        if key in _cache:
            _cache.move_to_end(key)
            _cache_hits += 1
            results[i] = _cache[key]
        else:
            miss_indices.append(i)
            miss_texts.append(text)

    if not miss_texts:
        return [r if r is not None else [] for r in results]

    _cache_misses += len(miss_texts)

    # Process in sub-batches to avoid Ollama timeout on large batches
    sub_batch_size = max(1, min(4, len(miss_texts)))
    try:
        if EMBEDDING.backend == "local":
            new_embeddings = _local_embed_batch(miss_texts)
            for j, emb in enumerate(new_embeddings):
                idx = miss_indices[j]
                results[idx] = emb
                key = _cache_key(miss_texts[j], model)
                _store_in_cache(key, emb)
        else:
            new_embeddings = []
            for start in range(0, len(miss_texts), sub_batch_size):
                sub_batch = miss_texts[start:start + sub_batch_size]
                if start > 0:
                    time.sleep(0.5)  # throttle between sub-batches
                batch_result = _ollama_embed_batch(sub_batch, model)
                new_embeddings.extend(batch_result)
            for j, emb in enumerate(new_embeddings):
                idx = miss_indices[j]
                results[idx] = emb
                key = _cache_key(miss_texts[j], model)
                _store_in_cache(key, emb)
    except Exception as e:
        logger.error(
            f"Batch embedding failed (model={model}, n={len(miss_texts)}): {e}"
        )
        _fallback_batch_with_circuit_breaker(results, miss_indices, miss_texts, model)

    return [r if r is not None else [] for r in results]


# P1-3 (GLM-5.3 复审): 回退熔断 — ollama 半死(accept 不答)时逐条回退 60s×N 会占死全部 worker 数小时.
# 机制: 首批探测 ≤FALLBACK_PROBE_MAX 条, 连续失败即断路 (余下返回 [] 不逐条试).
_FALLBACK_PROBE_MAX = 5
_fallback_consec_fail = 0
_FALLBACK_HALFOPEN_COOLDOWN = 600  # 断路后 10min 冷却, 期满复位放行整批 ≤5 条探测 (GLM复审必修2: 防永久断路无自愈)
_fallback_open_ts = 0.0
_fallback_need_halfopen = False


def _fallback_batch_with_circuit_breaker(results: list, miss_indices: list, miss_texts: list, model: str) -> None:
    """批量失败回退: 探测限次 + 连败断路 + half-open 自愈 (convention: 防无界时长).

    REJECT重修2 (GLM-5.3 2026-08-15): 状态机四态 —
      CLOSED(正常) → TRIPPED(断路, 记 open_ts) → HALF(冷却中拦截) → 到期复位回 CLOSED.
    两处 trip 点 (全探失败/续跑 break) 必须都置 halfopen+open_ts, 否则无自愈.
    """
    global _fallback_consec_fail, _fallback_open_ts, _fallback_need_halfopen
    import time as _t

    # ── 入口闸: 断路态 (consec>=5) 统一处理 ──
    if _fallback_consec_fail >= _FALLBACK_PROBE_MAX:
        if _fallback_need_halfopen:
            if _t.time() - _fallback_open_ts >= _FALLBACK_HALFOPEN_COOLDOWN:
                # 冷却期满 → 复位放行 (真自愈)
                _fallback_need_halfopen = False
                _fallback_consec_fail = 0
                logger.info("[embed] 断路冷却期满 → 复位连败计数, 放行探测")
            else:
                # 冷却期内 → 拦截, 零探测 (P1-3 防占死 worker 的核心)
                remain = int(_FALLBACK_HALFOPEN_COOLDOWN - (_t.time() - _fallback_open_ts))
                logger.warning(f"[embed] 断路冷却中 ({remain}s 后 half-open) — {len(miss_texts)} 条直接返回空, 零探测")
                for j in range(len(miss_texts)):
                    results[miss_indices[j]] = []
                return
        else:
            # 首次进断路态: 记 open_ts + 拦截
            _fallback_need_halfopen = True
            _fallback_open_ts = _t.time()
            logger.error(f"[embed] 回退断路触发 ({_fallback_consec_fail} 连败) — {len(miss_texts)} 条直接返回空, {_FALLBACK_HALFOPEN_COOLDOWN}s 后 half-open")
            for j in range(len(miss_texts)):
                results[miss_indices[j]] = []
            return

    # ── CLOSED: 探测 ≤PROBE_MAX 条 ──
    probe = miss_texts[:_FALLBACK_PROBE_MAX]
    ok_probe = 0
    for j, text in enumerate(probe):
        emb = _fallback_embed(text, model)
        results[miss_indices[j]] = emb
        if emb:
            ok_probe += 1
            _fallback_consec_fail = 0
        else:
            _fallback_consec_fail += 1
    if not ok_probe:
        # trip 点1: 全探失败 → 断路
        _fallback_need_halfopen = True
        _fallback_open_ts = _t.time()
        logger.error(f"[embed] 回退探测 {_FALLBACK_PROBE_MAX} 条全失败 — 断路 (累计 {_fallback_consec_fail} 连败), 余 {len(miss_texts)-_FALLBACK_PROBE_MAX} 条直接返回空")
        for j in range(_FALLBACK_PROBE_MAX, len(miss_texts)):
            results[miss_indices[j]] = []
        return
    # ── CLOSED: 探测有成功 → 续跑逐条 (连败仍可断路) ──
    for j in range(_FALLBACK_PROBE_MAX, len(miss_texts)):
        if _fallback_consec_fail >= _FALLBACK_PROBE_MAX:
            # trip 点2: 续跑连败 → 断路 (REJECT修: 必须置 halfopen, 否则复位分支永不可达)
            _fallback_need_halfopen = True
            _fallback_open_ts = _t.time()
            logger.error(f"[embed] 续跑中断路触发 ({_fallback_consec_fail} 连败) — 余 {len(miss_texts)-j} 条直接置空")
            for k in range(j, len(miss_texts)):
                results[miss_indices[k]] = []
            return
        emb = _fallback_embed(miss_texts[j], model)
        results[miss_indices[j]] = emb
        if emb:
            _fallback_consec_fail = 0
        else:
            _fallback_consec_fail += 1


def _fallback_embed(text: str, model: str) -> list[float]:
    key = _cache_key(text, model)
    if key in _cache:
        _cache.move_to_end(key)
        return _cache[key]

    try:
        if EMBEDDING.backend == "local":
            result = _local_embed_single(text)
        else:
            result = _ollama_embed_single(text, model)
        _store_in_cache(key, result)
        return result
    except Exception as e:
        logger.error(f"Fallback embed failed: {e}")
        return []


def health_check() -> bool:
    try:
        if EMBEDDING.backend == "local":
            model = _get_local_model()
            dim = model.get_sentence_embedding_dimension()
            logger.info(f"Local model health check OK, dim={dim}")
            return True
        else:
            req = urllib.request.Request(f"{EMBEDDING.ollama_url}/api/tags")
            with _DIRECT_OPENER.open(req, timeout=5) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                models = [m["name"] for m in data.get("models", [])]
                return EMBEDDING.long_text_model in models
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return False


def cache_stats() -> dict:
    total = _cache_hits + _cache_misses
    hit_rate = _cache_hits / total if total > 0 else 0.0
    return {
        "hits": _cache_hits,
        "misses": _cache_misses,
        "size": len(_cache),
        "max_size": EMBEDDING.cache_max_size,
        "hit_rate": round(hit_rate, 3),
    }
