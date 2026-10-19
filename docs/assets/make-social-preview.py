#!/usr/bin/env python3
"""Generate the GitHub social preview image (1280x640).

Usage: python3 docs/assets/make-social-preview.py
Output: docs/assets/social-preview.png
Requires: Pillow
"""

from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter, ImageFont

W, H = 1280, 640
OUT = Path(__file__).resolve().parent / "social-preview.png"

BG_TOP = (10, 14, 22)
BG_BOTTOM = (13, 19, 30)
ACCENT_A = (45, 212, 191)   # teal
ACCENT_C = (167, 139, 250)  # violet
TEXT = (240, 245, 255)
MUTED = (148, 163, 184)
DIM = (100, 116, 139)
CARD = (13, 17, 25)
CARD_BORDER = (30, 41, 59)

FONT_DIR = Path("/System/Library/Fonts")
FONT_BOLD = FONT_DIR / "Supplemental" / "Arial Bold.ttf"
FONT_REG = FONT_DIR / "Supplemental" / "Arial.ttf"
FONT_MONO = FONT_DIR / "Menlo.ttc"


def font(path, size):
    return ImageFont.truetype(str(path), size)


def linear_gradient(size, c1, c2):
    w, h = size
    grad = Image.new("RGB", (w, 1))
    px = grad.load()
    for x in range(w):
        t = x / max(w - 1, 1)
        px[x, 0] = tuple(int(c1[i] + (c2[i] - c1[i]) * t) for i in range(3))
    return grad.resize((w, h))


def rounded(draw, box, radius, fill=None, outline=None, width=1):
    draw.rounded_rectangle(box, radius=radius, fill=fill, outline=outline, width=width)


def main():
    img = Image.new("RGBA", (W, H), BG_TOP + (255,))
    img.alpha_composite(linear_gradient((W, H), BG_TOP, BG_BOTTOM).convert("RGBA"))

    # soft accent glows
    glow = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    gd = ImageDraw.Draw(glow)
    gd.ellipse((-260, -280, 780, 420), fill=(16, 74, 80, 150))
    gd.ellipse((860, 300, 1540, 920), fill=(48, 38, 96, 140))
    glow = glow.filter(ImageFilter.GaussianBlur(170))
    img.alpha_composite(glow)

    # --- translucent decorations go on their own layer (ImageDraw on RGBA
    # --- replaces pixels instead of blending; alpha_composite blends properly)
    deco = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    dd = ImageDraw.Draw(deco)
    for x in range(0, W, 64):
        dd.line([(x, 0), (x, H)], fill=(255, 255, 255, 6), width=1)
    for y in range(0, H, 64):
        dd.line([(0, y), (W, y)], fill=(255, 255, 255, 6), width=1)

    chip_f = font(FONT_BOLD, 18)
    tmp = ImageDraw.Draw(Image.new("RGBA", (10, 10)))
    chips = ["Claude Code", "Codex", "OpenCode", "ZCode"]
    chip_boxes = []
    cx, cy = 72, 266
    for name in chips:
        tw = tmp.textlength(name, font=chip_f)
        w, h = int(tw) + 34, 42
        chip_boxes.append((cx, cy, cx + w, cy + h, name))
        rounded(dd, (cx, cy, cx + w, cy + h), 21, fill=(255, 255, 255, 16),
                outline=(148, 163, 184, 80), width=1)
        cx += w + 12

    img.alpha_composite(deco)
    draw = ImageDraw.Draw(img)

    # top accent bar
    img.alpha_composite(linear_gradient((W, 6), ACCENT_A, ACCENT_C).convert("RGBA"), (0, 0))
    draw = ImageDraw.Draw(img)

    # ---- header ----
    x = 72
    draw.text((x, 62), "OPEN SOURCE  ·  MIT", font=font(FONT_BOLD, 17), fill=ACCENT_A)

    size = 66
    title_f = font(FONT_BOLD, size)
    t1, t2 = "Cross-Agent ", "Memory"
    while draw.textlength(t1 + t2, font=title_f) > W - 2 * x:
        size -= 2
        title_f = font(FONT_BOLD, size)
    w1 = draw.textlength(t1, font=title_f)
    draw.text((x, 96), t1, font=title_f, fill=TEXT)

    mw = int(draw.textlength(t2, font=title_f)) + 8
    mh = size + 24
    grad = linear_gradient((mw, mh), ACCENT_A, ACCENT_C)
    mask = Image.new("L", (mw, mh), 0)
    ImageDraw.Draw(mask).text((0, 0), t2, font=title_f, fill=255)
    img.paste(grad, (int(x + w1), 96), mask)
    draw = ImageDraw.Draw(img)

    draw.text((x, 196), "One local memory for all your AI coding agents.",
              font=font(FONT_REG, 28), fill=(203, 213, 225))

    for bx1, by1, bx2, by2, name in chip_boxes:
        draw.text((bx1 + 17, by1 + 10), name, font=chip_f, fill=(226, 232, 240))

    draw.text((x, 342), "SQLite · sqlite-vec · MCP", font=font(FONT_REG, 21), fill=MUTED)
    draw.text((x, 374), "2,000,000+ chunks in a real deployment", font=font(FONT_REG, 21), fill=MUTED)

    draw.line([(x, 452), (x + 596, 452)], fill=(148, 163, 184, 55), width=1)
    draw.text((x, 476), "github.com/indamices/cross-agent-memory",
              font=font(FONT_MONO, 18), fill=ACCENT_A)
    draw.text((x, 516), "Local-first  ·  Zero cloud  ·  Works offline",
              font=font(FONT_REG, 21), fill=DIM)

    # ---- terminal card (right) ----
    tx, ty, tw_, th = 736, 188, 472, 400
    shadow = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    sd = ImageDraw.Draw(shadow)
    sd.rounded_rectangle((tx + 6, ty + 16, tx + tw_ + 6, ty + th + 16), 18,
                         fill=(0, 0, 0, 150))
    shadow = shadow.filter(ImageFilter.GaussianBlur(22))
    img.alpha_composite(shadow)
    draw = ImageDraw.Draw(img)

    rounded(draw, (tx, ty, tx + tw_, ty + th), 18, fill=CARD, outline=CARD_BORDER, width=1)
    for i, c in enumerate([(255, 95, 86), (255, 189, 46), (39, 201, 63)]):
        draw.ellipse((tx + 20 + i * 22, ty + 18, tx + 32 + i * 22, ty + 30), fill=c)
    draw.line([(tx, ty + 46), (tx + tw_, ty + 46)], fill=CARD_BORDER, width=1)

    mono = font(FONT_MONO, 16)
    lines = [
        ("$ ", "./install.sh", (226, 232, 240)),
        ("", "Python 3.12 · sqlite-vec 0.1.9 · FTS5 OK", (110, 231, 183)),
        ("", "schema bootstrap OK (idempotent)", (110, 231, 183)),
        ("", "", MUTED),
        ("$ ", "memory-service", (226, 232, 240)),
        ("", "listening on 127.0.0.1:5370", MUTED),
        ("", "", MUTED),
        ("$ ", 'memory_search "why tiered storage"', (226, 232, 240)),
        ("", "→ docs/architecture.md   0.40", (125, 211, 252)),
        ("", "→ docs/decisions.md      0.46", (125, 211, 252)),
    ]
    ly = ty + 66
    for prompt, body, color in lines:
        if prompt:
            draw.text((tx + 26, ly), prompt, font=mono, fill=ACCENT_A)
            px = draw.textlength(prompt, font=mono)
            draw.text((tx + 26 + px, ly), body, font=mono, fill=color)
        elif body:
            draw.text((tx + 26, ly), body, font=mono, fill=color)
        ly += 27

    draw.text((tx + 26, ty + th - 34), "one memory · every agent",
              font=font(FONT_BOLD, 16), fill=(165, 180, 252))

    img.convert("RGB").save(OUT, "PNG")
    print("wrote", OUT, img.size)


if __name__ == "__main__":
    main()
