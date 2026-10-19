# projects/

Drop project configs (`*.yaml`, see `examples/`) in this directory.
They are discovered at service start and indexed + watched automatically.

Alternatively, register a project at runtime:

```bash
curl -s -X POST http://127.0.0.1:5370/api/register \
  -H 'Content-Type: application/json' \
  -d '{"root": "/path/to/your/project"}'
```
