# RDFLib-TinyDB

TinyDB-backed RDFLib store.

> :warning: **This project is no longer viable. TinyDB's performance is too slow as it does not support indices.**

### Features:
- Naive implementation (not context-aware, graph-aware or formula-aware).
- `Store.add()` and `Store.triples()` works.
- `Store.__len__()` works.
- `Store.query()` works.

## Running tests

```bash
pytest
```

With coverage:
```bash
pytest --cov=rdflib_tinydb --cov-report html
```
