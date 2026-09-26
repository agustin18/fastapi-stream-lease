# Sharing the project

Publish the next release after the correctness checks and GitHub protections pass. Link to the runnable example, not just the package page. A short introduction for FastAPI or Python communities:

> I built `fastapi-stream-lease` to cap the number of active SSE or LLM streams per user and across FastAPI workers. It uses Redis leases so a dead worker eventually frees its slots. The example shows two active streams and a third request returning 429. I'd value feedback from anyone running long-lived streams: [README](https://github.com/agustin18/fastapi-stream-lease#readme) · [PyPI](https://pypi.org/project/fastapi-stream-lease/).

Share this where project announcements are permitted and answer integration questions publicly. Track which example or explanation users struggle with in GitHub Issues. Avoid download or speed claims until there is a reproducible measurement.
