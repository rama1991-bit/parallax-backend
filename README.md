# Parallax Backend

FastAPI backend scaffold for Parallax / Narrative Intelligence.

This is a deployable MVP scaffold preserving the project direction:
- smart feed
- article analysis placeholder
- topics
- feeds
- monitors
- alerts
- public briefs
- authors

Run locally:

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Health:
```bash
curl http://localhost:8000/api/v1/health
```
