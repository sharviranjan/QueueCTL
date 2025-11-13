# QueueCTL

QueueCTL is a lightweight, production-grade CLI background job queue system written in Python. It uses SQLite for persistence, handles concurrent workers, and implements exponential backoff for retries with a Dead Letter Queue (DLQ).

## 🚀 Features
- **CLI Interface**: Simple commands to manage jobs and workers.
- **Concurrency**: Run multiple worker processes safely.
- **Persistence**: Jobs survive restarts (stored in `queuectl.db`).
- **Retries**: Exponential backoff strategy (`base ^ attempts`).
- **Dead Letter Queue**: Permanently failed jobs are moved to a DLQ for inspection or manual retry.

## 🛠 Setup

1. **Prerequisites**: Python 3.7+.
2. **Download**: Save `queuectl.py` to your desired folder.
3. No external libraries or `pip install` are required. This project uses only the Python Standard Library.

To run the tool:
```bash
python queuectl.py --help
