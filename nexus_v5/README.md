# NEXUS v5 — Shipping Intelligence
### 🔒 Runs 100% locally — no internet, no server, no cloud

---

## 🚀 Quick Start

### Windows
1. Double-click **`START_NEXUS.bat`**
2. Wait ~30 seconds on first run (installs dependencies)
3. Browser opens automatically at **http://localhost:5000**

### Mac / Linux
1. Open Terminal in this folder
2. Run: `bash start_nexus.sh`
3. Browser opens automatically at **http://localhost:5000**

> **Your data never leaves your computer.**

---

## 📋 Requirements
- Python 3.9 or higher → [python.org/downloads](https://www.python.org/downloads/)
- No other installs needed — launcher handles everything

---

## 🗂 Project Structure
```
nexus_v5/
├── START_NEXUS.bat        ← Windows one-click launcher
├── start_nexus.sh         ← Mac/Linux launcher
├── requirements.txt       ← Python dependencies
├── backend/
│   ├── app.py             ← Flask server (runs on localhost:5000)
│   ├── database.py        ← SQLite database (local file)
│   ├── extractors/        ← PDF/invoice parsers
│   ├── matchers/          ← Shipment matching logic
│   └── utils/             ← Analytics, chatbot, reports
├── frontend/
│   └── index.html         ← Full UI (served by Flask)
└── data/                  ← Your local SQLite database lives here
```

---

## ⌨️ Keyboard Shortcuts
| Key | Action |
|-----|--------|
| `Alt+D` | Dashboard |
| `Alt+S` | Shipments |
| `Alt+A` | Analytics |
| `Alt+I` | Import |
| `Alt+C` | Chat |
| `Alt+R` | Reminders |
| `Alt+P` | PDF Report |
| `Alt+E` | Estimator |
| `Alt+K` | Compare |
| `/` | Search |

---

## 🆕 What's New in v5
- Dark Mode (`Alt+N`)
- Period Comparison — compare two date ranges side-by-side
- Top Consignees — ranked spend by recipient
- Weight Distribution Histogram
- 52-week Activity Heatmap
- Service Mix Chart (Express / Expedited / Standard)
- Notification Bell for reminders
- Export to Excel
- 6 new API endpoints

---

## ❓ Troubleshooting

**"Python not found"** → Install Python from python.org and check "Add to PATH"

**Port 5000 already in use** → Open `backend/app.py`, change `port=5000` to `port=5001`, then visit http://localhost:5001

**Blank page** → Wait 5 seconds and refresh — Flask may still be starting up
