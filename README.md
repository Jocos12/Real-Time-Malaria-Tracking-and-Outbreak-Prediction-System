# Real-Time Malaria Tracking and Outbreak Prediction System

A real-time data pipeline that ingests malaria case reports through **Apache Kafka**, persists them to **MySQL**, and visualizes live operational metrics and ML-based outbreak predictions through a **Django** dashboard that updates automatically — no page reloads.

---

## Architecture

```
Case report (JSON)
      │
      ▼
 Kafka Producer  ──►  Kafka Cluster (2 brokers, KRaft mode)  ──►  Kafka Consumer
                                                                        │
                                                                        ▼
                                                                 MySQL Database
                                                                        │
                                                                        ▼
                                                          Django (views.py / ORM)
                                                                        │
                                                     ┌──────────────────┴──────────────────┐
                                                     ▼                                     ▼
                                          dashboard.html (first load)          /api/dashboard-data/ (JSON, polled every N sec)
                                                     │                                     │
                                                     └─────────────► Chart.js (live update, no reload)
```

Spark jobs (`spark_jobs/`) run separately to train and apply the ML models (case-risk and outbreak prediction), writing results back into MySQL via `sync_predictions_to_mysql.py`.

---

## Prerequisites

- Python 3.12+
- Java (JDK 11+) — required by Kafka
- Apache Kafka **4.3.1** (KRaft mode, no ZooKeeper)
- MySQL Server
- Windows 10/11 (setup below is Windows-specific; adapt paths for Linux/Mac)

---

## 1. Kafka Cluster Setup (2 brokers, KRaft mode)

This project runs a real 2-broker Kafka cluster locally (needed for `--replication-factor 2` topics).

### Cluster ID
A single Cluster ID is shared by both brokers:
```
FeXCZQQeSw_sneDEK-mkQ
```

### Broker 1 — `config/server.properties` (combined broker + controller)
Key settings:
```properties
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=localhost:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
controller.listener.names=CONTROLLER
log.dirs=/tmp/kraft-combined-logs
```

Format (first time only, standalone controller):
```powershell
bin\windows\kafka-storage.bat format -t FeXCZQQeSw_sneDEK-mkQ -c config\server.properties --standalone
```

Start:
```powershell
bin\windows\kafka-server-start.bat config\server.properties
```

### Broker 2 — `config/server-2.properties` (broker only)
Copied from `server.properties` with these changes:
```properties
process.roles=broker
node.id=2
listeners=PLAINTEXT://:9094
advertised.listeners=PLAINTEXT://localhost:9094
log.dirs=/tmp/kraft-broker-2-logs
# controller.quorum.bootstrap.servers and controller.listener.names stay unchanged
# so broker 2 knows where the existing controller (broker 1) is
```

Format (same Cluster ID as broker 1):
```powershell
bin\windows\kafka-storage.bat format -t FeXCZQQeSw_sneDEK-mkQ -c config\server-2.properties
```

Start (**important on Windows**, see note below):
```powershell
bin\windows\kafka-server-start.bat config\server-2.properties
```

> **Windows-specific note:** KRaft's metadata-log truncation can fail with
> `KafkaStorageException: ... The process cannot access the file because it is being used by another process`
> when a second broker joins an existing cluster and has to fetch a metadata snapshot. This is a known
> Windows file-locking limitation (NTFS won't rename a file with an open handle, unlike Linux). If it
> happens:
> 1. Stop both brokers, delete `/tmp/kraft-combined-logs` and `/tmp/kraft-broker-2-logs` completely.
> 2. Re-format both brokers (commands above).
> 3. Start broker 1, wait for `Transition from STARTING to STARTED`, then **immediately** start broker 2
>    in a separate terminal so there is nothing to catch up on.
> 4. Avoid clicking inside the broker terminal windows while they start — Windows' QuickEdit mode
>    pauses the process on click, which looks like a freeze.
>
> If it keeps failing, running Kafka inside **WSL2** or **Docker** avoids the issue entirely (Linux
> filesystems don't have this restriction).

### Verify both brokers are up
```powershell
bin\windows\kafka-broker-api-versions.bat --bootstrap-server localhost:9092
```
You should see both `id: 1` (port 9092) and `id: 2` (port 9094) listed.

### Create a replicated topic
```powershell
bin\windows\kafka-topics.bat --create --topic topic-3p-2r --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2
bin\windows\kafka-topics.bat --describe --topic topic-3p-2r --bootstrap-server localhost:9092
```

### Consumer groups — quick reference
- Consumers in the **same** `--group` share the topic's partitions between them (each message goes to
  only one consumer in the group). If a topic has fewer partitions than consumers in the group, some
  consumers stay idle.
- Consumers in **different** groups each get an independent full copy of every message.

---

## 2. Project Setup (Python / Django)

```powershell
cd C:\malaria-bigdata-project
python -m venv venv
(Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned) ; (& .\venv\Scripts\Activate.ps1)
pip install -r requirements.txt   # django, djangorestframework, mysqlclient, kafka-python, python-decouple, etc.
```

### Environment variables

Create a `.env` file at the project root (**same folder as `manage.py`** — this is important, `python-decouple`
looks for it there). This file is git-ignored and must never be committed.

```env
DB_NAME=malaria_db
DB_USER=malaria_user
DB_PASSWORD=your_mysql_password
DB_HOST=localhost
DB_PORT=3308
```

`dashboard_project/settings.py` reads these via:
```python
from decouple import config

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': config('DB_NAME'),
        'USER': config('DB_USER'),
        'PASSWORD': config('DB_PASSWORD'),
        'HOST': config('DB_HOST'),
        'PORT': config('DB_PORT'),
        'OPTIONS': {'charset': 'utf8mb4'},
    }
}
```

### Database & server
```powershell
python manage.py migrate
python manage.py check
python manage.py runserver
```

---

## 3. Real-Time Dashboard (no page reload)

`core/templates/core/dashboard.html` renders once on first load with server-side data (`_build_operational_context`
in `core/views.py`), then polls `GET /api/dashboard-data/` (`dashboard_data_api` view) at a configurable
interval and updates in place:

- Animated counters (`data-metric="total_cases"`, `total_positive`, `region_count`)
- All 7 Chart.js charts (`.update()` called with new data/labels, no chart re-creation)
- The **Region Summary** and **Recent Reported Cases** tables (`data-table="region-summary"` /
  `data-table="recent-cases"`), re-rendered from the JSON response

`dashboard_data_api` returns:
```json
{
  "total_cases": 0,
  "total_positive": 0,
  "region_count": 0,
  "region_summary": [{"region_id": "...", "total_cases": 0, "positive_cases": 0}],
  "recent_cases": [{"case_id": "...", "region_id": "...", "district": "...", "patient_age": 0,
                     "gender": "...", "rapid_test_result": "positive|negative", "timestamp": "..."}],
  "charts": { "region_labels": [], "region_totals": [], "...": "..." }
}
```

### Configurable refresh interval (Settings page)

`core/templates/core/settings.html` lets the user set a refresh interval (minimum 3 seconds), saved in
`localStorage`. `dashboard.html` reads this value on load **and listens for live changes** (via the
`storage` event), so updating the setting in one tab immediately reschedules the polling interval in any
other open dashboard tab — without ever reloading the page. This replaces the old approach, which simply
reloaded the whole page every N seconds.

---

## 4. Security notes

- **Never commit `.env`.** It's excluded via `.gitignore`.
- `settings.py` must only reference credentials through `config('...')` — never hardcode them.
- If a secret is ever accidentally committed and pushed:
  1. Rotate the credential immediately (e.g. `ALTER USER 'malaria_user'@'localhost' IDENTIFIED BY '...';`).
  2. Fix the code to remove the hardcoded value and push the fix.
  3. Because the old value remains in Git history, delete and recreate the GitHub repository (simplest
     option for a young repo) or rewrite history with `git filter-repo` for an established one.

### `.gitignore`
```
.env
venv/
__pycache__/
*.pyc
db.sqlite3
.vscode/
```

---

## 5. Project Structure

```
malaria-bigdata-project/
├── analytics/              # Django app — Analytics & Predictions page
├── consumer/                # Kafka consumer → writes cases into MySQL
├── core/                    # Main Django app
│   ├── kafka_producer.py    # Sends case reports to Kafka
│   ├── models.py            # MalariaCase, CasePrediction, OutbreakPrediction, FeatureImportance
│   ├── views.py             # dashboard_home, dashboard_data_api, analytics_view, analytics_data_api
│   ├── urls.py
│   └── templates/core/
│       ├── dashboard.html   # Operational View — live, no-reload
│       ├── analytics.html   # ML predictions view
│       └── settings.html    # Refresh interval + theme settings
├── dashboard_project/        # Django project settings
├── data_generator/           # Synthetic case data generator (for testing/demo)
├── spark_jobs/                # ETL to HDFS, ML training & prediction jobs (MLlib)
├── .env                      # Local secrets (NOT committed)
├── .gitignore
├── manage.py
└── requirements.txt
```

---

## 6. Pushing to GitHub

```powershell
git init
git add .
git status          # confirm .env is NOT listed before continuing
git commit -m "Initial commit - Real-Time Malaria Tracking System"
git branch -M main
git remote add origin https://github.com/Jocos12/Real-Time-Malaria-Tracking-and-Outbreak-Prediction-System.git
git push -u origin main
```

Repository: https://github.com/Jocos12/Real-Time-Malaria-Tracking-and-Outbreak-Prediction-System