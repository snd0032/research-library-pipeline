# Research Library Pipeline (INFO-H420 – Assignment 3)

A small local **Apache Airflow** pipeline that turns research PDFs into a searchable study library.

```
Google Drive/ResearchLibrary/papers/*.pdf
        │  (every 10 min, Airflow DAG "paper_pipeline")
        ▼
find_new_pdfs ─► convert_to_markdown ─► extract_metadata ─► save_to_db ─► build_html ─► notify_discord
                  (PyMuPDF4LLM)          (local LLM, JSON)    (SQLite)     (index.html)   (webhook)
```

* PDFs and the generated `index.html` live in a **Google Drive-synced** folder.
* The SQLite database (`data/papers.db`) stays on the **local machine**, outside the synced folder.

## Folder layout

```
Google Drive/                      Local project/ (this repository)
└── ResearchLibrary/               ├── dags/
    ├── papers/                    │   ├── paper_pipeline.py   # the Airflow DAG
    │   └── paper.pdf              │   └── library_utils.py    # PDF, LLM, SQLite, HTML, Discord logic
    └── output/                    ├── data/                   # papers.db + intermediate .md (git-ignored)
        └── index.html             ├── scripts/run_once.py     # run the pipeline once without Airflow
                                   ├── requirements.txt
                                   └── .env.example
```

## Requirements

* Python 3.10 – 3.13 (macOS or Linux; on Windows use WSL2 — Airflow does not run natively on Windows)
* Google Drive for desktop, with a `ResearchLibrary/papers` folder
* [Ollama](https://ollama.com) (or LM Studio) to serve a local LLM
* A Discord channel webhook (Channel settings → Integrations → Webhooks → New webhook → Copy URL)

## Run instructions

```bash
# 1. Clone and create a virtual environment
git clone <this-repo-url> research-library-pipeline
cd research-library-pipeline
python3 -m venv .venv
source .venv/bin/activate

# 2. Install Airflow (with the official constraints file) and the other packages
AIRFLOW_VERSION=3.1.0            # any recent 3.x works; the DAG also runs on 2.x
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install -r requirements.txt

# 3. Start the local LLM (Ollama exposes an OpenAI-compatible API on :11434/v1)
ollama pull llama3.2
ollama serve            # skip if the Ollama app is already running

# 4. Configure the pipeline
cp .env.example .env    # then edit .env (Drive path, model, Discord webhook)
set -a; source .env; set +a
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# 5. Start Airflow (scheduler + API server + UI, all in one)
airflow standalone
```

Open <http://localhost:8080>, log in with the `admin` password printed in the terminal
(also saved in `~/airflow/` as `simple_auth_manager_passwords.json.generated` in Airflow 3),
switch the **paper_pipeline** DAG on, and drop a PDF into `ResearchLibrary/papers/`.
Within 10 minutes (or immediately with the ▶ *Trigger* button) the paper appears in
`ResearchLibrary/output/index.html` and a message is posted on Discord.

Useful commands:

```bash
airflow dags list                      # the DAG must appear, with no import errors
airflow dags test paper_pipeline       # run one complete DAG run in the terminal
python scripts/run_once.py             # same logic without Airflow (debugging)
sqlite3 data/papers.db "SELECT filename, title, category FROM papers;"
```

## Configuration (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `LIBRARY_DIR` | `~/Google Drive/ResearchLibrary` | Drive-synced folder containing `papers/` and `output/` |
| `DATA_DIR` | `<project>/data` | local folder for `papers.db` and Markdown files |
| `LLM_BASE_URL` | `http://localhost:11434/v1` | OpenAI-compatible endpoint (LM Studio: `http://localhost:1234/v1`) |
| `LLM_MODEL` | `llama3.2` | model name as known by the server |
| `LLM_MAX_CHARS` | `8000` | characters of Markdown sent to the model (title + abstract + intro) |
| `DISCORD_WEBHOOK_URL` | *(empty)* | Discord webhook; if empty, notifications are skipped |

On macOS the Drive folder is usually
`~/Library/CloudStorage/GoogleDrive-<you>@gmail.com/My Drive/ResearchLibrary`.

## How it works

| Task | What it does |
|---|---|
| `init_db` | creates the `papers` table (`filename` is the primary key → unique) |
| `find_new_pdfs` | lists `papers/*.pdf` whose filename is not yet in the database |
| `convert_to_markdown` | one mapped task per new PDF; PyMuPDF4LLM → `data/markdown/<name>.md` |
| `extract_metadata` | sends the Markdown to the local LLM in JSON mode, `json.loads()` the answer, checks the 4 fields and their types, **retries once** if parsing/validation fails |
| `save_to_db` | `INSERT OR REPLACE` one row; `keywords` stored as a JSON string |
| `build_html` | regenerates `output/index.html` (search box + category filter, plain JS); runs even when there is no new PDF (`trigger_rule="none_failed"`) |
| `notify_discord` | one webhook message per newly processed paper, after the page is updated |

The DAG uses `catchup=False` and `max_active_runs=1` so a PDF is never processed twice
by overlapping runs, and **dynamic task mapping** (`.expand()`) so each PDF is an
independent task instance (one failing paper does not block the others).

## Troubleshooting

* **DAG not visible** → check `AIRFLOW__CORE__DAGS_FOLDER` and `airflow dags list-import-errors`.
* **`Connection refused` in `extract_metadata`** → Ollama is not running, or wrong `LLM_BASE_URL`.
* **Invalid JSON twice** → use a larger model (e.g. `qwen2.5:7b`) or lower `LLM_MAX_CHARS`.
* **macOS crash `objc ... fork()`** → `export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES` before `airflow standalone`.
* **Scanned PDFs** → PyMuPDF4LLM falls back to OCR (needs Tesseract or `pip install rapidocr_onnxruntime`).
