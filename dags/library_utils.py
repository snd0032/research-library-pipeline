"""
Helper functions for the research-library pipeline.

All the "real work" lives here, so it can be tested without Airflow.
The DAG file (paper_pipeline.py) only wires these functions together.
"""

from __future__ import annotations

import html
import json
import logging
import os
import sqlite3
from pathlib import Path

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration (read from environment variables, with sensible defaults)
# ---------------------------------------------------------------------------
PROJECT_DIR = Path(__file__).resolve().parent.parent

# Google Drive-synced folder that contains papers/ and output/
LIBRARY_DIR = Path(
    os.environ.get("LIBRARY_DIR", str(Path.home() / "Google Drive" / "ResearchLibrary"))
)
PAPERS_DIR = LIBRARY_DIR / "papers"
OUTPUT_DIR = LIBRARY_DIR / "output"

# Local-only data (NOT synced): SQLite DB and intermediate Markdown files
DATA_DIR = Path(os.environ.get("DATA_DIR", str(PROJECT_DIR / "data")))
DB_PATH = Path(os.environ.get("DB_PATH", str(DATA_DIR / "papers.db")))
MARKDOWN_DIR = DATA_DIR / "markdown"

# Local LLM served through an OpenAI-compatible API (Ollama by default)
LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "http://localhost:11434/v1")
LLM_MODEL = os.environ.get("LLM_MODEL", "llama3.2")
LLM_API_KEY = os.environ.get("LLM_API_KEY", "ollama")  # ignored by Ollama/LM Studio
MAX_CHARS = int(os.environ.get("LLM_MAX_CHARS", "8000"))  # text sent to the LLM

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")

CATEGORIES = [
    "Data Engineering",
    "Machine Learning",
    "Natural Language Processing",
    "Databases",
    "Process Mining",
    "Responsible AI",
    "Other",
]

REQUIRED_FIELDS = {"title": str, "summary": str, "keywords": list, "category": str}


# ---------------------------------------------------------------------------
# 1. Database
# ---------------------------------------------------------------------------
def init_db(db_path: Path = DB_PATH) -> None:
    """Create the SQLite database and the papers table if they don't exist."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS papers (
                filename     TEXT PRIMARY KEY,   -- unique
                title        TEXT NOT NULL,
                summary      TEXT NOT NULL,
                keywords     TEXT NOT NULL,      -- JSON string, e.g. '["a", "b"]'
                category     TEXT NOT NULL,
                processed_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )


def known_filenames(db_path: Path = DB_PATH) -> set[str]:
    with sqlite3.connect(db_path) as conn:
        return {row[0] for row in conn.execute("SELECT filename FROM papers")}


def save_paper(metadata: dict, db_path: Path = DB_PATH) -> None:
    """Insert (or replace) one paper record."""
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO papers (filename, title, summary, keywords, category)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                metadata["filename"],
                metadata["title"],
                metadata["summary"],
                json.dumps(metadata["keywords"], ensure_ascii=False),
                metadata["category"],
            ),
        )


def all_papers(db_path: Path = DB_PATH) -> list[dict]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT filename, title, summary, keywords, category, processed_at "
            "FROM papers ORDER BY processed_at DESC, title"
        ).fetchall()
    papers = []
    for r in rows:
        p = dict(r)
        p["keywords"] = json.loads(p["keywords"])
        papers.append(p)
    return papers


# ---------------------------------------------------------------------------
# 2. Detect new PDFs
# ---------------------------------------------------------------------------
def find_new_pdfs(papers_dir: Path = PAPERS_DIR, db_path: Path = DB_PATH) -> list[str]:
    """Return the filenames of PDFs in papers/ that are not yet in the DB."""
    if not papers_dir.exists():
        log.warning("Papers folder %s does not exist yet", papers_dir)
        return []
    done = known_filenames(db_path)
    new = sorted(p.name for p in papers_dir.glob("*.pdf") if p.name not in done)
    log.info("Found %d new PDF(s): %s", len(new), new)
    return new


# ---------------------------------------------------------------------------
# 3. PDF -> Markdown
# ---------------------------------------------------------------------------
def pdf_to_markdown(filename: str, papers_dir: Path = PAPERS_DIR,
                    markdown_dir: Path = MARKDOWN_DIR) -> str:
    """Convert one PDF to Markdown with PyMuPDF4LLM; return the .md path."""
    import pymupdf4llm  # imported here so the DAG file parses quickly

    markdown_dir.mkdir(parents=True, exist_ok=True)
    md_text = pymupdf4llm.to_markdown(str(papers_dir / filename))
    md_path = markdown_dir / (Path(filename).stem + ".md")
    md_path.write_text(md_text, encoding="utf-8")
    log.info("Converted %s -> %s (%d chars)", filename, md_path, len(md_text))
    return str(md_path)


# ---------------------------------------------------------------------------
# 4. Local LLM -> JSON metadata
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = (
    "You are a research librarian. You read the beginning of a scientific paper "
    "and return ONLY a JSON object, with no extra text, with exactly these keys:\n"
    '  "title": string, the paper title\n'
    '  "summary": string, 2-4 sentences summarising the paper\n'
    '  "keywords": array of 3-6 short strings\n'
    '  "category": one string chosen from: ' + ", ".join(CATEGORIES) + "\n"
)


def validate_metadata(data: object) -> dict:
    """Basic checks: required fields exist and have the expected types."""
    if not isinstance(data, dict):
        raise ValueError("LLM output is not a JSON object")
    for field, expected_type in REQUIRED_FIELDS.items():
        if field not in data:
            raise ValueError(f"Missing field: {field}")
        if not isinstance(data[field], expected_type):
            raise ValueError(f"Field {field} should be {expected_type.__name__}")
    if not all(isinstance(k, str) for k in data["keywords"]):
        raise ValueError("All keywords must be strings")
    return {f: data[f] for f in REQUIRED_FIELDS}


def call_llm(text: str, client=None) -> str:
    """Send the text to the local LLM and return its raw answer (a string)."""
    if client is None:
        from openai import OpenAI
        client = OpenAI(base_url=LLM_BASE_URL, api_key=LLM_API_KEY)
    response = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": "Paper (Markdown):\n\n" + text[:MAX_CHARS]},
        ],
        response_format={"type": "json_object"},  # JSON mode
        temperature=0.2,
    )
    return response.choices[0].message.content


def extract_metadata(filename: str, md_path: str, client=None) -> dict:
    """Ask the LLM for metadata; parse with json.loads; retry once on failure."""
    text = Path(md_path).read_text(encoding="utf-8")
    last_error = None
    for attempt in (1, 2):  # first try + one retry
        raw = call_llm(text, client=client)
        try:
            metadata = validate_metadata(json.loads(raw))
            metadata["filename"] = filename
            log.info("Metadata for %s (attempt %d): %s", filename, attempt, metadata)
            return metadata
        except (json.JSONDecodeError, ValueError) as err:
            last_error = err
            log.warning("Attempt %d failed for %s: %s | raw=%r", attempt, filename, err, raw[:300])
    raise ValueError(f"LLM returned invalid JSON twice for {filename}: {last_error}")


# ---------------------------------------------------------------------------
# 5. Static HTML page
# ---------------------------------------------------------------------------
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Research Library</title>
<style>
  body {{ font-family: system-ui, sans-serif; max-width: 900px; margin: 2rem auto; padding: 0 1rem; color: #222; }}
  h1 {{ margin-bottom: .2rem; }}
  .controls {{ display: flex; gap: .5rem; margin: 1rem 0; }}
  input, select {{ padding: .5rem; font-size: 1rem; }}
  input {{ flex: 1; }}
  .paper {{ border: 1px solid #ddd; border-radius: 8px; padding: 1rem; margin-bottom: .8rem; }}
  .paper h2 {{ font-size: 1.1rem; margin: 0 0 .3rem; }}
  .cat {{ display: inline-block; background: #eef; border-radius: 4px; padding: 0 .4rem; font-size: .85rem; }}
  .kw {{ color: #555; font-size: .85rem; }}
  .muted {{ color: #777; font-size: .85rem; }}
</style>
</head>
<body>
<h1>Research Library</h1>
<p class="muted">{count} paper(s) &middot; generated automatically by the Airflow DAG <code>paper_pipeline</code></p>
<div class="controls">
  <input id="q" type="search" placeholder="Search title, summary, keywords...">
  <select id="cat"><option value="">All categories</option>{options}</select>
</div>
<div id="list">
{cards}
</div>
<p id="empty" class="muted" style="display:none">No paper matches your search.</p>
<script>
  const q = document.getElementById('q'), cat = document.getElementById('cat');
  function filter() {{
    const text = q.value.toLowerCase(), c = cat.value;
    let shown = 0;
    document.querySelectorAll('.paper').forEach(el => {{
      const ok = el.dataset.search.includes(text) && (!c || el.dataset.category === c);
      el.style.display = ok ? '' : 'none';
      if (ok) shown++;
    }});
    document.getElementById('empty').style.display = shown ? 'none' : '';
  }}
  q.addEventListener('input', filter);
  cat.addEventListener('change', filter);
</script>
</body>
</html>
"""

CARD_TEMPLATE = """<div class="paper" data-category="{category}" data-search="{search}">
  <h2><a href="../papers/{href}">{title}</a></h2>
  <span class="cat">{category}</span> <span class="muted">{filename}</span>
  <p>{summary}</p>
  <div class="kw">Keywords: {keywords}</div>
</div>"""


def build_html(output_dir: Path = OUTPUT_DIR, db_path: Path = DB_PATH) -> str:
    """Regenerate output/index.html from the database; return its path."""
    from urllib.parse import quote

    papers = all_papers(db_path)
    esc = lambda s: html.escape(str(s), quote=True)
    cards = []
    for p in papers:
        search = " ".join([p["title"], p["summary"], " ".join(p["keywords"]), p["filename"]]).lower()
        cards.append(CARD_TEMPLATE.format(
            category=esc(p["category"]), search=esc(search), href=quote(p["filename"]),
            title=esc(p["title"]), filename=esc(p["filename"]),
            summary=esc(p["summary"]), keywords=esc(", ".join(p["keywords"])),
        ))
    categories = sorted({p["category"] for p in papers})
    options = "".join(f'<option value="{esc(c)}">{esc(c)}</option>' for c in categories)
    page = HTML_TEMPLATE.format(count=len(papers), options=options, cards="\n".join(cards))

    output_dir.mkdir(parents=True, exist_ok=True)
    out = output_dir / "index.html"
    out.write_text(page, encoding="utf-8")
    log.info("Wrote %s with %d paper(s)", out, len(papers))
    return str(out)


# ---------------------------------------------------------------------------
# 6. Discord notification
# ---------------------------------------------------------------------------
def notify_discord(metadata: dict, webhook_url: str = DISCORD_WEBHOOK_URL) -> bool:
    """Post a short message to a Discord channel through a webhook."""
    if not webhook_url:
        log.warning("DISCORD_WEBHOOK_URL not set: skipping notification")
        return False
    import requests

    message = (
        f"📄 **New paper processed:** {metadata['title']}\n"
        f"**Category:** {metadata['category']}\n"
        f"**Keywords:** {', '.join(metadata['keywords'])}\n"
        f"{metadata['summary']}"
    )
    resp = requests.post(webhook_url, json={"content": message[:2000]}, timeout=10)
    resp.raise_for_status()  # Discord answers 204 No Content on success
    return True
