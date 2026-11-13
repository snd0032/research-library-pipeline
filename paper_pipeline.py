"""
paper_pipeline — turns research PDFs into a searchable study library.

Every 10 minutes:
  init_db -> find_new_pdfs -> convert_to_markdown (one task per PDF)
          -> extract_metadata (local LLM) -> save_to_db
          -> build_html -> notify_discord (one message per new paper)
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pendulum

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task

sys.path.insert(0, str(Path(__file__).resolve().parent))  # make library_utils importable
import library_utils as lib  # noqa: E402


@dag(
    dag_id="paper_pipeline",
    description="PDF -> Markdown -> local LLM -> SQLite -> HTML -> Discord",
    schedule=timedelta(minutes=10),  # look for new PDFs every 10 minutes
    start_date=pendulum.datetime(2026, 11, 1, tz="Europe/Brussels"),
    catchup=False,          # don't run the missed past intervals
    max_active_runs=1,      # never process the same PDF in two runs at once
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["INFO-H420", "assignment-3"],
)
def paper_pipeline():

    @task
    def init_db() -> None:
        lib.init_db()

    @task
    def find_new_pdfs() -> list[str]:
        return lib.find_new_pdfs()

    @task
    def convert_to_markdown(filename: str) -> dict:
        return {"filename": filename, "md_path": lib.pdf_to_markdown(filename)}

    @task
    def extract_metadata(doc: dict) -> dict:
        return lib.extract_metadata(doc["filename"], doc["md_path"])

    @task
    def save_to_db(metadata: dict) -> dict:
        lib.save_paper(metadata)
        return metadata

    # "none_failed": also runs when there were no new PDFs (mapped tasks skipped),
    # so index.html always exists and reflects the database.
    @task(trigger_rule="none_failed")
    def build_html() -> str:
        return lib.build_html()

    @task
    def notify_discord(metadata: dict) -> bool:
        return lib.notify_discord(metadata)

    # ---- Dependencies -----------------------------------------------------
    new_files = find_new_pdfs()
    init_db() >> new_files

    # Dynamic task mapping: one task instance per new PDF
    docs = convert_to_markdown.expand(filename=new_files)
    metadata = extract_metadata.expand(doc=docs)
    saved = save_to_db.expand(metadata=metadata)

    html_page = build_html()
    saved >> html_page

    notifications = notify_discord.expand(metadata=saved)
    html_page >> notifications  # notify only once the web page is up to date


paper_pipeline()
