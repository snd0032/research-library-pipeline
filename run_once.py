"""
Run the whole pipeline once WITHOUT Airflow (useful for debugging).

    python scripts/run_once.py

Uses the same configuration (environment variables) as the DAG.
"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dags"))
import library_utils as lib  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

lib.init_db()
for filename in lib.find_new_pdfs():
    md_path = lib.pdf_to_markdown(filename)
    metadata = lib.extract_metadata(filename, md_path)
    lib.save_paper(metadata)
    lib.build_html()
    lib.notify_discord(metadata)
lib.build_html()
print("Done. Open:", lib.OUTPUT_DIR / "index.html")
