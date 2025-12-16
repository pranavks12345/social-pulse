#!/usr/bin/env python3
"""
Setup Script
============
Sets up the Social Pulse data pipeline.

Usage: python scripts/setup.py
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent


def run(cmd, cwd=None):
    print(f"  $ {' '.join(cmd)}")
    subprocess.run(cmd, cwd=cwd or ROOT, check=True)


def main():
    print("""
╔═══════════════════════════════════════════════════════════════╗
║                    SOCIAL PULSE SETUP                          ║
╚═══════════════════════════════════════════════════════════════╝
    """)
    
    # Install dependencies
    print("[1/4] Installing Python dependencies...")
    run([sys.executable, "-m", "pip", "install", "-q", "-r", "requirements.txt"])
    
    # Install spaCy model (small for speed)
    print("\n[2/4] Installing spaCy model...")
    try:
        import spacy
        spacy.load("en_core_web_sm")
        print("  Already installed")
    except:
        run([sys.executable, "-m", "spacy", "download", "en_core_web_sm"])
    
    # Initialize database
    print("\n[3/4] Initializing database...")
    sys.path.insert(0, str(ROOT))
    from database.models import db
    db.create_tables()
    
    # Run initial scrape
    print("\n[4/4] Running initial data collection...")
    print("  This will scrape Reddit and HackerNews...")
    
    import asyncio
    from orchestration.flows import quick_scrape_flow
    result = asyncio.run(quick_scrape_flow())
    
    print(f"""
╔═══════════════════════════════════════════════════════════════╗
║                    SETUP COMPLETE! ✅                          ║
╠═══════════════════════════════════════════════════════════════╣
║                                                               ║
║  Data collected:                                              ║
║    Reddit: {result.get('reddit_stored', 0):>5} posts                                 ║
║    HackerNews: {result.get('hn_stored', 0):>5} stories                              ║
║                                                               ║
║  To start the dashboard:                                      ║
║    python scripts/run.py                                      ║
║                                                               ║
║  Or run manually:                                             ║
║    streamlit run dashboard/app.py                             ║
║                                                               ║
║  To collect more data:                                        ║
║    python -m orchestration.flows                              ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
    """)


if __name__ == "__main__":
    main()
