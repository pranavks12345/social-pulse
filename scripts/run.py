#!/usr/bin/env python3
"""
Run Script
==========
Starts the Social Pulse dashboard.

Usage: python scripts/run.py
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent


def main():
    print("""
╔═══════════════════════════════════════════════════════════════╗
║                    SOCIAL PULSE                                ║
╚═══════════════════════════════════════════════════════════════╝
    """)
    
    print("Starting dashboard on http://localhost:8501")
    print("Press Ctrl+C to stop\n")
    
    subprocess.run([
        sys.executable, "-m", "streamlit", "run",
        str(ROOT / "dashboard" / "app.py"),
        "--server.port=8501",
        "--browser.gatherUsageStats=false"
    ])


if __name__ == "__main__":
    main()
