from pathlib import Path

"""
Centralizes the definition of all important directory and file paths for the project.
"""

# --- Core Paths ---
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
PACKAGE_DIR = SRC_DIR / "db-scraper"
OUTPUTS_DIR = PROJECT_ROOT / "outputs"

# --- Specific Output Paths ---
MUSICS_DIR = OUTPUTS_DIR / "musics"
