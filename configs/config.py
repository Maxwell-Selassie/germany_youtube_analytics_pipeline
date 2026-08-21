from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"

RAW_DATA_DIR = DATA_DIR / "raw"
BRONZE_DATA_DIR = DATA_DIR / "bronze"
SILVER_DATA_DIR = DATA_DIR / "silver"
GOLD_DATA_DIR = DATA_DIR / "gold"
AUDIT_DATA_DIR = DATA_DIR / "audit"