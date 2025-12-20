#!/usr/bin/env python3
"""
Centralized configuration for all scripts.
Loads configuration from environment variables.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
# Look for .env in parent directory (project root)
env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
# If .env doesn't exist, assume env vars are already set (e.g., by Docker)

# Database configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_SCHEMA = os.getenv("DB_SCHEMA", "finance")

# Validate required database credentials
if not DB_USER or not DB_PASSWORD:
    raise ValueError(
        "Database credentials not configured. "
        "Please set DB_USER and DB_PASSWORD in .env file"
    )

# OTE Portal certificate configuration (for authenticated downloads)
OTE_CERT_PATH = os.getenv("OTE_CERT_PATH")
OTE_CERT_PASSWORD = os.getenv("OTE_CERT_PASSWORD")
OTE_LOCAL_STORAGE_PASSWORD = os.getenv("OTE_LOCAL_STORAGE_PASSWORD")

# ENTSO-E API configuration
ENTSOE_BASE_URL = os.getenv("ENTSOE_BASE_URL", "https://web-api.tp.entsoe.eu/api")
ENTSOE_SECURITY_TOKEN = os.getenv("ENTSOE_SECURITY_TOKEN")
ENTSOE_CONTROL_AREA_DOMAIN = os.getenv("ENTSOE_CONTROL_AREA_DOMAIN", "10YCZ-CEPS-----N")
