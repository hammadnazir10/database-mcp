import os

DATABASE_URL = os.environ.get("DATABASE_URL", "")
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "")
PG_DATABASE = os.environ.get("PG_DATABASE", "postgres")
PG_SCHEMA = os.environ.get("PG_SCHEMA", "public")

READ_ONLY = os.environ.get("DB_READ_ONLY", "false").lower() == "true"
MAX_ROWS = int(os.environ.get("DB_MAX_ROWS", "100"))
POOL_MIN = int(os.environ.get("DB_POOL_MIN", "2"))
POOL_MAX = int(os.environ.get("DB_POOL_MAX", "10"))

WRITE_KEYWORDS = {
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
    "REPLACE", "TRUNCATE", "RENAME", "GRANT", "REVOKE",
}

DANGEROUS_PATTERNS = [
    r";\s*(DROP|DELETE|ALTER|TRUNCATE|GRANT|REVOKE)",
    r"--",
    r"/\*",
    r"\bCOPY\b",
    r"\bEXECUTE\b",
    r"\bPREPARE\b",
    r"\bDEALLOCATE\b",
    r"\bLISTEN\b",
    r"\bNOTIFY\b",
    r"\bLOAD\b",
    r"\bDO\s+\$",
]
