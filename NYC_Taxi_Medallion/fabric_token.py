import json
import os
import sys
from pathlib import Path

import msal
from msal_extensions import FilePersistenceWithDataProtection, PersistedTokenCache


def load_environment_file():
    default_path = Path(__file__).resolve().parents[1] / ".env"
    path = Path(os.environ.get("FABRIC_ENV_FILE", default_path))
    if not path.exists():
        return

    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        entry = line.strip()
        if not entry or entry.startswith("#"):
            continue
        if "=" not in entry:
            raise RuntimeError(f"Invalid environment entry at {path}:{line_number}")
        name, value = (part.strip() for part in entry.split("=", 1))
        if not name:
            raise RuntimeError(
                f"Empty environment variable name at {path}:{line_number}"
            )
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        os.environ.setdefault(name, value)


def require_environment_variable(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


load_environment_file()

CACHE_PATH = os.environ.get(
    "FABRIC_TOKEN_CACHE_PATH",
    str(Path.home() / ".azure" / "fabric_msal_cache.bin"),
)
CLIENT_ID = os.environ.get(
    "FABRIC_PUBLIC_CLIENT_ID",
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46",
)
TENANT_ID = require_environment_variable("AZURE_TENANT_ID")
TARGET_ACCOUNT = require_environment_variable("FABRIC_TARGET_ACCOUNT")
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"


def get_app():
    persistence = FilePersistenceWithDataProtection(CACHE_PATH)
    cache = PersistedTokenCache(persistence)
    app = msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY, token_cache=cache)
    return app


def main():
    if len(sys.argv) < 2:
        print("Usage: fabric_token.py <scope>", file=sys.stderr)
        sys.exit(1)
    scope = sys.argv[1]
    app = get_app()
    accounts = app.get_accounts()
    target = None
    for a in accounts:
        if a.get("username", "").lower() == TARGET_ACCOUNT.lower():
            target = a
            break
    if not target:
        print(
            json.dumps(
                {"error": "account_not_found", "accounts_found": len(accounts)}
            )
        )
        sys.exit(2)
    result = app.acquire_token_silent([scope], account=target)
    if not result or "access_token" not in result:
        print(
            json.dumps(
                {
                    "error": "silent_acquire_failed",
                    "error_code": (result or {}).get("error"),
                }
            )
        )
        sys.exit(3)
    print(result["access_token"])


if __name__ == "__main__":
    main()
