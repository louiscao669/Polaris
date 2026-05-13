#!/usr/bin/env python3
"""Print a Bearer JWT using the same signing rules as the API (jwt_codec + settings_jwt).

Requires backend/event_bus/.env (or env vars) to match the deployment you call:
  JWT_SIGNING_ALG, JWT_SECRET or JWT_PRIVATE_KEY_*, JWT_ISSUER, JWT_AUDIENCE, etc.

Usage (from backend/event_bus):

  python3 mint_bearer_from_env.py
  python3 mint_bearer_from_env.py --user-id 18 --org-id 3

Pipe-friendly (token only on stdout):

  export BENCHMARK_JWT="$(python3 mint_bearer_from_env.py)"

Depends on: pip install python-dotenv pyjwt cryptography  (cryptography if RS*)
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Allow `python3 mint_bearer_from_env.py` without PYTHONPATH=.
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env")


def main() -> int:
    parser = argparse.ArgumentParser(description="Mint JWT using app jwt_codec + .env")
    parser.add_argument("--user-id", type=int, default=int(os.getenv("MINT_USER_ID", "18")))
    parser.add_argument("--org-id", type=int, default=None)
    parser.add_argument("--username", default=os.getenv("MINT_USERNAME", "benchmark"))
    parser.add_argument("--ttl-seconds", type=int, default=int(os.getenv("MINT_TTL_SECONDS", "3600")))
    parser.add_argument("--verbose", action="store_true", help="Print claims to stderr")
    args = parser.parse_args()

    # Import after dotenv so JWT_* are loaded
    from app.auth.jwt_codec import mint_access_token

    org_id = args.org_id
    if org_id is None and os.getenv("MINT_ORG_ID"):
        org_id = int(os.getenv("MINT_ORG_ID", "0"))

    token, expires_at = mint_access_token(
        user_id=args.user_id,
        username=args.username,
        first=os.getenv("MINT_FIRST") or None,
        last=os.getenv("MINT_LAST") or None,
        org_id=org_id,
        scopes=None,
        ttl_seconds=max(60, args.ttl_seconds),
    )
    if args.verbose:
        print(f"expires_at: {expires_at.isoformat()}", file=sys.stderr)
    print(token)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
