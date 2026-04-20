#!/usr/bin/env python3
"""
Mint a local RS256 JWT for event_bus testing.

Usage:
  python scripts/mint_test_jwt.py \
    --private-key /absolute/path/to/private_key.pem \
    --sub 1 \
    --user-id 1 \
    --expires-min 60

Optional:
  --issuer https://your-issuer
  --audience your-audience
  --scope "write:market write:org"
"""

from __future__ import annotations

import argparse
import time
import json
from pathlib import Path

import jwt  # pip install pyjwt
from dotenv import load_dotenv
import os


def main() -> None:
    load_dotenv()  # load .env if present

    parser = argparse.ArgumentParser()
    parser.add_argument("--private-key", required=True, help="Path to RSA private key PEM")
    parser.add_argument("--sub", required=True, help="JWT subject (string)")
    parser.add_argument("--user-id", type=int, default=None, help="Optional user_id claim")
    parser.add_argument("--org-id", default=None, help="Optional org_id claim")
    parser.add_argument("--issuer", default=os.getenv("JWT_ISSUER") or None)
    parser.add_argument("--audience", default=(os.getenv("JWT_AUDIENCE", "").split(",")[0].strip() or None))
    parser.add_argument("--scope", default=None, help='Space-separated scope string, e.g. "read write"')
    parser.add_argument("--expires-min", type=int, default=60)
    args = parser.parse_args()

    private_key = Path(args.private_key).read_text(encoding="utf-8")

    now = int(time.time())
    payload: dict[str, object] = {
        "sub": str(args.sub),
        "iat": now,
        "exp": now + args.expires_min * 60,
    }

    if args.issuer:
        payload["iss"] = args.issuer
    if args.audience:
        payload["aud"] = args.audience
    if args.user_id is not None:
        payload["user_id"] = args.user_id
    if args.org_id is not None:
        payload["org_id"] = args.org_id
    if args.scope:
        payload["scope"] = args.scope

    token = jwt.encode(payload, private_key, algorithm="RS256")
    print(token)
    print("\nDecoded payload:")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()