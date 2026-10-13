#!/usr/bin/env python3
"""Generate secrets.
  gen_secret.py apikey <label> [agent1|agent2]  -> prints the key (give to partner) and the A2A_API_KEYS entry
  gen_secret.py hash <key>                      -> sha256 of an existing key
  gen_secret.py secret                          -> random 48-byte secret for tokens / shared secrets
"""
import hashlib
import secrets
import sys

if len(sys.argv) < 2:
    print(__doc__); sys.exit(1)
cmd = sys.argv[1]
if cmd == "apikey":
    label = sys.argv[2] if len(sys.argv) > 2 else "client"
    key = "a2a_" + secrets.token_urlsafe(32)
    suffix = f"@{sys.argv[3]}" if len(sys.argv) > 3 else ""
    print(f"API key (give to the client, store safely): {key}")
    print(f"A2A_API_KEYS entry: {label}=sha256:{hashlib.sha256(key.encode()).hexdigest()}{suffix}")
elif cmd == "hash":
    print(hashlib.sha256(sys.argv[2].encode()).hexdigest())
elif cmd == "secret":
    print(secrets.token_urlsafe(48))
else:
    print(__doc__); sys.exit(1)
