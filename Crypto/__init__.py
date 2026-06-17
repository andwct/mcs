"""
Compatibility shim: maps Crypto.* to Cryptodome.* (pycryptodomex).
Avoids pycryptodome (security-flagged package) while keeping
EdgeService code (security.py) unmodified.
"""
