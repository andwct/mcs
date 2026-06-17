# ============================================================================
# security.py
# Copied from EdgeService — encryption/decryption utilities for artifacts.
#
# Responsibilities:
# - decrypt_rsa_aes_tunnel(): for model files — decrypts the RSA+AES-CBC
#   tunnel used by siteArtifactCacheService
# - decrypt_object(): for kernel/package files
# - Partial AES-GCM encryption/decryption using ENCRYPTION_KEY:
#   - KDF(ENCRYPTION_KEY, segment_id) → derived key per segment
#   - Encrypts/decrypts only segments marked encrypted=True in .meta file
#   - Writes results in-place at exact byte offsets
#
# TODO: Copy from EdgeService security.py
# ============================================================================
