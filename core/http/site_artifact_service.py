# ============================================================================
# site_artifact_service.py
# Copied from EdgeService — handles network-level interaction with
# siteArtifactCacheService.
#
# Responsibilities:
# - Constructs download requests (RSA key generation, encrypted_aes_key)
# - Manages HTTP session for artifact download
# - Orchestrates the 3-step tunnel:
#   1. Generate RSA key pair
#   2. Encrypt AES key with artifact-cache-service RSA public key
#   3. POST to artifact-cache-service with model_id, rsa_public_key,
#      encrypted_aes_key
#   4. Receive encrypted model data + X-DUMMY-MODEL-ENC header
#   5. Decrypt X-DUMMY-MODEL-ENC to recover AES key
#   6. Decrypt model data with AES key → plaintext model file
#
# TODO: Copy from EdgeService site_artifact_service.py
# ============================================================================
