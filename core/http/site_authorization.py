# ============================================================================
# site_authorization.py
# Copied from EdgeService — handles authentication and key derivation for
# artifact downloads.
#
# Responsibilities:
# - get_one_time_access_token(): POST to siteAuthorizationService to get
#   a one-time access token for a specific artifact
# - get_artifact_key(): POST to siteAuthorizationService to get the AES
#   encryption key for that artifact using the access token
#
# TODO: Copy from EdgeService site_authorization.py
# ============================================================================
