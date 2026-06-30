"""
HTTPBasic auth dependency for mcs-serving endpoints.
Validates credentials against productConfig MODEL_CENTER_ACCOUNT/PASSWORD
(looked up via function_id) — same credentials MCS uses to authenticate
to siteMC, so Model Service's existing siteMC credentials work unchanged.
"""
import logging
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from apps.synchronizer.state import get_product_by_func_id

logger = logging.getLogger(__name__)

security = HTTPBasic()


async def verify_credentials_path(
    function_id: str,
    credentials: HTTPBasicCredentials = Depends(security),
) -> None:
    """
    Dependency for GET endpoints where function_id is a path param.
    Usage: _: None = Depends(verify_credentials_path)
    """
    _check(function_id, credentials)


def _check(function_id: str, credentials: HTTPBasicCredentials) -> None:
    try:
        product = get_product_by_func_id(function_id)
    except KeyError:
        logger.warning(f"verify_credentials: unknown function_id={function_id}")
        raise HTTPException(status_code=404, detail="function_id not found")

    correct = (
        credentials.username == product.MODEL_CENTER_ACCOUNT
        and credentials.password == product.MODEL_CENTER_PASSWORD
    )
    if not correct:
        logger.warning(f"verify_credentials: invalid credentials for function_id={function_id}")
        raise HTTPException(
            status_code=401,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )


def verify_credentials_for_function_id(function_id: str, credentials: HTTPBasicCredentials) -> None:
    """
    For POST endpoints where function_id comes from the request body
    (not a path param) — call explicitly inside the handler after
    parsing the body, since FastAPI dependencies can't see body fields
    until the body model is resolved.
    """
    _check(function_id, credentials)
