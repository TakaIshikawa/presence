"""Shared utilities for Twitter/X API interactions."""


def get_bearer_token(api_key: str, api_secret: str) -> str:
    """Get OAuth 2.0 bearer token from consumer credentials."""
    import base64
    import requests

    credentials = base64.b64encode(f"{api_key}:{api_secret}".encode()).decode()
    resp = requests.post(
        "https://api.twitter.com/oauth2/token",
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        },
        data="grant_type=client_credentials",
    )
    resp.raise_for_status()
    return resp.json()["access_token"]
