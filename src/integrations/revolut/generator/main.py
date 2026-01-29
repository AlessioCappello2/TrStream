import jwt
import time
import json
import requests
import uuid
from .config import REVOLUT_BASE_URL, CERT_PATH, KEY_PATH, TIMEOUT_SECONDS, CLIENT_ID, TOKEN_FILE, OA_CODE

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def trigger_transfer():
    """
    Creates a small internal transfer to trigger webhook events.
    Adjust endpoint based on enabled Revolut features.
    """
    def generate_jwt(client_id, private_key_path):
        with open(private_key_path, 'r') as key_file:
            private_key = key_file.read()
        
        payload = {
            "iss": "example.com",  # Your client ID
            "sub": client_id,
            "aud": "https://revolut.com",
            "exp": int(time.time()) + 3600  # 1 hour expiry
        }
        
        return jwt.encode(payload, private_key, algorithm="RS256")

    transfer_id = str(uuid.uuid4())

    payload = {
        "request_id": transfer_id,
        "amount": 1.00,
        "currency": "EUR",
        "description": "event-generator-test"
    }

    myjwt = generate_jwt(CLIENT_ID, KEY_PATH)

    cert = ("./src/certs/public.pem", "./src/certs/private.pem")

    token_response = requests.post(
        "https://sandbox-b2b.revolut.com/api/1.0/auth/token",
        headers={
            "Content-Type": "application/x-www-form-urlencoded"
        },
        data={
            "grant_type": "authorization_code",
            "code": "oa_sand_tGhYdPOLNbk7voLs6AtKzrjMvt-wgOOX7LbI6NmuTr8",
            "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "client_assertion": myjwt  # You need to generate this
        },
        cert=cert,
        verify=False
    )

    access_token = token_response.json()["access_token"]

    # 2. Now use the token in your API calls
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    # response = requests.post(
    #     f"{REVOLUT_BASE_URL}/transfers",
    #     json=payload,
    #     cert=(CERT_PATH, KEY_PATH),
    #     timeout=TIMEOUT_SECONDS
    # )


    response = requests.get(
        "https://sandbox-b2b.revolut.com/api/1.0/accounts",
        headers=headers,
        cert=cert,
        verify=False  # sandbox only
    )

    print(response.status_code, flush=True)
    print(response.text, flush=True)

    if not response.ok:
        raise RuntimeError(
            f"Revolut error {response.status_code}: {response.text}"
        )

    print("Transfer triggered:", response.json(), flush=True)


#######################################################
from pathlib import Path
TOKEN_PATH = Path(TOKEN_FILE)

def load_tokens():
    if not TOKEN_PATH.exists():
        return None
    with TOKEN_PATH.open("r", encoding="utf-8") as t:
        return json.load(t)


def save_tokens(token_response):
    tokens = {
        "access_token": token_response["access_token"],
        "refresh_token": token_response["refresh_token"],
        "expires_at": int(time.time()) + token_response["expires_in"] - 60
    }

    TOKEN_PATH.write_text(
        json.dumps(tokens),
        encoding="utf-8"
    )

    return tokens


def generate_jwt(client_id, private_key_path):
    with open(private_key_path, "r") as f:
        private_key = f.read()

    payload = {
        "iss": "example.com",
        "sub": client_id,
        "aud": "https://revolut.com",
        "exp": int(time.time()) + 60 # 60 seconds expiry
    }

    return jwt.encode(payload, private_key, algorithm="RS256")

def refresh_access_token(refresh_token):
    myjwt = generate_jwt(CLIENT_ID, KEY_PATH)

    response = requests.post(
        "https://sandbox-b2b.revolut.com/api/1.0/auth/token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "client_assertion": myjwt,
        },
        cert=(CERT_PATH, KEY_PATH),
        verify=False,
    )

    if not response.ok:
        raise RuntimeError(f"Refresh failed: {response.text}")

    return save_tokens(response.json())


def initial_authorization(auth_code):
    myjwt = generate_jwt(CLIENT_ID, KEY_PATH)

    response = requests.post(
        "https://sandbox-b2b.revolut.com/api/1.0/auth/token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "authorization_code",
            "code": auth_code,
            "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "client_assertion": myjwt,
        },
        cert=(CERT_PATH, KEY_PATH),
        verify=False,
    )

    if not response.ok:
        raise RuntimeError(f"Auth failed: {response.text}")

    return save_tokens(response.json())


def get_valid_access_token():
    tokens = load_tokens()

    if tokens and tokens["expires_at"] > time.time():
        return tokens["access_token"]

    if tokens:
        tokens = refresh_access_token(tokens["refresh_token"])
        return tokens["access_token"]

    raise RuntimeError("No tokens available: run initial authorization once.")

def trigger_transfer_2():
    access_token = get_valid_access_token()

    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    response = requests.get(
        "https://sandbox-b2b.revolut.com/api/1.0/accounts",
        headers=headers,
        cert=(CERT_PATH, KEY_PATH),
        verify=False,
    )

    print(response.status_code, flush=True)
    print(response.text, flush=True)

    if not response.ok:
        raise RuntimeError(response.text)


if __name__ == "__main__":
    if not TOKEN_PATH.exists():
        initial_authorization(OA_CODE)
    while True:
        trigger_transfer_2()
        time.sleep(25)