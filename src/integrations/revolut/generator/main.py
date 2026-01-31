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
            "code": "oa_sand_xxxx",
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
    

def simulate_transfer():
    access_token = get_valid_access_token()

    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    request_id = str(uuid.uuid4())

    payload = {
        "request_id": request_id,
        "source_account_id": "source",
        "target_account_id": "target",
        "amount": 1,
        "currency": "GBP"
    }

    response = requests.post("https://sandbox-b2b.revolut.com/api/1.0/transfer", json=payload, headers=headers)

    print(response.status_code, flush=True)
    print(response.text, flush=True)



#######################################################
# from datetime import datetime, timezone 
# import random
# import hashlib
# import hmac

# def generate_signature(payload: bytes) -> str:
#     """
#     Generates HMAC SHA256 signature.
#     """
#     secret_bytes = WEBHOOK_SECRET.encode("utf-8")
#     signature = hmac.new(secret_bytes, payload, hashlib.sha256).hexdigest()
#     return signature

# def send_event(payload: dict):
#     body = json.dumps(payload).encode("utf-8")
#     signature = generate_signature(body)

#     headers = {
#         "Content-Type": "application/json",
#         "Revolut-Signature": signature,
#     }

#     response = requests.post(WEBHOOK_URL, data=body, headers=headers)

#     print(f"Sent {payload['event']} | Status: {response.status_code}")
#     if response.status_code != 200:
#         print("Response:", response.text)

# def create_transaction():
#     tx_id = f"tx_{uuid.uuid4().hex[:10]}"

#     payload = {
#         "event": "TransactionCreated",
#         "timestamp": datetime.now(timezone.utc).isoformat(),
#         "data": {
#             "id": tx_id,
#             "account_id": "acc_001",
#             "amount": round(random.uniform(10, 500), 2),
#             "currency": "EUR",
#             "state": "PENDING",
#             "description": "Test Purchase",
#         }
#     }

#     send_event(payload)
#     return tx_id


# def update_transaction_state(tx_id):
#     payload = {
#         "event": "TransactionStateChanged",
#         "timestamp": datetime.now(timezone.utc).isoformat(),
#         "data": {
#             "id": tx_id,
#             "previous_state": "PENDING",
#             "new_state": "COMPLETED",
#         }
#     }

#     send_event(payload)

WEBHOOK_URL = "https://revolut-webhook.vercel.app/api/webhook"

def register_webhook():
    access_token = get_valid_access_token()

    url = "https://sandbox-b2b.revolut.com/api/1.0/webhook"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    payload = {
        "url": WEBHOOK_URL,
        "events": [
            "TransactionCreated",
            "TransactionStateChanged"
        ]
    }

    response = requests.post(url, json=payload, headers=headers)

    print(response.status_code, flush=True)
    print(response.text, flush=True)


def trigger_vercel():
    
    payload = {
        "event": "TransactionCreated",
        "data": {"id": "tx_123", "amount": 100, "currency": "EUR", "state": "PENDING"}
    }

    requests.post(WEBHOOK_URL, json=payload)

if __name__ == "__main__":
    if not TOKEN_PATH.exists():
        initial_authorization(OA_CODE)
        register_webhook()
    #trigger_transfer_2()
    simulate_transfer()
    #while True:
        #trigger_transfer_2()
        #time.sleep(25)
    #trigger_vercel()