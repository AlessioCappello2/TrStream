import jwt
import time
import json
import requests

from upstash_redis import Redis
from ..config.settings import settings
from ..config.load_config import load_config

####################################################################
# Constants and cfg
####################################################################
REVOLUT_TOKEN_API = "https://sandbox-b2b.revolut.com/api/1.0/auth/token"
TOKEN_KEY = "revolut:tokens"
LOCK_KEY = "revolut:refresh_lock"

cfg = load_config()
CERT_PATH, KEY_PATH = cfg['keys']['cert_path'], cfg['keys']['key_path']

####################################################################
# Redis connection and session
####################################################################
r = Redis(
    url=settings.upstash_redis_rest_url,
    token=settings.upstash_redis_rest_token
) 

session = requests.Session()
session.cert = (CERT_PATH, KEY_PATH)
session.verify = False

# ---------- JWT GENERATION ----------

def generate_jwt():
    with open(KEY_PATH, "r") as f:
        private_key = f.read()

    payload = {
        "iss": settings.revolut_redirect_auth,
        "sub": settings.revolut_client_id,
        "aud": "https://revolut.com",
        "exp": int(time.time()) + 60
    }

    return jwt.encode(payload, private_key, algorithm="RS256")


# ---------- REDIS TOKEN STORAGE ----------

def save_tokens(token_response):
    tokens = {
        "access_token": token_response["access_token"],
        "refresh_token": token_response["refresh_token"],
        "expires_at": int(time.time()) + token_response["expires_in"] - 60
    }

    r.set(TOKEN_KEY, json.dumps(tokens), ex=token_response["expires_in"])
    return tokens


def load_tokens():
    data = r.get(TOKEN_KEY)
    if not data:
        return None
    if isinstance(data, bytes):
        data = data.decode("utf-8")
    return json.loads(data)


# ---------- TOKEN REFRESH (LOCK SAFE) ----------

def refresh_access_token(refresh_token):
    myjwt = generate_jwt()

    response = session.post(
        REVOLUT_TOKEN_API,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "client_assertion": myjwt,
        }
    )

    if not response.ok:
        raise RuntimeError(f"Refresh failed: {response.text}")

    return save_tokens(response.json())


def get_valid_access_token():
    tokens = load_tokens()

    if tokens and tokens["expires_at"] > time.time():
        return tokens["access_token"]

    if not tokens:
        raise RuntimeError("No tokens found. Run auth setup first.")

    while True:
        # Acquire refresh lock (expires after 30s)
        lock = r.set(LOCK_KEY, "1", nx=True, ex=30)

        if lock:
            # Lock acquired, refresh and release
            tokens = refresh_access_token(tokens["refresh_token"])
            r.delete(LOCK_KEY)
            return tokens["access_token"]
        else:
            # Another process refreshing: wait and retry
            time.sleep(2)
            tokens = load_tokens()
            if tokens["expires_at"] > time.time():
                return tokens["access_token"]
