import sys
import requests
from pathlib import Path

from setup.config.settings import settings
from shared.config.load_config import load_config_from_directory
from setup.auth.auth_service import generate_jwt, save_tokens, load_tokens

####################################################################
# Constants, cfg and session
####################################################################
REVOLUT_TOKEN_API = "https://sandbox-b2b.revolut.com/api/1.0/auth/token"
REVOLUT_WEBHOOK_API = "https://sandbox-b2b.revolut.com/api/2.0/webhooks"
REVOLUT_ACCOUNT_API = "https://sandbox-b2b.revolut.com/api/1.0/accounts"

cfg = load_config_from_directory(Path("src"), "setup.yaml")
CERT_PATH, KEY_PATH = cfg['keys']['cert_path'], cfg['keys']['key_path']

session = requests.Session()
session.cert = (CERT_PATH, KEY_PATH)
session.verify = False


def authenticate():
    """Authenticate with Revolut and save tokens"""
    try:

        OA_CODE = input("Paste Revolut OA code: ").strip()

        print("Exchanging authorization code for tokens...")
        
        myjwt = generate_jwt()

        response = requests.post(
            REVOLUT_TOKEN_API,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "authorization_code",
                "code": OA_CODE,
                "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                "client_assertion": myjwt
            }
        )

        if not response.ok:
            print(f"Auth failed: {response.text}")
            return False
        
        tokens = save_tokens(response.json())
        print("Authentication successful!")
        return True

    except Exception as e:
        print(f"Authentication failed: {e}")
        return False


def register_webhook():
    """Register webhook with Revolut"""
    try:
        tokens = load_tokens()
        if not tokens:
            print("No tokens found. Please authenticate first (option 1)")
            return False
        
        # Allow override of webhook URL
        default_url = settings.revolut_webhook_url
        print(f"Default webhook URL: {default_url}")
        use_default = input("Use default URL? (y/n): ").strip().lower()
        
        if use_default == 'y':
            webhook_url = default_url
        else:
            webhook_url = input("Enter webhook URL: ").strip()

        headers = {
            "Authorization": f"Bearer {tokens['access_token']}",
            "Content-Type": "application/json"
        }

        payload = {
            "url": webhook_url,
            "events": [
                "TransactionCreated",
                "TransactionStateChanged"
            ]
        }

        print(f"Registering webhook: {webhook_url}")

        response = requests.post(
            REVOLUT_WEBHOOK_API,
            json=payload,
            headers=headers
        )

        print(f"Status: {response.status_code}")

        if response.status_code in (200, 201):
            data = response.json()
            print("Webhook registered successfully!")
            print(f"Webhook ID: {data.get('id')}")
            print(f"\nSIGNING SECRET: {data.get('signing_secret')}")
            print("\nIMPORTANT: Add this to your Vercel environment:")
            print("- vercel env add/update REVOLUT_SECRET production")
            print("  (paste the signing secret above when prompted)")
            print("- or add/update it through your Vercel dashboard.")
            return True
        elif response.status_code == 204:
            print("Webhook is already registered.")
            return True
        else:
            print(f"Webhook registration failed: {response.status_code}, {response.text}")
            return False

    except Exception as e:
        print(f"Webhook registration failed: {e}")
        return False


def list_webhooks():
    """List all registered webhooks"""
    try:
        tokens = load_tokens()
        if not tokens:
            print("No tokens found. Please authenticate first (option 1)")
            return False

        headers = {"Authorization": f"Bearer {tokens['access_token']}"}

        response = requests.get(
            REVOLUT_WEBHOOK_API,
            headers=headers
        )

        if response.ok:
            webhooks = response.json()
            
            if not webhooks:
                print("No webhooks registered")
                return True
            
            print(f"\n{'='*60}")
            print(f"Found {len(webhooks)} webhook(s):")
            print(f"{'='*60}")
            
            for i, wh in enumerate(webhooks, 1):
                print(f"\n{i}. Webhook ID: {wh.get('id')}")
                print(f"   URL: {wh.get('url')}")
                print(f"   Events: {', '.join(wh.get('events', []))}")
            
            print(f"\n{'='*60}")
            return True
        else:
            print(f"Failed to list webhooks: {response.status_code}, {response.text}")
            return False

    except Exception as e:
        print(f"Failed to list webhooks: {e}")
        return False


def delete_webhook():
    """Delete a webhook"""
    try:
        tokens = load_tokens()
        if not tokens:
            print("No tokens found. Please authenticate first (option 1)")
            return False

        # First, list webhooks
        print("\nFetching webhooks...")
        if not list_webhooks():
            return False

        webhook_id = input("\nEnter webhook ID to delete (leave empty to cancel): ").strip()
        
        if webhook_id.lower() == '':
            print("Deletion cancelled.")
            return True

        confirm = input(f"Are you sure you want to delete webhook {webhook_id}? (yes/no): ").strip().lower()
        if confirm != 'yes':
            print("Deletion cancelled.")
            return True

        headers = {"Authorization": f"Bearer {tokens['access_token']}"}

        response = requests.delete(
            f"{REVOLUT_WEBHOOK_API}/{webhook_id}",
            headers=headers
        )

        if response.status_code == 204:
            print(f"Webhook {webhook_id} deleted successfully!")
            return True
        elif response.status_code == 404:
            print(f"Webhook {webhook_id} not found")
            return False
        else:
            print(f"Failed to delete: {response.status_code}, {response.text}")
            return False

    except Exception as e:
        print(f"Failed to delete webhook: {e}")
        return False


def check_tokens():
    """Check token status"""
    try:
        import time
        
        tokens = load_tokens()
        
        if not tokens:
            print("No tokens found. Please authenticate first (option 1)")
            return False
        
        print("Tokens found in Redis")
        
        expires_at = tokens.get('expires_at', 0)
        current_time = time.time()
        
        remaining = int(expires_at - current_time)
        print(f"Status: Valid")
        print(f"Expires in: {remaining} seconds ({remaining // 60} minutes)")
        
        return True

    except Exception as e:
        print(f"Failed to check tokens: {e}")
        return False


def list_accounts():
    """List all Revolut accounts"""
    try:
        tokens = load_tokens()
        if not tokens:
            print("No tokens found. Please authenticate first (option 1)")
            return False

        headers = {"Authorization": f"Bearer {tokens['access_token']}"}

        print("Fetching accounts...")
        response = requests.get(
            REVOLUT_ACCOUNT_API,
            headers=headers
        )

        if response.ok:
            accounts = response.json()
            
            if not accounts:
                print("No accounts found")
                return True
            
            print(f"\n{'='*80}")
            print(f"Found {len(accounts)} account(s):")
            print(f"{'='*80}")
            
            for i, acc in enumerate(accounts, 1):
                print(f"\n{i}. Account ID: {acc.get('id')}")
                print(f"   Name: {acc.get('name')}")
                print(f"   Currency: {acc.get('currency')}")
                print(f"   Balance: {acc.get('balance')}")
                print(f"   State: {acc.get('state')}")
                print(f"   Created: {acc.get('created_at')}")
            
            print(f"\n{'='*60}")
            print("\nIMPORTANT: Copy the Account IDs you want to use")
            print("Add them to config/env/integrations/revolut/revolut.env:")
            print("  REVOLUT_SOURCE_ACCOUNT=<account_id>")
            print("  REVOLUT_TARGET_ACCOUNT=<account_id>")
            print(f"{'='*60}")
            return True
        else:
            print(f"Failed to list accounts: {response.status_code}, {response.text}")
            return False

    except Exception as e:
        print(f"Failed to list accounts: {e}")
        return False

def quick_setup():
    """Run complete setup flow"""
    print("\n" + "="*60)
    print("QUICK SETUP - Auth + Register Webhook")
    print("="*60 + "\n")
    
    # Step 1: Authenticate
    print("Step 1/3: Authentication")
    if not authenticate():
        print("Setup failed at authentication step")
        return False
    
    # Step 2: Register webhook
    print("\nStep 2/3: Webhook Registration")
    if not register_webhook():
        print("Setup failed at webhook registration step")
        return False
    
    # Step 3: List accounts
    print("\nStep 3/3: Listing Accounts")
    list_accounts()

    print("\n" + "="*60)
    print("SETUP COMPLETE!")
    print("="*60)
    return True


def show_menu():
    """Display menu options"""
    print("\n" + "="*60)
    print("REVOLUT INTEGRATION SETUP CLI")
    print("="*60)
    print("\n1. Authenticate (get & save tokens)")
    print("2. Register webhook")
    print("3. List webhooks")
    print("4. Delete webhook")
    print("5. Check token status")
    print("6. List accounts")
    print("7. Quick setup (auth + webhook)")
    print("0. Exit")
    print("\n" + "="*60)


def main():
    """Main CLI loop"""
    while True:
        show_menu()
        
        try:
            choice = input("\nSelect an option: ").strip()
            
            if choice == '1':
                authenticate()
            elif choice == '2':
                register_webhook()
            elif choice == '3':
                list_webhooks()
            elif choice == '4':
                delete_webhook()
            elif choice == '5':
                check_tokens()
            elif choice == '6':
                list_accounts()
            elif choice == '7':
                quick_setup()
            elif choice == '0':
                print("Exiting...")
                sys.exit(0)
            else:
                print("Invalid option. Please try again.")
            
            input("\nPress Enter to continue...")
            
        except KeyboardInterrupt:
            print("\n\nExiting...")
            sys.exit(0)
        except Exception as e:
            print(f"Unexpected error: {e}")
            input("\nPress Enter to continue...")


if __name__ == "__main__":
    main()