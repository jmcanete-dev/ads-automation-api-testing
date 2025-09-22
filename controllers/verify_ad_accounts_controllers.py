import requests
from flask import jsonify
from models.models import db, User

FACEBOOK_GRAPH_API_URL = "https://graph.facebook.com/v23.0"

def get_facebook_user_id(access_token):
    """Validate access token and return Facebook user ID or error."""
    url = f"{FACEBOOK_GRAPH_API_URL}/me?access_token={access_token}"
    response = requests.get(url).json()
    if "error" in response:
        return None, response["error"]["message"]
    return response["id"], None

def get_ad_accounts(ad_account_id, access_token):
    """Check if the access token has access to a specific ad account."""
    url = f"{FACEBOOK_GRAPH_API_URL}/act_{ad_account_id}?access_token={access_token}"
    response = requests.get(url).json()
    if "error" in response:
        return False, response["error"]["message"]
    return True, None

def get_facebook_pages(facebook_page_id, access_token):
    """Check if the access token has access to a specific Facebook page and return page name."""
    url = f"{FACEBOOK_GRAPH_API_URL}/{facebook_page_id}?fields=id,name&access_token={access_token}"
    response = requests.get(url).json()
    if "error" in response:
        return False, response["error"]["message"], None
    page_name = response.get("name", "Unknown")
    return True, None, page_name

def verify_ad_accounts(data):
    """Verify ad accounts, Facebook pages, and access tokens."""
    user_id = data.get("user_id")
    campaigns = data.get("campaigns", [])

    user = User.query.filter_by(id=user_id).first()
    if not user:
        return jsonify({"error": "Unauthorized: Not a user of Facebook-Marketing-Automation WebApp"}), 403

    access_token_map = {}
    verified_accounts = []

    grouped_campaigns = {}
    for campaign in campaigns:
        access_token = campaign.get("access_token")
        if access_token not in grouped_campaigns:
            grouped_campaigns[access_token] = []
        grouped_campaigns[access_token].append(campaign)

    for access_token, campaign_list in grouped_campaigns.items():
        ad_account_ids = [c["ad_account_id"] for c in campaign_list]

        if access_token not in access_token_map:
            fb_user_id, token_error = get_facebook_user_id(access_token)
            if token_error:
                access_token_map[access_token] = None
                for campaign in campaign_list:
                    verified_accounts.append({
                        "ad_account_id": campaign["ad_account_id"],
                        "ad_account_status": "Not Verified",
                        "ad_account_error": "Invalid access token",
                        "access_token": access_token,
                        "access_token_status": "Not Verified",
                        "access_token_error": token_error,
                        "facebook_page_id": campaign["facebook_page_id"],
                        "facebook_page_status": "Not Verified",
                        "facebook_page_error": "Invalid access token"
                    })
                continue
            access_token_map[access_token] = fb_user_id

        fb_user_id = access_token_map[access_token]
        if not fb_user_id:
            continue

        for campaign in campaign_list:
            ad_account_id = campaign["ad_account_id"]
            facebook_page_id = campaign["facebook_page_id"]

            # Verify ad account access
            ad_account_verified, ad_account_error = get_ad_accounts(ad_account_id, access_token)
            ad_account_status = "Verified" if ad_account_verified else "Not Verified"
            ad_account_error = None if ad_account_status == "Verified" else "Ad account not associated with this access token"

            # Verify Facebook page access
            facebook_page_verified, facebook_page_error, page_name = get_facebook_pages(facebook_page_id, access_token)
            facebook_page_status = "Verified" if facebook_page_verified else "Not Verified"

            verified_accounts.append({
                "ad_account_id": ad_account_id,
                "ad_account_status": ad_account_status,
                "ad_account_error": ad_account_error,
                "access_token": access_token,
                "access_token_status": "Verified",
                "access_token_error": None,
                "facebook_page_id": facebook_page_id,
                "facebook_page_status": facebook_page_status,
                "facebook_page_error": facebook_page_error,
                "facebook_page_name": page_name
            })

    return jsonify({
        "user_id": user_id,
        "verified_accounts": verified_accounts
    })

def verify_ad_account_id(data):
    """Verify ad accounts and access tokens (no Facebook page check). Accepts a list of dicts."""
    verified_accounts = []
    user_id = None
    from models.models import User

    for campaign in data:
        user_id = campaign.get("user_id")
        ad_account_id = campaign.get("ad_account_id")
        access_token = campaign.get("access_token")

        user = User.query.filter_by(id=user_id).first()
        if not user:
            return jsonify({"error": "Unauthorized: Not a user of Facebook-Marketing-Automation WebApp"}), 403

        # Verify ad account access
        ad_account_verified, ad_account_error = get_ad_accounts(ad_account_id, access_token)
        ad_account_status = "Verified" if ad_account_verified else "Not Verified"
        ad_account_error = None if ad_account_verified else "Ad account not associated with this access token"

        verified_accounts.append({
            "ad_account_id": ad_account_id,
            "ad_account_status": ad_account_status,
            "ad_account_error": ad_account_error,
            "access_token": access_token,
            "access_token_status": "Verified" if ad_account_verified else "Not Verified",
            "access_token_error": None if ad_account_verified else ad_account_error
        })

    return jsonify({
        "user_id": user_id,
        "verified_accounts": verified_accounts
    })