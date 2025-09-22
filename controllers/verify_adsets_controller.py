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

def verify_ad_accounts(data):
    """Verify ad accounts and access tokens."""
    user_id = data[0].get("user_id")  # Extract user_id from the first item in the list
    campaigns = data

    # Fetch the user from the database
    user = User.query.filter_by(id=user_id).first()
    if not user:
        return jsonify({"error": "Unauthorized: Not a user of Facebook-Marketing-Automation WebApp"}), 403

    access_token_map = {}
    verified_accounts = []

    # Group campaigns by access_token
    grouped_campaigns = {}
    for campaign in campaigns:
        access_token = campaign.get("access_token")
        if access_token not in grouped_campaigns:
            grouped_campaigns[access_token] = []
        grouped_campaigns[access_token].append(campaign)

    for access_token, campaign_list in grouped_campaigns.items():
        # For each campaign list, we'll process the campaigns
        if access_token not in access_token_map:
            fb_user_id, token_error = get_facebook_user_id(access_token)
            if token_error:
                access_token_map[access_token] = None
                # Add to verified accounts with error details
                for campaign in campaign_list:
                    verified_accounts.append({
                        "ad_account_id": campaign["ad_account_id"],
                        "ad_account_status": "Not Verified",
                        "ad_account_error": "Invalid access token",
                        "access_token": access_token,
                        "access_token_status": "Not Verified",
                        "access_token_error": token_error,
                        "schedule_data": campaign["schedule_data"]
                    })
                continue
            access_token_map[access_token] = fb_user_id

        fb_user_id = access_token_map[access_token]
        if not fb_user_id:
            continue

        # Verify the ad account
        for campaign in campaign_list:
            ad_account_id = campaign["ad_account_id"]
            ad_account_verified, ad_account_error = get_ad_accounts(ad_account_id, access_token)

            # If the ad account is not accessible, append an error to the result
            if not ad_account_verified:
                verified_accounts.append({
                    "ad_account_id": ad_account_id,
                    "ad_account_status": "Not Verified",
                    "ad_account_error": ad_account_error,
                    "access_token": access_token,
                    "access_token_status": "Verified",  # Assuming the token itself is valid
                    "access_token_error": None,
                    "schedule_data": campaign["schedule_data"]
                })
            else:
                # If the ad account is accessible, mark it as verified
                verified_accounts.append({
                    "ad_account_id": ad_account_id,
                    "ad_account_status": "Verified",
                    "ad_account_error": None,
                    "access_token": access_token,
                    "access_token_status": "Verified",
                    "access_token_error": None,
                    "schedule_data": campaign["schedule_data"]
                })

    return jsonify({
        "user_id": user_id,
        "verified_accounts": verified_accounts
    })
