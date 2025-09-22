import json
import logging
import re
import time
import pytz
import redis
import requests
from celery import shared_task
from datetime import datetime
from flask import request, jsonify
from workers.on_off_functions.on_off_page_message import append_redis_message_pages

# Set up Redis clients
redis_client_pn = redis.StrictRedis(
    host="redisAds",
    port=6379,
    db=12,
    decode_responses=True
)

manila_tz = pytz.timezone("Asia/Manila")

FACEBOOK_API_VERSION = "v23.0"
FACEBOOK_GRAPH_URL = f"https://graph.facebook.com/{FACEBOOK_API_VERSION}"

def fetch_facebook_data(url, access_token):
    """Helper function to fetch data from Facebook API and handle errors."""
    try:
        response = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
        response.raise_for_status()
        data = response.json()

        if "error" in data:
            logging.error(f"Facebook API Error: {data['error']}")
            return {"error": data["error"]}

        return data

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from Facebook API: {e}")
        return {"error": {"message": str(e), "type": "RequestException"}}

def normalize_text(text):
    """Normalize text by removing special characters, leading/trailing hyphens and spaces."""
    # First strip leading/trailing spaces and hyphens
    text = text.strip(' -')
    # Then remove all special characters and convert to lowercase
    normalized = re.sub(r'[^a-zA-Z0-9]', '', text.lower())
    return normalized

def is_page_name_in_campaign(campaign_name, page_name):
    """Check if page name exists in campaign name after normalization."""
    normalized_campaign = normalize_text(campaign_name)
    normalized_page = normalize_text(page_name)
    return normalized_page in normalized_campaign

def update_facebook_status(user_id, ad_account_id, entity_id, new_status, access_token):
    """Update the status of a Facebook campaign or ad set using the Graph API."""
    url = f"{FACEBOOK_GRAPH_URL}/{entity_id}"
    payload = {"status": new_status}
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        logging.info(f"Successfully updated {entity_id} to {new_status}")
        append_redis_message_pages(user_id, f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Successfully updated {entity_id} to {new_status}")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"Error updating {entity_id} to {new_status}: {e}")
        append_redis_message_pages(user_id, f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Error updating {entity_id} to {new_status}: {e}")
        return False

@shared_task
def fetch_campaign_off(user_id, ad_account_id, access_token, matched_schedule):
    """Efficiently fetch campaigns from Facebook API and update only scheduled ones."""

    operation = "ON" if matched_schedule.get("on_off") == "ON" else "OFF"
    
    for page_name in matched_schedule.get("page_name", []):
        lock_key = f"lock:fetch_campaign_only:{ad_account_id}:{access_token}:{normalize_text(page_name)}"
        
        # Lock for the specific ad_account_id, access_token, and page_name combination
        lock = redis_client_pn.lock(lock_key, timeout=300)
        
        if not lock.acquire(blocking=False):
            logging.info(f"Lock already held for {ad_account_id} with access token and page {page_name}. Skipping...")
            append_redis_message_pages(
                user_id,
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠️ Lock already held for page: {page_name}. Skipping..."
            )
            continue  # Skip processing for this specific page if lock is held
        
        try:
            logging.info(f"SCHEDULE DATA: {matched_schedule} for page {page_name}")

            # Now the task is specific to this page_name
            on_off_value = matched_schedule.get("on_off", "").upper()
            target_status = "ACTIVE" if on_off_value == "ON" else "PAUSED"

            append_redis_message_pages(
                user_id,
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Fetching Campaign Data for page: {page_name} in account {ad_account_id} ({operation})"
            )

            # Fetch all campaigns without filtering at API level
            campaign_url = (
                f"{FACEBOOK_GRAPH_URL}/act_{ad_account_id}/campaigns"
                f"?fields=id,name,status"
                f"&limit=1000"  # Reduced batch size for better performance
            )
            
            campaigns_to_update = []
            matched_page_names = set()

            while campaign_url:
                response_data = fetch_facebook_data(campaign_url, access_token)

                if "error" in response_data:
                    raise Exception(response_data["error"].get("message", "Unknown API error"))

                for campaign in response_data.get("data", []):
                    campaign_id = campaign["id"]
                    campaign_name = campaign["name"]
                    campaign_status = campaign["status"]

                    # Use the new matching function to check if page name exists in campaign name
                    if is_page_name_in_campaign(campaign_name, page_name):
                        # Track matched page name
                        matched_page_names.add(normalize_text(page_name))

                        if campaign_status != target_status:
                            campaigns_to_update.append((campaign_id, campaign_name))
                        else:
                            append_redis_message_pages(
                                user_id,
                                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠ Campaign {campaign_name} ({campaign_id}) for page: {page_name} IS ALREADY {target_status}."
                            )

                # Get next page URL if it exists
                campaign_url = response_data.get("paging", {}).get("next")
                
                # Add a small delay between requests to avoid rate limits
                if campaign_url:
                    time.sleep(0.5)  # 500ms delay between requests

            # Log unmatched page names
            if not matched_page_names:
                append_redis_message_pages(
                    user_id,
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ No campaign found for page: {page_name}"
                )

            if not campaigns_to_update:
                append_redis_message_pages(
                    user_id,
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] No campaigns needed updates for page: {page_name}"
                )

            for campaign_id, campaign_name in campaigns_to_update:
                success = update_facebook_status(user_id, ad_account_id, campaign_id, target_status, access_token)

                status_message = (
                    f"✅ Updated {campaign_name} ({campaign_id}) to {target_status} for page: {page_name}"
                    if success
                    else f"❌ Failed to update {campaign_name} ({campaign_id}) for page: {page_name}"
                )
                append_redis_message_pages(user_id, f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {status_message}")

            append_redis_message_pages(
                user_id,
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Campaign updates completed for page: {page_name} in account {ad_account_id} ({operation})"
            )

        except Exception as e:
            error_message = f"❌ Error fetching campaigns for page: {page_name} in account {ad_account_id} ({operation}): {e}"
            logging.error(error_message)
            append_redis_message_pages(user_id, f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {error_message}")

        finally:
            if lock.locked():
                lock.release()
            logging.info(f"🔓 Released lock for {ad_account_id} - page {page_name}")
    
    # Return a summary message after processing all pages
    all_pages = ", ".join(matched_schedule.get("page_name", []))
    return f"Campaign updates completed for all pages: {all_pages} in Ad Account {ad_account_id} ({operation})."
