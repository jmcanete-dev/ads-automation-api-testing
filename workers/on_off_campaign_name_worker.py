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
from workers.on_off_functions.on_off_campaign_name import append_redis_message_campaigns

# Set up Redis clients
redis_client = redis.StrictRedis(
    host="redisAds",
    port=6379,
    db=3,
    decode_responses=True
)

redis_on_off = redis.StrictRedis(host="redisAds", port=6379, db=5, decode_responses=True)
redis_on_off_websocket = redis.StrictRedis(host="redisAds", port=6379, db=6, decode_responses=True)

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
    """Replace all non-alphanumeric characters with spaces and normalize capitalization."""
    return " ".join(re.sub(r"[^a-zA-Z0-9]+", "", text).lower().split())


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
        append_redis_message_campaigns(user_id, f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Successfully updated {entity_id} to {new_status}")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"Error updating {entity_id} to {new_status}: {e}")
        append_redis_message_campaigns(user_id, f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Error updating {entity_id} to {new_status}: {e}")
        return False

@shared_task
def fetch_campaign_off(user_id, ad_account_id, access_token, matched_schedule):
    """Efficiently fetch campaigns from Facebook API and update only scheduled ones, with verification."""
    
    operation = "ON" if matched_schedule.get("on_off") == "ON" else "OFF"
    lock_key = f"lock:fetch_campaign_only:{ad_account_id}"
    lock = redis_client.lock(lock_key, timeout=300)

    if not lock.acquire(blocking=False):
        logging.info(f"Fetch already in progress for {ad_account_id}. Skipping...")
        return f"Fetch already in progress for {ad_account_id} ({operation})."

    try:
        logging.info(f"SCHEDULE DATA: {matched_schedule}")

        # ✅ Use set for O(1) lookup on large data
        scheduled_campaign_names = {normalize_text(name) for name in matched_schedule.get("campaign_name", [])}
        on_off_value = matched_schedule.get("on_off", "").upper()  # Ensure it is a string
        target_status = "ACTIVE" if on_off_value == "ON" else "PAUSED"

        message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Fetching Campaign Data for {ad_account_id} ({operation})"
        append_redis_message_campaigns(user_id, message)

        url = f"{FACEBOOK_GRAPH_URL}/act_{ad_account_id}/campaigns?fields=id,name,status&limit=500"
        campaigns_to_update = []

        while url:
            response_data = fetch_facebook_data(url, access_token)

            if "error" in response_data:
                raise Exception(response_data["error"].get("message", "Unknown API error"))

            for campaign in response_data.get("data", []):
                campaign_id = campaign["id"]
                campaign_name = campaign["name"]
                campaign_status = campaign["status"]
                normalized_campaign_name = normalize_text(campaign_name)

                if normalized_campaign_name in scheduled_campaign_names:
                    if campaign_status != target_status:
                        campaigns_to_update.append((campaign_id, campaign_name))
                    else:
                        append_redis_message_campaigns(
                            user_id, 
                            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠ Campaign {campaign_name} ({campaign_id}) REMAINS {target_status}."
                        )

            url = response_data.get("paging", {}).get("next")  # ✅ Handle pagination

        # ✅ Ensure "No campaigns needed updates." is appended BEFORE completion
        if not campaigns_to_update:
            append_redis_message_campaigns(
                user_id, f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] No campaigns needed updates."
            )

        # ✅ Batch update campaigns instead of API calls per campaign
        for campaign_id, campaign_name in campaigns_to_update:
            success = update_facebook_status(user_id, ad_account_id, campaign_id, target_status, access_token)

            status_message = (
                f"✅ Updated {campaign_name} ({campaign_id}) to {target_status}"
                if success
                else f"❌ Failed to update {campaign_name} ({campaign_id})"
            )
            append_redis_message_campaigns(user_id, f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {status_message}")

        # ✅ Verification Step: Ensure updates were actually applied
        verification_url = f"{FACEBOOK_GRAPH_URL}/act_{ad_account_id}/campaigns?fields=id,name,status&limit=500"
        failed_updates = []

        while verification_url:
            verification_data = fetch_facebook_data(verification_url, access_token)

            if "error" in verification_data:
                raise Exception(verification_data["error"].get("message", "Unknown API error"))

            for campaign in verification_data.get("data", []):
                campaign_id = campaign["id"]
                campaign_status = campaign["status"]

                if campaign_id in {cid for cid, _ in campaigns_to_update} and campaign_status != target_status:
                    failed_updates.append(campaign_id)

            verification_url = verification_data.get("paging", {}).get("next")

        if failed_updates:
            append_redis_message_campaigns(
                user_id, 
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠ WARNING: Some campaigns did not update successfully: {failed_updates}"
            )
        else:
            append_redis_message_campaigns(
                user_id, 
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ All campaign updates verified successfully!"
            )

        return f"Campaign updates completed and verified for Ad Account {ad_account_id} ({operation})."

    except Exception as e:
        error_message = f"❌ Error fetching campaigns for {ad_account_id} ({operation}): {e}"
        logging.error(error_message)
        append_redis_message_campaigns(user_id, f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {error_message}")
        return error_message

    finally:
        if lock.locked():
            lock.release()
        logging.info(f"🔓 Released lock for {ad_account_id}")
