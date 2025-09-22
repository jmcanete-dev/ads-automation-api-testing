import json
import logging
import re
import pytz
import redis
from celery import shared_task
from datetime import datetime
from models.models import db, CampaignOffOnly
from workers.campaign_fetcher import fetch_campaign
from workers.on_off_functions.only_add_message import append_redis_message2
from app import create_app
from sqlalchemy.orm.attributes import flag_modified
import requests
from sqlalchemy.orm import scoped_session, sessionmaker


# Set up Redis clients
redis_client = redis.StrictRedis(
    host="redisAds",
    port=6379,
    db=3,
    decode_responses=True
)

redis_websocket = redis.Redis(
    host="redisAds",
    port=6379,
    db=11,
    decode_responses=True
)

manila_tz = pytz.timezone("Asia/Manila")

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

@shared_task
def check_campaign_off_only():
    """Check campaigns in CampaignOffOnly and trigger fetch_campaign based on schedule data."""

    app = create_app()
    with app.app_context():
        SessionLocal = scoped_session(sessionmaker(bind=db.engine))
        session = SessionLocal()

        now = datetime.now(manila_tz).strftime("%H:%M")
        current_time = datetime.now(manila_tz).strftime("%Y-%m-%d %H:%M:%S")

        try:
            campaigns = session.query(CampaignOffOnly).all()
            checked_ad_account_ids = []

            for campaign in campaigns:
                user_id = campaign.user_id
                ad_account_id = campaign.ad_account_id
                access_token = campaign.access_token
                schedule_data = campaign.schedule_data

                if not user_id or not ad_account_id or not access_token:
                    logging.warning(f"[{current_time}] Skipping {ad_account_id}: Missing required fields.")
                    append_redis_message2(user_id, ad_account_id, f"[{current_time}] Skipping: Missing required fields.")
                    continue

                if not isinstance(schedule_data, dict):
                    logging.warning(f"[{current_time}] Invalid schedule_data format for {ad_account_id}.")
                    append_redis_message2(user_id, ad_account_id, f"[{current_time}] Invalid schedule_data format.")
                    continue

                matched_schedules = [s for s in schedule_data.values() if s.get("time", "")[:5] == datetime.now().strftime("%H:%M") and s.get("status") != "Paused"]

                if matched_schedules:
                    try:
                        for schedule in matched_schedules:
                            logging.info(f"[{current_time}] SCHEDULE DATA: {schedule}")
                            fetch_campaign_only.apply_async(args=[user_id, ad_account_id, access_token, schedule])
                            logging.info(f"[{current_time}] Triggered fetch_campaign for {ad_account_id}: {schedule}")
                            append_redis_message2(user_id, ad_account_id, f"[{current_time}] Triggered fetch_campaign: {schedule}")
                        
                        checked_ad_account_ids.append(ad_account_id)
                    except Exception as e:
                        logging.error(f"[{current_time}] Error triggering fetch_campaign for {ad_account_id}: {e}")
                        append_redis_message2(user_id, ad_account_id, f"[{current_time}] Error: {e}")

            return f"{current_time} - Checked OFF Campaigns for Ad-Account-IDs: {checked_ad_account_ids}"

        except Exception as e:
            logging.error(f"Error fetching campaigns: {e}")
            return f"Error fetching campaigns: {e}"

        finally:
            session.close()
            SessionLocal.remove()

FACEBOOK_API_VERSION = "v23.0"
FACEBOOK_GRAPH_URL = f"https://graph.facebook.com/{FACEBOOK_API_VERSION}"

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
        append_redis_message2(user_id, ad_account_id, f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Successfully updated {entity_id} to {new_status}")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"Error updating {entity_id} to {new_status}: {e}")
        append_redis_message2(user_id, ad_account_id, f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Error updating {entity_id} to {new_status}: {e}")
        return False
    
def normalize_text(text):
    """Replace all non-alphanumeric characters with spaces and normalize capitalization."""
    return " ".join(re.sub(r"[^a-zA-Z0-9]+", "", text).lower().split())

@shared_task
def fetch_campaign_only(user_id, ad_account_id, access_token, matched_schedule):
    """Fetch campaigns, update only those in schedule, and store in CampaignOffOnly."""

    lock_key = f"lock:fetch_campaign_only:{ad_account_id}"
    lock = redis_client.lock(lock_key, timeout=300)
    pending_schedules_key = f"pending_schedules_only:{ad_account_id}"

    append_redis_message2(
        user_id,
        ad_account_id,
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Fetching Campaign Data for {ad_account_id}, schedule {matched_schedule}",
    )

    if not lock.acquire(blocking=False):
        redis_client.rpush(pending_schedules_key, json.dumps(matched_schedule))
        return f"Fetch already in progress for {ad_account_id}, queued process_scheduled_campaigns_only"

    try:
        scheduled_campaign_names = {normalize_text(name) for name in matched_schedule.get("campaign_name", [])}
        on_off_value = matched_schedule.get("on_off", "").upper()  # Ensure it is a string
        target_status = "ACTIVE" if on_off_value == "ON" else "PAUSED"


        url = f"{FACEBOOK_GRAPH_URL}/act_{ad_account_id}/campaigns?fields=id,name,status"
        campaigns_data = {}

        while url:
            response_data = fetch_facebook_data(url, access_token)
            if "error" in response_data:
                raise Exception(response_data["error"].get("message", "Unknown API error"))

            campaign_batch = {
                campaign["id"]: {
                    "NAME": campaign["name"],
                    "CURRENT_STATUS": campaign["status"],
                    "TARGET_STATUS": target_status,
                    "UPDATED": False,
                }
                for campaign in response_data.get("data", [])
                if normalize_text(campaign["name"]) in scheduled_campaign_names
            }

            campaigns_data.update(campaign_batch)
            url = response_data.get("paging", {}).get("next")

        with db.session.begin():
            campaign_entry = CampaignOffOnly.query.filter_by(ad_account_id=ad_account_id).first()
            if campaign_entry:
                existing_campaigns = campaign_entry.campaigns_data or {}
                updated_campaigns = {**existing_campaigns, **campaigns_data}
                campaign_entry.campaigns_data = updated_campaigns
                campaign_entry.last_time_checked = datetime.now()
                campaign_entry.last_check_status = "Ongoing"
                campaign_entry.last_check_message = f"Campaigns fetched but not updated yet."
                flag_modified(campaign_entry, "campaigns_data")
            else:
                campaign_entry = CampaignOffOnly(
                    ad_account_id=ad_account_id,
                    campaigns_data=campaigns_data,
                    last_time_checked=datetime.now(),
                    last_check_status="Ongoing",
                    last_check_message="Campaigns fetched but not updated yet.",
                )
                db.session.add(campaign_entry)
            db.session.commit()

        append_redis_message2(
            user_id, ad_account_id, f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Filtered campaigns saved."
        )

        updated_campaigns = {}
        for campaign_id, campaign_info in campaigns_data.items():
            campaign_name = campaign_info["NAME"]
            current_status = campaign_info["CURRENT_STATUS"]

            if current_status == target_status:
                status_message = f"Campaign {campaign_name}: {campaign_id} REMAINS {target_status}."
                success = "REMAINS"
                new_status = current_status  # ✅ Ensure new_status is set
            else:
                success = update_facebook_status(user_id, ad_account_id, campaign_id, target_status, access_token)

                if success:
                    new_status = fetch_facebook_data(f"{FACEBOOK_GRAPH_URL}/{campaign_id}?fields=status", access_token).get(
                        "status", target_status
                    )
                else:
                    new_status = current_status

                status_message = (
                    f"Campaign {campaign_name}: {campaign_id} changed to {target_status}."
                    if new_status == target_status
                    else f"Failed to update {campaign_name} ({campaign_id})"
                )

            append_redis_message2(user_id, ad_account_id, f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {status_message}")

            updated_campaigns[campaign_id] = {
                "NAME": campaign_name,
                "CURRENT_STATUS": new_status,
                "TARGET_STATUS": target_status,
                "UPDATED": success,
                "STATUS_MESSAGE": status_message,
            }

        with db.session.begin():
            campaign_entry = CampaignOffOnly.query.filter_by(ad_account_id=ad_account_id).first()
            if campaign_entry:
                campaign_entry.campaigns_data = updated_campaigns
                campaign_entry.last_time_checked = datetime.now()
                campaign_entry.last_check_status = "Success"
                campaign_entry.last_check_message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Campaigns updated."
                flag_modified(campaign_entry, "campaigns_data")
                db.session.commit()

        append_redis_message2(user_id, ad_account_id, f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Campaign updates saved.")
        return f"Fetched and updated selected campaigns for {ad_account_id}."

    except Exception as e:
        error_message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Error fetching campaigns for {ad_account_id}: {e}"
        logging.error(error_message)
        append_redis_message2(user_id, ad_account_id, f"ERROR: {error_message}")

        with db.session.begin():
            campaign_entry = CampaignOffOnly.query.filter_by(ad_account_id=ad_account_id).first()
            if campaign_entry:
                campaign_entry.last_time_checked = datetime.now()
                campaign_entry.last_check_status = "Failed"
                campaign_entry.last_check_message = error_message
                db.session.commit()
            else:
                new_entry = CampaignOffOnly(
                    ad_account_id=ad_account_id,
                    campaigns_data={},
                    last_time_checked=datetime.now(),
                    last_check_status="Failed",
                    last_check_message=error_message,
                )
                db.session.add(new_entry)
                db.session.commit()

        return error_message

    finally:
        if lock.locked():
            lock.release()
        logging.info(f"🔓 Released lock for {ad_account_id}")
