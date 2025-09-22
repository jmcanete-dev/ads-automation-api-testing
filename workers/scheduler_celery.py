import logging
import redis
import pytz
from celery import shared_task
from datetime import datetime
from models.models import db, CampaignsScheduled
from workers.campaign_fetcher import fetch_campaign
from workers.on_off_functions.account_message import append_redis_message
from sqlalchemy.orm import scoped_session, sessionmaker

# Set up Redis clients
redis_client = redis.StrictRedis(
    host="redisAds",
    port=6379,
    db=2,
    decode_responses=True
)

redis_websocket = redis.Redis(
    host="redisAds",
    port=6379,
    db=10,
    decode_responses=True
)

manila_tz = pytz.timezone("Asia/Manila")

@shared_task
def check_scheduled_adaccounts():
    """Check scheduled campaigns and trigger fetch_campaign with the correct schedule data."""
    now = datetime.now(manila_tz).strftime("%H:%M")
    current_time = datetime.now(manila_tz).strftime("%Y-%m-%d %H:%M:%S")

    # Create an independent session
    SessionLocal = scoped_session(sessionmaker(bind=db.engine))
    session = SessionLocal()

    try:
        # Fetch all campaigns with schedules
        campaigns = session.query(CampaignsScheduled).all()
        checked_ad_account_ids = []

        for campaign in campaigns:
            user_id = campaign.user_id
            ad_account_id = campaign.ad_account_id
            access_token = campaign.access_token

            if not user_id or not ad_account_id or not access_token:
                error_message = f"[{current_time}] Skipping campaign {campaign.id}: Missing user_id, ad_account_id, or access_token."
                logging.warning(error_message)
                append_redis_message(user_id, ad_account_id, error_message)
                continue

            if not isinstance(campaign.schedule_data, dict):
                error_message = f"[{current_time}] Invalid schedule_data format for campaign {campaign.id}: {campaign.schedule_data}"
                logging.warning(error_message)
                append_redis_message(user_id, ad_account_id, error_message)
                continue

            matched_schedules = [s for s in campaign.schedule_data.values() if s.get("time", "")[:5] == datetime.now().strftime("%H:%M") and s.get("status") != "Paused"]

            if matched_schedules:
                try:
                    for schedule in matched_schedules:
                        fetch_campaign.apply_async(args=[user_id, ad_account_id, access_token, schedule])

                        success_message = f"[{current_time}] Triggered fetch_campaign for ad_account_id: {ad_account_id} with schedule: {schedule}"
                        logging.info(success_message)
                        append_redis_message(user_id, ad_account_id, success_message)

                    checked_ad_account_ids.append(ad_account_id)

                except Exception as e:
                    error_message = f"[{current_time}] Error triggering fetch_campaign for {ad_account_id}: {str(e)}"
                    logging.error(error_message)
                    append_redis_message(user_id, ad_account_id, error_message)

        return f"{current_time} - Checked Ad-Account-IDs: {checked_ad_account_ids}"

    except Exception as e:
        logging.error(f"Error fetching campaigns: {str(e)}")
        return f"Error fetching campaigns: {str(e)}"

    finally:
        session.close()
        SessionLocal.remove()  # Cleanup session to prevent leaks
