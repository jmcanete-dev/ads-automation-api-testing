import logging
import requests
import time
from celery import shared_task
from models.models import db, CampaignsScheduled
from datetime import datetime
from sqlalchemy.orm.attributes import flag_modified
from pytz import timezone
import re

from workers.on_off_functions.account_message import append_redis_message
from workers.on_off_functions.on_off_adsets import append_redis_message_adsets

# Manila timezone
manila_tz = timezone("Asia/Manila")

# Facebook API constants
FACEBOOK_API_VERSION = "v23.0"
FACEBOOK_GRAPH_URL = f"https://graph.facebook.com/{FACEBOOK_API_VERSION}"

def fetch_entity_status(entity_id, access_token):
    """Fetch the current status of an entity from Facebook."""
    url = f"{FACEBOOK_GRAPH_URL}/{entity_id}?fields=status"
    try:
        response = requests.get(
            url, 
            headers={"Authorization": f"Bearer {access_token}"}, 
            timeout=10  # Increased timeout
        )
        response.raise_for_status()
        data = response.json()
        return data.get("status")
    except Exception as e:
        logging.error(f"Error fetching status for {entity_id}: {e}")
        return None

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
        append_redis_message(user_id, ad_account_id, f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Successfully updated {entity_id} to {new_status}")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"Error updating {entity_id} to {new_status}: {e}")
        append_redis_message(user_id, ad_account_id, f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Error updating {entity_id} to {new_status}: {e}")
        return False

def update_facebook_status_with_retry(user_id, ad_account_id, entity_id, entity_name, new_status, access_token, max_retries=2):
    """Update Facebook entity status with retry logic and verification."""
    for attempt in range(max_retries + 1):  # +1 for the initial attempt
        if attempt > 0:
            # Log that we're retrying
            append_redis_message_adsets(
                user_id,
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Retry #{attempt} updating {entity_name} ({entity_id}) to {new_status}"
            )
        
        # Attempt the update
        success = update_facebook_status(user_id, ad_account_id, entity_id, new_status, access_token)
        
        if success:
            # Verify the status change
            time.sleep(1)  # Short delay to allow Facebook to process the change
            current_status = fetch_entity_status(entity_id, access_token)
            
            if current_status == new_status:
                append_redis_message_adsets(
                    user_id,
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Verified {entity_name} is now {new_status}"
                )
                return True
            else:
                logging.warning(
                    f"Status mismatch for {entity_id}: Expected {new_status}, got {current_status}"
                )
                append_redis_message_adsets(
                    user_id,
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Warning: {entity_name} status mismatch - Expected {new_status}, got {current_status}"
                )
        
        # If we're not on the last attempt, wait before retrying
        if attempt < max_retries:
            # Exponential backoff (2^attempt seconds)
            wait_time = 2 ** attempt
            time.sleep(wait_time)
    
    # If we get here, all retries failed
    append_redis_message_adsets(
        user_id,
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Failed to update {entity_name} to {new_status} after {max_retries + 1} attempts"
    )
    return False

# def extract_campaign_code(campaign_name):
#     # Assuming campaign_code is part of the campaign_name (e.g., "Campaign XYZ-12345")
#     # You can adapt the logic here depending on how the campaign_code is embedded in the name
#     """Extract campaign_code from the campaign_name. Assuming the campaign_code is a part of the name."""
#     parts = campaign_name.split("-")  # Split by some delimiter like "-"
#     if len(parts) > 1:
#         return parts[-1].strip()  # Assuming the campaign_code is the last part
#     return None  # Return None if no campaign_code is found

def extract_campaign_code_from_db(campaign_entry):
    """
    Fetch the campaign_code directly from the database.
    """
    return campaign_entry.campaign_code

def normalize_campaign_code(code):
    """Normalize campaign code by removing special characters and extra spaces."""
    # Remove special characters and convert to lowercase
    normalized = re.sub(r'[^a-zA-Z0-9]', '', code.lower())
    return normalized

def is_campaign_code_match(campaign_name, campaign_code):
    """Check if campaign code exists in campaign name after normalization."""
    normalized_name = normalize_campaign_code(campaign_name)
    normalized_code = normalize_campaign_code(campaign_code)
    return normalized_code in normalized_name

@shared_task
def process_scheduled_campaigns(user_id, ad_account_id, access_token, schedule_data):
    try:
        logging.info(f"Processing schedule: {schedule_data}")

        campaign_code = schedule_data["campaign_code"]
        watch = schedule_data["watch"]
        cpp_metric = int(schedule_data.get("cpp_metric", 0))
        on_off = schedule_data["on_off"]

        campaign_entry = CampaignsScheduled.query.filter_by(ad_account_id=ad_account_id).first()
        if not campaign_entry:
            logging.warning(f"No campaign data found for Ad Account {ad_account_id}")
            return f"No campaign data found for Ad Account {ad_account_id}"

        # Use the pre-matched campaigns
        campaign_data = campaign_entry.matched_campaign_data or {}

        if not campaign_data:
            logging.warning(f"No matched campaign data found for Ad Account {ad_account_id}")
            append_redis_message(user_id, ad_account_id, f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] No matched campaign data found.")
            return f"No matched campaign data found for Ad Account {ad_account_id}"

        update_success = False
        if watch == "Campaigns":
            for campaign_id, campaign_info in campaign_data.items():
                current_status = campaign_info.get("STATUS", "")
                campaign_cpp = campaign_info.get("CPP", 0)
                campaign_name = campaign_info.get("campaign_name", "")

                # Decide whether to turn ON or OFF
                if on_off == "ON" and campaign_cpp < cpp_metric:
                    new_status = "ACTIVE"
                elif on_off == "OFF" and campaign_cpp >= cpp_metric:
                    new_status = "PAUSED"
                else:
                    logging.info(f"Campaign {campaign_id} remains {current_status}")
                    append_redis_message(user_id, ad_account_id, f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Campaign {campaign_name} ID: {campaign_id} remains {current_status}")
                    continue

                if current_status != new_status:
                    success = update_facebook_status(user_id, ad_account_id, campaign_id, new_status, access_token)
                    if success:
                        campaign_info["STATUS"] = new_status
                        update_success = True
                        logging.info(f"Updated Campaign {campaign_id} -> {new_status}")
                        append_redis_message(user_id, ad_account_id, f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Updated Campaign {campaign_name} ID: {campaign_id} -> {new_status}")

        if update_success:
            campaign_entry.matched_campaign_data = campaign_data
            flag_modified(campaign_entry, "matched_campaign_data")
            campaign_entry.last_time_checked = datetime.now(manila_tz)
            campaign_entry.last_check_status = "Success"
            campaign_entry.last_check_message = (
                f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Successfully updated {watch} statuses."
            )
            db.session.commit()
            append_redis_message(user_id, ad_account_id, f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Successfully updated {watch} statuses.")

        return f"Processed scheduled {watch} for Ad Account {ad_account_id}"

    except Exception as e:
        logging.error(f"Error processing scheduled {watch} for Ad Account {ad_account_id}: {e}")
        if campaign_entry:
            campaign_entry.last_check_status = "Failed"
            campaign_entry.last_check_message = (
                f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Error: {e}"
            )
            db.session.commit()
        append_redis_message(user_id, ad_account_id, f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Error processing scheduled {watch}: {e}")
        return f"Error processing scheduled {watch} for Ad Account {ad_account_id}: {e}"
    
@shared_task
def process_adsets(user_id, ad_account_id, access_token, schedule_data, campaigns_data):
    try:
        # Extract schedule parameters
        campaign_code = schedule_data["campaign_code"]
        cpp_metric = float(schedule_data.get("cpp_metric", 0))
        on_off = schedule_data["on_off"].upper()

        # Add detailed logging
        append_redis_message_adsets(
            user_id,
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Processing adsets with CPP threshold: ${cpp_metric}, Mode: {on_off}"
        )

        total_processed = 0
        total_updated = 0

        for campaign_id, campaign_info in campaigns_data.items():
            campaign_name = campaign_info.get("campaign_name", "")
            
            # Check if this campaign matches the campaign code
            if not is_campaign_code_match(campaign_name, campaign_code):
                continue

            append_redis_message_adsets(
                user_id,
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Processing campaign: {campaign_name}"
            )

            adsets = campaign_info.get("ADSETS", {})
            
            for adset_id, adset_info in adsets.items():
                adset_cpp = adset_info.get("CPP", 0)
                current_status = adset_info.get("STATUS", "")
                adset_name = adset_info.get("NAME", "Unknown")
                
                total_processed += 1

                # Handle adsets with no sales (CPP = infinity) - turn them OFF
                if adset_cpp == float('inf') or adset_cpp is None:
                    if current_status != "PAUSED":
                        target_status = "PAUSED"
                        reason = "No sales/checkouts - turning OFF"
                        append_redis_message_adsets(
                            user_id,
                            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {adset_name}: No sales detected - will turn OFF"
                        )
                    else:
                        append_redis_message_adsets(
                            user_id,
                            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ➤ {adset_name} already OFF (no sales)"
                        )
                        continue
                # Skip adsets with CPP = 0 (no spend)
                elif adset_cpp == 0:
                    append_redis_message_adsets(
                        user_id,
                        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Skipping {adset_name} - No spend data (CPP = 0)"
                    )
                    continue
                else:
                    # Determine target status based on CPP and mode for adsets with valid CPP
                    if on_off == "OFF":
                        # OFF mode: Turn OFF adsets with CPP >= threshold
                        if adset_cpp >= cpp_metric:
                            target_status = "PAUSED"
                            reason = f"CPP ${adset_cpp:.2f} >= threshold ${cpp_metric} - turning OFF"
                        else:
                            # CPP is below threshold, do not turn ON, just leave as is
                            append_redis_message_adsets(
                                user_id,
                                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {adset_name}: CPP ${adset_cpp:.2f} < threshold ${cpp_metric} - will remain {current_status}"
                            )
                            continue
                            
                    elif on_off == "ON":
                        # ON mode: Turn ON adsets with CPP < threshold, turn OFF those with CPP >= threshold
                        if adset_cpp < cpp_metric:
                            target_status = "ACTIVE"
                            reason = f"CPP ${adset_cpp:.2f} < threshold ${cpp_metric} - turning ON"
                        else:
                            target_status = "PAUSED"
                            reason = f"CPP ${adset_cpp:.2f} >= threshold ${cpp_metric} - turning OFF"

                # Log the decision
                append_redis_message_adsets(
                    user_id,
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {adset_name}: Current={current_status}, Target={target_status} ({reason})"
                )

                # Update if status needs to change
                if current_status != target_status:
                    append_redis_message_adsets(
                        user_id,
                        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Updating {adset_name} from {current_status} to {target_status}"
                    )
                    
                    success = update_facebook_status_with_retry(
                        user_id, ad_account_id, adset_id, adset_name, target_status, access_token
                    )
                    
                    if success:
                        adset_info["STATUS"] = target_status
                        total_updated += 1
                        append_redis_message_adsets(
                            user_id,
                            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✓ Successfully updated {adset_name} to {target_status}"
                        )
                    else:
                        append_redis_message_adsets(
                            user_id,
                            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✗ Failed to update {adset_name} to {target_status}"
                        )
                else:
                    append_redis_message_adsets(
                        user_id,
                        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ➤ {adset_name} already in correct state ({current_status})"
                    )

        # Final summary
        append_redis_message_adsets(
            user_id,
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Processing complete: {total_processed} adsets processed, {total_updated} adsets updated"
        )

        return f"Processed {total_processed} adsets, updated {total_updated} adsets for campaign code {campaign_code}"

    except Exception as e:
        error_msg = f"Error in process_adsets: {str(e)}"
        logging.error(error_msg)
        append_redis_message_adsets(
            user_id,
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: {error_msg}"
        )
        return error_msg

# Additional debugging function to check CPP values
def debug_cpp_values(campaigns_data, cpp_threshold):
    """Debug function to print all CPP values and comparisons"""
    print(f"\n=== CPP DEBUG REPORT ===")
    print(f"Threshold: ${cpp_threshold} ({type(cpp_threshold)})")
    print("-" * 50)
    
    for campaign_id, campaign_info in campaigns_data.items():
        campaign_name = campaign_info.get("campaign_name", "")
        print(f"\nCampaign: {campaign_name}")
        
        adsets = campaign_info.get("ADSETS", {})
        for adset_id, adset_info in adsets.items():
            adset_cpp = adset_info.get("CPP", 0)
            adset_name = adset_info.get("NAME", "Unknown")
            adset_status = adset_info.get("STATUS", "")
            
            print(f"  AdSet: {adset_name}")
            print(f"    CPP: ${adset_cpp} ({type(adset_cpp)})")
            print(f"    Status: {adset_status}")
            
            # Handle different CPP scenarios
            if adset_cpp == float('inf') or adset_cpp is None:
                print(f"    Action: Will turn OFF (no sales)")
            elif adset_cpp == 0:
                print(f"    Action: Skip (no spend data)")
            else:
                print(f"    CPP >= Threshold? {adset_cpp >= cpp_threshold}")
                print(f"    Should pause? {adset_cpp >= cpp_threshold and adset_status != 'PAUSED'}")
    
    print("=" * 50)