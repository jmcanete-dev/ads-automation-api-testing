import json
import logging
import pytz
import requests
import time
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from celery import shared_task
from workers.on_off_functions.ad_spent_message import append_redis_message_adspent

# Constants
FACEBOOK_GRAPH_URL = "https://graph.facebook.com/v23.0"
manila_tz = pytz.timezone("Asia/Manila")

# Logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Session with connection pooling
session = requests.Session()
session.mount('https://', requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=50, max_retries=3))

def get_current_time():
    """Get current time in Manila timezone"""
    return datetime.now(manila_tz).strftime("%Y-%m-%d %H:%M:%S")

def append_message(user_id, message):
    """Append message with current Manila time"""
    timestamp = get_current_time()
    append_redis_message_adspent(user_id, f"[{timestamp}] {message}")
    time.sleep(0.1)  # Small delay to ensure SSE can display the message

def get_facebook_user_info(access_token):
    url = f"{FACEBOOK_GRAPH_URL}/me"
    params = {"access_token": access_token, "fields": "id,name"}
    try:
        response = session.get(url, params=params, timeout=10)
        if response.status_code == 200:
            return response.json()
        logger.error(f"User info error: {response.status_code}, {response.text}")
    except Exception as e:
        logger.error(f"Exception in get_facebook_user_info: {e}")
    return None


def get_ad_accounts(access_token):
    url = f"{FACEBOOK_GRAPH_URL}/me/adaccounts"
    params = {"access_token": access_token, "fields": "id,name", "limit": 1000}
    ad_accounts = []
    try:
        while url:
            response = session.get(url, params=params if '?' not in url else {}, timeout=15)
            if response.status_code != 200:
                logger.error(f"Ad accounts error: {response.status_code}, {response.text}")
                break
            data = response.json()
            for acc in data.get("data", []):
                ad_accounts.append({
                    "id": acc.get("id", "").replace("act_", ""),
                    "name": acc.get("name", "Unnamed Account")
                })
            url = data.get("paging", {}).get("next")
        return ad_accounts
    except Exception as e:
        logger.error(f"Exception in get_ad_accounts: {e}")
        return []


def determine_delivery_status(campaign_status, ad_effective_statuses):
    campaign_status = campaign_status.upper() if campaign_status else ""
    ad_statuses = [s.upper() for s in ad_effective_statuses if s]
    if not ad_statuses:
        return "INACTIVE"

    ACTIVE_STATUSES = {"ACTIVE"}
    NOT_DELIVERING_STATUSES = {
        "ADSET_PAUSED", "DISAPPROVED", "PENDING_REVIEW",
        "PREAPPROVED", "PENDING_BILLING_INFO", "WITH_ISSUES"
    }

    active_count = sum(1 for s in ad_statuses if s in ACTIVE_STATUSES)
    adset_paused_count = sum(1 for s in ad_statuses if s == "ADSET_PAUSED")
    disapproved_count = sum(1 for s in ad_statuses if s == "DISAPPROVED")

    if campaign_status == "ACTIVE":
        # If there are any active ads, campaign is active
        if active_count > 0:
            return "ACTIVE"
        # If all ads are disapproved, campaign is recently rejected
        if disapproved_count == len(ad_statuses):
            return "RECENTLY_REJECTED"
        # If all ads are paused, campaign is not delivering
        if adset_paused_count == len(ad_statuses):
            return "NOT_DELIVERING"
        # If there are no active ads and some are in not delivering state
        if active_count == 0 and any(s in NOT_DELIVERING_STATUSES for s in ad_statuses):
            return "NOT_DELIVERING"

    return "INACTIVE"


def process_single_account_batch(account_data):
    ad_account_id, ad_account_name, access_token, user_id = account_data
    try:
        append_message(user_id, f"🔄 Processing account: {ad_account_name} ({ad_account_id})")

        batch = [
            {"method": "GET", "relative_url": f"act_{ad_account_id}/campaigns?fields=id,name,status,daily_budget,budget_remaining&limit=1000"},
            {"method": "GET", "relative_url": f"act_{ad_account_id}/adsets?fields=id,campaign_id,status,ads{{effective_status}}&limit=1000"},
            {"method": "GET", "relative_url": f"act_{ad_account_id}/insights?fields=campaign_id,campaign_name,spend&level=campaign&date_preset=today&limit=1000"}
        ]

        response = session.post(
            FACEBOOK_GRAPH_URL,
            data={"access_token": access_token, "batch": json.dumps(batch)},
            timeout=30
        )

        if response.status_code != 200:
            append_message(user_id, f"❌ Batch error for {ad_account_id}: {response.status_code}")
            return None

        responses = response.json()
        if not isinstance(responses, list) or len(responses) != 3:
            return None

        campaigns_data = json.loads(responses[0].get("body", "{}")).get("data", [])
        adsets_data = json.loads(responses[1].get("body", "{}")).get("data", [])
        insights_data = json.loads(responses[2].get("body", "{}")).get("data", [])

        return {
            "ad_account_id": ad_account_id,
            "ad_account_name": ad_account_name,
            "campaigns": campaigns_data,
            "adsets": adsets_data,
            "insights": insights_data
        }
    except Exception as e:
        logger.error(f"Error processing account {ad_account_id}: {e}")
        return None


@shared_task
def fetch_ad_spend_data(user_id, access_token, max_workers=10):
    try:
        append_message(user_id, "🚀 Starting ad spend fetch")

        user_info = get_facebook_user_info(access_token)
        if not user_info:
            append_message(user_id, "❌ Failed to get user info")
            return {"error": "Failed to get user info"}

        ad_accounts = get_ad_accounts(access_token)
        if not ad_accounts:
            append_message(user_id, "❌ No ad accounts found")
            return {"error": "No ad accounts found"}

        append_message(user_id, f"📊 Found {len(ad_accounts)} ad account(s)")

        account_data_list = [(acc['id'], acc['name'], access_token, user_id) for acc in ad_accounts]

        campaigns = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(process_single_account_batch, account_data_list))

        for r in results:
            if not r:
                continue

            campaign_spends = {
                i.get("campaign_id"): float(i.get("spend", "0"))
                for i in r.get("insights", []) if i.get("campaign_id")
            }

            adsets_by_campaign = defaultdict(list)
            for adset in r.get("adsets", []):
                cid = adset.get("campaign_id")
                if cid:
                    adsets_by_campaign[cid].append(adset)

            for campaign in r.get("campaigns", []):
                cid = campaign.get("id")
                if not cid:
                    continue

                spend = campaign_spends.get(cid, 0.0)
                daily_budget = float(campaign.get("daily_budget", "0") or 0) / 100
                budget_remaining = float(campaign.get("budget_remaining", "0") or 0) / 100
                campaign_status = campaign.get("status", "").upper()

                # Fallback calculation if spend is 0
                if spend <= 0 and daily_budget > 0 and budget_remaining >= 0:
                    spend = round(daily_budget - budget_remaining, 2)
                    # If still 0 or negative, skip
                    if spend <= 0:
                        continue
                elif spend <= 0:
                    continue

                ad_statuses = []
                for adset in adsets_by_campaign.get(cid, []):
                    ads = adset.get("ads", {}).get("data", [])
                    ad_statuses += [a.get("effective_status") for a in ads if a.get("effective_status")]

                delivery_status = determine_delivery_status(campaign_status, ad_statuses)

                campaigns.append({
                    "campaign_id": cid,
                    "campaign_name": campaign.get("name", ""),
                    "ad_account_id": r['ad_account_id'],
                    "ad_account_name": r['ad_account_name'],
                    "delivery_status": delivery_status,
                    "spent": spend,
                    "daily_budget": daily_budget,
                    "budget_remaining": budget_remaining
                })

        append_message(user_id, f"✅ Done! Fetched {len(campaigns)} campaigns with spend.")
        
        return {
            "campaign_spending_data": {
                "campaigns": campaigns,
                "user_name": user_info.get("name", ""),
                "total_campaigns": len(campaigns),
                "total_accounts": len(ad_accounts),
                "updated_at": get_current_time()
            }
        }

    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg)
        append_message(user_id, f"❌ {error_msg}")
        return {"error": str(e)}