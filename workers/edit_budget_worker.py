import json
import re
import logging
import pytz
import requests
import time
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from celery import shared_task
from workers.on_off_functions.edit_budget_message import append_redis_message_editbudget

# Constants
FACEBOOK_GRAPH_URL = "https://graph.facebook.com/v23.0"
manila_tz = pytz.timezone("Asia/Manila")

# Logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def get_current_time():
    """Get current time in Manila timezone"""
    return datetime.now(manila_tz).strftime("%Y-%m-%d %H:%M:%S")

def normalize_name(name: str) -> str:
    """
    Normalize campaign names by lowercasing and removing extra spaces (but NOT dashes or special characters except spaces).
    """
    name = name.lower().strip()
    name = re.sub(r'\s+', ' ', name)
    return name

def parse_campaign_name_flexible(campaign_name: str, expected_page_name: str, expected_item_name: str, expected_campaign_code: str) -> dict:
    """
    Flexibly parse campaign name, matching page_name, item_name, and campaign_code by value, not position.
    Returns a dict indicating if each was found.
    """
    if not campaign_name:
        return {"page_name_found": False, "item_name_found": False, "campaign_code_found": False}
    parts = [p.strip().lower() for p in campaign_name.split('-')]
    return {
        "page_name_found": expected_page_name.lower() in parts if expected_page_name else False,
        "item_name_found": expected_item_name.lower() in parts if expected_item_name else False,
        "campaign_code_found": expected_campaign_code.lower() in parts if expected_campaign_code else False,
    }

def extract_page_name(campaign_name: str) -> str:
    """
    Extract the page name (first part before '-') from the campaign name.
    """
    return campaign_name.split('-')[0].lower().strip() if campaign_name else ""

def convert_to_minor_units(user_input, user_id=None) -> int:
    """
    Convert user input budget in pesos to centavos (minor units).
    Accepts numbers with commas, e.g., "1,000" or "1,000.50".
    Silently removes commas and only raises an error if the value is not a valid number after removing commas.
    """
    try:
        if isinstance(user_input, str):
            user_input = user_input.replace(",", "")
        return int(float(user_input) * 100)
    except (ValueError, TypeError):
        if user_id:
            from workers.on_off_functions.edit_budget_message import append_redis_message_editbudget
            append_redis_message_editbudget(user_id, f"new_budget invalid: '{user_input}' is not a valid number")
        raise ValueError(f"new_budget invalid: '{user_input}' is not a valid number")

def find_campaign_id_by_components(ad_account_id: str, access_token: str, input_page_name: str, input_item_name: str = None, input_campaign_code: str = None, user_id=None) -> str:
    """
    Get the campaign ID by matching page_name, item_name, and campaign_code in the campaign name.
    FLEXIBLE MATCHING: All three components must be present, regardless of order.
    """
    url = f"{FACEBOOK_GRAPH_URL}/act_{ad_account_id}/campaigns"
    params = {
        "fields": "name",
        "limit": 1000,
        "access_token": access_token
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        # Normalize input values
        input_page_name = input_page_name.lower().strip() if input_page_name else ""
        input_item_name = input_item_name.lower().strip() if input_item_name else ""
        input_campaign_code = input_campaign_code.strip() if input_campaign_code else ""

        if not input_page_name or not input_item_name or not input_campaign_code:
            logger.warning(f"[{get_current_time()}] FLEXIBLE MODE: All three components (page_name, item_name, campaign_code) must be provided. Got: page_name='{input_page_name}', item_name='{input_item_name}', campaign_code='{input_campaign_code}'")
            return ""

        best_match = None
        best_match_score = 0
        best_match_details = {}

        for campaign in data.get("data", []):
            campaign_name = campaign.get("name", "")
            parsed = parse_campaign_name_flexible(campaign_name, input_page_name, input_item_name, input_campaign_code)
            page_match = parsed["page_name_found"]
            item_match = parsed["item_name_found"]
            code_match = parsed["campaign_code_found"]
            match_score = sum([page_match, item_match, code_match])
            if match_score > best_match_score:
                best_match_score = match_score
                best_match = campaign
                best_match_details = {
                    "campaign_name": campaign_name,
                    "campaign_id": campaign["id"],
                    "page_match": page_match,
                    "item_match": item_match,
                    "code_match": code_match
                }
            if page_match and item_match and code_match:
                logger.info(f"[{get_current_time()}] FLEXIBLE MATCH FOUND: {campaign_name} ({campaign['id']})")
                return campaign["id"]
        if best_match and user_id:
            from workers.on_off_functions.edit_budget_message import append_redis_message_editbudget
            error_details = []
            if not best_match_details["page_match"]:
                error_details.append(f"page_name: expected '{input_page_name}' not found")
            if not best_match_details["item_match"]:
                error_details.append(f"item_name: expected '{input_item_name}' not found")
            if not best_match_details["code_match"]:
                error_details.append(f"campaign_code: expected '{input_campaign_code}' not found")
            append_redis_message_editbudget(user_id, f"FLEXIBLE MODE: No exact match. Closest match '{best_match_details['campaign_name']}' has mismatches: {', '.join(error_details)}")
        elif not best_match:
            logger.warning(f"[{get_current_time()}] FLEXIBLE MODE: No campaigns found in account {ad_account_id}")
        return ""
    except requests.RequestException as e:
        logger.error(f"[{get_current_time()}] Error while fetching campaigns: {e}")
        return ""

def update_campaign_budget(campaign_id: str, access_token: str, new_daily_budget: int) -> bool:
    """
    Update the daily budget of a campaign.
    """
    url = f"{FACEBOOK_GRAPH_URL}/{campaign_id}"
    payload = {
        "daily_budget": str(new_daily_budget),
        "access_token": access_token
    }

    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()
        logger.info(f"[{get_current_time()}] Updated budget for campaign {campaign_id} to {new_daily_budget}")
        return True

    except requests.RequestException as e:
        logger.error(f"[{get_current_time()}] Failed to update budget for campaign {campaign_id}: {e}")
        return False

@shared_task
def update_budget_by_campaign_name(ad_account_id: str, campaign_name: str, new_budget_dollars: float, access_token: str, user_id, item_name: str = None, campaign_code: str = None) -> str:
    """
    Celery task to find a campaign by name components and update its daily budget.
    User input is in regular pesos (e.g., 300 or 300.00), and will be converted to centavos.
    """
    start_msg = f"⏳ Starting budget update for campaign: '{campaign_name}' (Item: {item_name}, Code: {campaign_code}) to ₱{new_budget_dollars:.2f}"
    append_redis_message_editbudget(user_id, start_msg)

    campaign_id = find_campaign_id_by_components(ad_account_id, access_token, campaign_name, item_name, campaign_code, user_id=user_id)

    if not campaign_id:
        # The detailed error message is already logged in find_campaign_id_by_components
        error_msg = f"❌ STRICT MODE: No exact match found under ad account {ad_account_id} for page_name: '{campaign_name}', item_name: '{item_name}', campaign_code: '{campaign_code}'. Check the logs for specific mismatch details."
        append_redis_message_editbudget(user_id, error_msg)
        return error_msg

    try:
        new_daily_budget = convert_to_minor_units(new_budget_dollars, user_id=user_id)
    except ValueError as ve:
        error_msg = str(ve)
        append_redis_message_editbudget(user_id, error_msg)
        return error_msg

    success = update_campaign_budget(campaign_id, access_token, new_daily_budget)

    if success:
        result_msg = f"[{get_current_time()}] ✅ Budget for campaign '{campaign_name}' (ID: {campaign_id}) updated to ₱{new_budget_dollars:.2f}"
        append_redis_message_editbudget(user_id, result_msg)
        return result_msg
    else:
        error_msg = f"❌ Failed to update budget for campaign '{campaign_name}' (ID: {campaign_id})"
        append_redis_message_editbudget(user_id, error_msg)
        return error_msg