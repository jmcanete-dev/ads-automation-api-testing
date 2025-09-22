import json
import logging
import requests
import pytz
from datetime import datetime
from celery import shared_task
import re

# Assuming your models and a new redis logger are accessible
from models.models import PHRegionTable, PHCityTable
from workers.on_off_functions.edit_location_message import append_redis_message_editlocation

# Constants
FACEBOOK_GRAPH_URL = "https://graph.facebook.com/v23.0"
manila_tz = pytz.timezone("Asia/Manila")

# Logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def get_current_time():
    """Get current time in Manila timezone"""
    return datetime.now(manila_tz).strftime("%Y-%m-%d %H:%M:%S")

def parse_campaign_name(campaign_name: str) -> dict:
    """
    Parse campaign name into components: page_name, item_name, campaign_code (pattern: P followed by digits, e.g., P199)
    Example: "HiganteWash-AdonisWash-W04162025ANDREA1Cb-P100" or "Marisse-OrganicHairOil-OHO04112025ARREN1Cb-so2-P199"
    Returns: {"page_name": ..., "item_name": ..., "campaign_code": ...}
    """
    if not campaign_name:
        return {"page_name": "", "item_name": "", "campaign_code": ""}
    parts = campaign_name.split('-')
    if len(parts) < 2:
        return {"page_name": campaign_name, "item_name": "", "campaign_code": ""}
    page_name = parts[0].strip()
    item_name = parts[1].strip()
    campaign_code = ""
    # Search for campaign_code pattern in the rest
    for part in parts[2:]:
        if re.match(r"^P\d+$", part):
            campaign_code = part
            break
    return {
        "page_name": page_name,
        "item_name": item_name,
        "campaign_code": campaign_code
    }

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

def get_location_keys(location_names: list[str]) -> tuple[list[dict], list[dict]]:
    """
    Queries the database to get region and city keys from a list of names.
    """
    region_keys = [{"key": str(r.region_key)} for r in PHRegionTable.query.filter(PHRegionTable.region_name.in_(location_names)).all()]
    city_keys = [{"key": str(c.city_key)} for c in PHCityTable.query.filter(PHCityTable.city_name.in_(location_names)).all()]
    logger.info(f"DB Query: Found {len(region_keys)} region keys and {len(city_keys)} city keys.")
    return region_keys, city_keys

def find_campaign_id_by_components(ad_account_id: str, access_token: str, input_page_name: str, input_item_name: str = None, input_campaign_code: str = None) -> str:
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
        if best_match:
            error_details = []
            if not best_match_details["page_match"]:
                error_details.append(f"page_name: expected '{input_page_name}' not found")
            if not best_match_details["item_match"]:
                error_details.append(f"item_name: expected '{input_item_name}' not found")
            if not best_match_details["code_match"]:
                error_details.append(f"campaign_code: expected '{input_campaign_code}' not found")
            logger.warning(f"[{get_current_time()}] FLEXIBLE MODE: No exact match. Closest match '{best_match_details['campaign_name']}' has mismatches: {', '.join(error_details)}")
        else:
            logger.warning(f"[{get_current_time()}] FLEXIBLE MODE: No campaigns found in account {ad_account_id}")
        return ""
    except requests.RequestException as e:
        logger.error(f"[{get_current_time()}] Error while fetching campaigns: {e}")
        return ""

def find_ad_set_ids_by_campaign_id(campaign_id: str, access_token: str) -> list[str]:
    """
    Get all ad set IDs from a specific campaign.
    """
    url = f"{FACEBOOK_GRAPH_URL}/{campaign_id}/adsets"
    params = {
        "fields": "id",
        "limit": 1000,
        "access_token": access_token
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        ad_set_ids = [ad_set["id"] for ad_set in data.get("data", [])]
        logger.info(f"[{get_current_time()}] Found {len(ad_set_ids)} ad sets in campaign {campaign_id}")
        return ad_set_ids

    except requests.RequestException as e:
        logger.error(f"[{get_current_time()}] Error fetching ad sets: {e}")
        return []

def update_ad_set_targeting(ad_set_id: str, access_token: str, targeting_payload: dict) -> bool:
    """
    Updates the targeting of a single ad set.
    """
    url = f"{FACEBOOK_GRAPH_URL}/{ad_set_id}"
    payload = {"targeting": json.dumps(targeting_payload), "access_token": access_token}
    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()
        return True
    except requests.RequestException:
        return False

@shared_task
def update_locations_by_campaign_components(user_id: str, ad_account_id: str, access_token: str, page_name: str, new_regions_city: list[str], item_name: str = None, campaign_code: str = None) -> str:
    """
    Celery task to update locations for all ad sets in a campaign identified by strict component matching.
    """
    start_msg = f"[{get_current_time()}] ⏳ Processing location update for page: '{page_name}' (Item: {item_name}, Code: {campaign_code})..."
    append_redis_message_editlocation(user_id, start_msg)

    # Step 1: Find campaign ID using strict component matching
    campaign_id = find_campaign_id_by_components(ad_account_id, access_token, page_name, item_name, campaign_code)
    
    if not campaign_id:
        error_msg = f"[{get_current_time()}] ❌ STRICT MODE: No exact match found under ad account {ad_account_id} for page_name: '{page_name}', item_name: '{item_name}', campaign_code: '{campaign_code}'. Check the logs for specific mismatch details."
        append_redis_message_editlocation(user_id, error_msg)
        return error_msg

    # Step 2: Get all ad set IDs from the matched campaign
    ad_set_ids = find_ad_set_ids_by_campaign_id(campaign_id, access_token)
    if not ad_set_ids:
        error_msg = f"[{get_current_time()}] ❌ No ad sets found in campaign {campaign_id}."
        append_redis_message_editlocation(user_id, error_msg)
        return error_msg
    
    append_redis_message_editlocation(user_id, f"[{get_current_time()}] Found {len(ad_set_ids)} ad sets to update.")

    # Step 3: Translate location names to API keys
    region_keys, city_keys = get_location_keys(new_regions_city)
    if not region_keys and not city_keys:
        error_msg = f"[{get_current_time()}] ❌ Could not find valid location keys for the names provided."
        append_redis_message_editlocation(user_id, error_msg)
        return error_msg
        
    # Step 4: Build the targeting payload once
    cities_with_radius = [{"key": c["key"], "radius": 25, "distance_unit": "mile"} for c in city_keys]
    targeting_payload = {
        "geo_locations": {"countries": ["PH"]},
        "excluded_geo_locations": {"regions": region_keys, "cities": cities_with_radius}
    }

    # Step 5: Loop through ad sets and update them
    success_count = 0
    failure_count = 0
    for ad_set_id in ad_set_ids:
        if update_ad_set_targeting(ad_set_id, access_token, targeting_payload):
            success_count += 1
            append_redis_message_editlocation(user_id, f"[{get_current_time()}]  → Successfully updated ad set {ad_set_id}")
        else:
            failure_count += 1
            append_redis_message_editlocation(user_id, f"[{get_current_time()}]  → ❌ Failed to update ad set {ad_set_id}")

    # Step 6: Final report
    final_msg = f"[{get_current_time()}] ✅ Finished. Successfully updated {success_count} ad sets. Failed to update {failure_count}."
    append_redis_message_editlocation(user_id, final_msg)
    return final_msg