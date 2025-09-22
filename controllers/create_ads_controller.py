# controllers/create_campaign_controller.py
import logging
from flask import json
import requests
from datetime import datetime, timedelta
import pytz
import requests
import time


# Helper function to make requests to Facebook API
def make_facebook_api_request(url, headers, data):
    response = requests.post(url, headers=headers, json=data)
    return response.json()


def create_campaign(ad_account_id, access_token, campaign_name, daily_budget):
    # Force the objective to always be 'OUTCOME_ENGAGEMENT'
    formatted_objective = "OUTCOME_ENGAGEMENT"

    # Validate that the daily budget is a positive integer
    if not isinstance(daily_budget, int) or daily_budget <= 0:
        raise ValueError(
            "The daily budget must be a positive integer representing the budget in cents (e.g., 5000 for $50).")

    # No multiplication, assume budget is already in the correct format
    adjusted_daily_budget = daily_budget * 10

    # Set the Facebook API URL for creating the campaign
    url = f"https://graph.facebook.com/v23.0/act_{ad_account_id}/campaigns"

    # Set the headers for the API request
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    # Define the request body to create the campaign
    campaign_data = {
        "name": campaign_name,
        "objective": formatted_objective,  # Fixed objective
        "special_ad_categories": [],
        "daily_budget": str(adjusted_daily_budget),
        "bid_strategy": "LOWEST_COST_WITHOUT_CAP",
        "status": "ACTIVE"
    }

    # Make the request to the Facebook API and return the response
    return make_facebook_api_request(url, headers, campaign_data)

def create_adset(
    ad_account_id,
    access_token,
    campaign_id,
    adset_name,
    start_time,
    interests=None,
    excluded_regions=None  # Add excluded_regions as a parameter
):
    """Creates a Facebook Ad Set with automatic retries for transient errors (code: 2)."""

    url = f"https://graph.facebook.com/v23.0/act_{ad_account_id}/adsets"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    if not start_time:
        # Default start time to next day in Manila timezone
        manila_tz = pytz.timezone("Asia/Manila")
        start_time = (datetime.now(manila_tz) + timedelta(days=1)).strftime('%Y-%m-%dT00:00:00+0800')

    logging.info(f"START TIME IN CREATE ADSET: {start_time}")
    # Separate interests into categories
    interest_ids, demographics_ids, behavior_ids = [], [], []
    if interests:
        for interest in interests:
            if "type" in interest:
                if "Interests" in interest["type"]:
                    interest_ids.append({"id": interest["id"]})
                elif "Demographics" in interest["type"]:
                    demographics_ids.append({"id": interest["id"]})
                elif "Behaviors" in interest["type"]:
                    behavior_ids.append({"id": interest["id"]})

    # Build flexible_spec
    flexible_spec = []
    if interest_ids:
        flexible_spec.append({"interests": interest_ids})
    if demographics_ids:
        flexible_spec.append({"demographics": demographics_ids})
    if behavior_ids:
        flexible_spec.append({"behaviors": behavior_ids})

    logging.info(f"ADSET NAME: {adset_name}")

    # Prepare excluded regions only if provided
    excluded_geo_locations = None
    if excluded_regions:
        excluded_geo_locations = {
            "regions": excluded_regions,
            "location_types": ["home", "recent"]
        }
        logging.info(f"Excluded Geo Locations: {excluded_geo_locations}")

    # Ad set data payload
    adset_data = {
        "name": adset_name,
        "status": "ACTIVE",
        "start_time": start_time,
        "campaign_id": campaign_id,
        "targeting": {
            "age_max": 65,
            "age_min": 18,
            "geo_locations": {
                "countries": ["PH"],
                "location_types": ["home", "recent"]
            },
            "brand_safety_content_filter_levels": ["FACEBOOK_STANDARD", "AN_STANDARD"],
            "targeting_automation": {"advantage_audience": 1}
        },
        "billing_event": "IMPRESSIONS",
        "optimization_goal": "CONVERSATIONS",
        "destination_type": "MESSENGER",
    }

    # Include excluded_geo_locations only if it's provided
    if excluded_geo_locations:
        adset_data["targeting"]["excluded_geo_locations"] = excluded_geo_locations
    
    if flexible_spec:
        adset_data["targeting"]["flexible_spec"] = flexible_spec

    # Retry logic 
    max_retries = 30
    general_delay = 10  # 10-second delay for normal retries
    cooldown_delay = 60  # 1-minute (60-second) cooldown for transient errors (code: 2)

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(url, headers=headers, json=adset_data)
            response_data = response.json()

            # If request is successful, return response
            if response.status_code == 200:
                return response_data

            # Handle specific errors
            error_details = response_data.get("error", {})
            error_code = error_details.get("code")

            if error_code == 2:  # Transient error, wait 1 minute before retrying
                logging.warning(f"Transient error detected (code: 2). Cooling down for 1 minute... [{attempt}/{max_retries}]")
                time.sleep(cooldown_delay)
            else:  # Other errors, log and retry after 10 seconds
                logging.error(f"Request failed: {response_data} [{attempt}/{max_retries}]")
                time.sleep(general_delay)

        except requests.exceptions.RequestException as req_err:
            logging.error(f"Request error occurred: {req_err} [{attempt}/{max_retries}]")
            time.sleep(general_delay)

    logging.error("Max retries reached. Ad set creation failed.")

def create_ad_creative(ad_account_id, access_token, name, page_id, video_id, title, message, image_url_from_fb):
    """Create a Facebook Ad Creative with automatic spec switching and retry handling."""
    
    MAX_RETRIES = 10
    RETRY_DELAY = 60  # seconds
    url = f"https://graph.facebook.com/v23.0/act_{ad_account_id}/adcreatives"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    # Base creative data
    ad_creative_data_base = {
        "name": name,
        "object_story_spec": {
            "page_id": page_id,
            "video_data": {
                "video_id": video_id,
                "title": title,
                "message": message,
                "call_to_action": {
                    "type": "MESSAGE_PAGE",
                    "value": {
                        "app_destination": "MESSENGER",
                        "link": "https://fb.com/messenger_doc/"
                    }
                },
                "image_url": image_url_from_fb
            }
        }
    }

    # Different specs to try
    advantage_plus_spec = {
        "degrees_of_freedom_spec": {
            "creative_features_spec": {
                "advantage_plus_creative": {"enroll_status": "OPT_IN"},
                "enhance_cta": {
                    "enroll_status": "OPT_IN",
                    "customizations": {"text_extraction": {"enroll_status": "OPT_IN"}}
                },
                "inline_comment": {"enroll_status": "OPT_IN"},
                "pac_relaxation": {"enroll_status": "OPT_IN"},
                "text_optimizations": {
                    "enroll_status": "OPT_IN",
                    "customizations": {"text_extraction": {"enroll_status": "OPT_IN"}}
                }
            }
        }
    }

    fallback_spec = {
        "degrees_of_freedom_spec": {
            "creative_features_spec": {
                "advantage_plus_creative": {"enroll_status": "OPT_IN"},
                "enhance_cta": {
                    "enroll_status": "OPT_IN",
                    "customizations": {"text_extraction": {"enroll_status": "OPT_IN"}}
                },
                "inline_comment": {"enroll_status": "OPT_IN"},
                "pac_relaxation": {"enroll_status": "OPT_IN"},
                "standard_enhancements": {"enroll_status": "OPT_IN"},
                "text_optimizations": {
                    "enroll_status": "OPT_IN",
                    "customizations": {"text_extraction": {"enroll_status": "OPT_IN"}}
                }
            }
        }
    }

    standard_enhancements_spec = {
        "degrees_of_freedom_spec": {
            "creative_features_spec": {
                "standard_enhancements": {"enroll_status": "OPT_OUT"}
            }
        }
    }

    specs_to_try = [advantage_plus_spec, fallback_spec, standard_enhancements_spec]

    # Function to send requests with retries for transient errors
    def send_request(payload):
        manila_tz = pytz.timezone('Asia/Manila')

        for attempt in range(1, MAX_RETRIES + 1):
            response = requests.post(url, headers=headers, json=payload)
            response_data = response.json()

            if response.status_code == 200:
                return response_data  # Success!

            error = response_data.get("error", {})
            error_code = error.get("code", 0)
            error_message = error.get("message", "Unknown error")

            # Log error message
            timestamp = datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')
            log_message = f"[{timestamp}] Attempt {attempt}/{MAX_RETRIES}: Failed to create ad creative. Error: {error_message} (Code: {error_code})"
            logging.warning(log_message)

            # Retry for transient errors (code 2)
            if error_code == 2:
                time.sleep(RETRY_DELAY)
                continue  # Retry again

            return response_data  # Return non-retryable errors immediately

        return {"error": "Max retries reached", "details": response_data}

    # Try each spec in sequence
    for spec in specs_to_try:
        ad_creative_data = {**ad_creative_data_base, **spec}
        result = send_request(ad_creative_data)

        # If there's no error, return success
        if "error" not in result:
            return result

        error_code = result.get("error", {}).get("code", 0)

        # If error is 100 (invalid spec), try the next one
        if error_code == 100:
            logging.warning("Spec error (code 100). Trying next spec...")
            continue  # Move to the next spec

        # If another error occurs, log and return it
        logging.error(f"Ad creative creation failed: {result}")
        return result

    # If all attempts failed, return the last error
    return {"error": "All spec attempts failed", "details": result}

def create_ad(ad_account_id, access_token, name, adset_id, creative_id):
    """
    Function to create an ad in Facebook Ad Manager with retry handling for transient errors.

    :param ad_account_id: Facebook Ad Account ID
    :param access_token: Access token for API authentication
    :param name: Name of the ad
    :param adset_id: ID of the Ad Set the ad belongs to
    :param creative_id: ID of the Creative to be used for the ad
    :return: Response from the Facebook API as a JSON object
    """
    MAX_RETRIES = 10
    RETRY_DELAY = 60
    url = f"https://graph.facebook.com/v23.0/act_{ad_account_id}/ads"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    ad_data = {
        "name": name,
        "adset_id": adset_id,
        "creative": {"creative_id": creative_id},
        "status": "ACTIVE"
    }

    manila_tz = pytz.timezone('Asia/Manila')

    for attempt in range(1, MAX_RETRIES + 1):
        response = requests.post(url, headers=headers, json=ad_data)
        response_data = response.json()

        if response.status_code == 200:
            return response_data  # Success!

        error = response_data.get("error", {})
        error_code = error.get("code", 0)
        error_message = error.get("message", "Unknown error")

        # Log error
        timestamp = datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')
        log_message = f"[{timestamp}] Attempt {attempt}/{MAX_RETRIES}: Failed to create ad. Error: {error_message} (Code: {error_code})"
        logging.warning(log_message)

        # Retry if the error code is 2 (transient error)
        if error_code == 2:
            time.sleep(RETRY_DELAY)
            continue  # Retry again

        return response_data  # Return non-retryable errors immediately

    return {"error": "Max retries reached", "details": response_data}


def create_ad_usepost(ad_account_id, access_token, name, adset_id, object_story_id):
    """
    Function to create an ad in Facebook Ad Manager using an object_story_id with retry handling for transient errors.

    :param ad_account_id: Facebook Ad Account ID
    :param access_token: Access token for API authentication
    :param name: Name of the ad
    :param adset_id: ID of the Ad Set the ad belongs to
    :param object_story_id: ID of the object story to be used for the ad
    :return: Response from the Facebook API as a JSON object
    """
    MAX_RETRIES = 10
    RETRY_DELAY = 60

    url = f"https://graph.facebook.com/v23.0/act_{ad_account_id}/ads"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    ad_data = {
        "name": name,
        "adset_id": adset_id,
        "creative": {"object_story_id": object_story_id},
        "status": "ACTIVE"
    }

    manila_tz = pytz.timezone('Asia/Manila')

    for attempt in range(1, MAX_RETRIES + 1):
        response = requests.post(url, headers=headers, json=ad_data)
        response_data = response.json()

        if response.status_code == 200:
            return response_data  # Success!

        error = response_data.get("error", {})
        error_code = error.get("code", 0)
        error_message = error.get("message", "Unknown error")

        # Log error
        timestamp = datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')
        log_message = f"[{timestamp}] Attempt {attempt}/{MAX_RETRIES}: Failed to create ad. Error: {error_message} (Code: {error_code})"
        logging.warning(log_message)

        # Retry if the error code is 2 (transient error)
        if error_code == 2:
            time.sleep(RETRY_DELAY)
            continue  # Retry again

        return response_data  # Return non-retryable errors immediately

    return {"error": "Max retries reached", "details": response_data}

def get_best_interests_for_keywords(access_token, interest_keywords):
    """
    Function to fetch the best matching interest for up to three provided interest keywords.
    If a keyword doesn't return data, retries with another keyword to find a suitable match.
    Ensures no duplicate interest ID-name pairs are returned.

    :param access_token: Authorization Bearer token
    :param interest_keywords: List of up to three interest keywords
    :return: List of best matching interest ID and name for each keyword
    """
    try:
        # Ensure we have up to three interest keywords
        if not isinstance(interest_keywords, list) or len(interest_keywords) > 3:
            raise ValueError("Provide up to 3 interest keywords as a list")

        # Prepare the result dictionary to hold the best match for each keyword
        best_matches = []
        seen_ids = set()  # To avoid duplicate interest ID-name pairs

        # Define the interest type
        interest_type = "adinterestsuggestion"

        # Helper function to call the API and get interest suggestions
        def fetch_interests_for_keyword(keyword):
            url = "https://graph.facebook.com/v23.0/search"
            params = {
                # JSON-encoded string
                'interest_list': f'[{json.dumps(keyword)}]',
                'type': interest_type
            }
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                return response.json().get('data', [])
            return []

        # Loop through each interest keyword
        for keyword in interest_keywords:
            # Fetch suggestions for the current keyword
            suggestions = fetch_interests_for_keyword(keyword)

            # If no suggestions are found, try fetching suggestions for another keyword
            if not suggestions:
                for fallback_keyword in interest_keywords:
                    if fallback_keyword != keyword:
                        suggestions = fetch_interests_for_keyword(
                            fallback_keyword)
                        if suggestions:
                            break

            # If suggestions are available, find the best match not already selected
            for suggestion in suggestions:
                interest_id = suggestion.get('id')
                interest_name = suggestion.get('name')
                if interest_id not in seen_ids:
                    seen_ids.add(interest_id)  # Mark as seen
                    best_matches.append({
                        "input_keyword": keyword,
                        "interest_name": interest_name,
                        "interest_id": interest_id
                    })
                    break  # Add only one match per keyword

        return best_matches

    except Exception as e:
        raise e
