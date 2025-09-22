import redis
import json
from datetime import datetime
import pytz
from flask import request, jsonify

# Import the new worker task
from workers.edit_location_worker import update_locations_by_campaign_components

# Redis client for location update websocket/logging
# Using a different DB (e.g., db=9) to keep logs separate
redis_websocket_location = redis.Redis(
    host="redisAds",
    port=6379,
    db=9,
    decode_responses=True
)

def edit_locations(data):
    """
    Controller to handle the request for updating ad set locations based on a page name.
    """
    data = request.get_json()

    # Extract required fields from the request body
    user_id = data.get("user_id")
    ad_account_id = data.get("ad_account_id")
    access_token = data.get("access_token")
    page_name = data.get("page_name")
    new_regions_city = data.get("new_regions_city") # This is an array of names
    item_name = data.get("item_name")  # New parameter
    campaign_code = data.get("campaign_code")  # New parameter

    # Validate the input
    if not all([user_id, ad_account_id, access_token, page_name, new_regions_city]):
        return jsonify({"error": "Missing one or more required fields"}), 400
    
    if not isinstance(new_regions_city, list):
        return jsonify({"error": "'new_regions_city' must be an array of location names"}), 400

    # Set up Redis key for real-time logging
    websocket_key = f"{user_id}-location-key"
    if not redis_websocket_location.exists(websocket_key):
        redis_websocket_location.set(websocket_key, json.dumps({"message": ["Location update initiated"]}), ex=3600)

    try:
        # Asynchronously call the new Celery worker task
        task = update_locations_by_campaign_components.apply_async(
            args=[user_id, ad_account_id, access_token, page_name, new_regions_city, item_name, campaign_code],
            countdown=0
        )

        # Wait for the task to complete and get the result
        result = task.get(timeout=600) # Increased timeout for potentially long-running task

    except Exception as e:
        return jsonify({"error": f"Task failed or timed out: {str(e)}"}), 500

    # Get the current timestamp for the response
    updated_at = datetime.now(pytz.timezone("Asia/Manila")).isoformat()

    return jsonify({
        "message": result,
        "data_updated_at": updated_at
    }), 200