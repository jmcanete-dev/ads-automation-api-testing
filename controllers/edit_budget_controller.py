import redis
import json
from datetime import datetime
import pytz
from flask import request, jsonify
from workers.edit_budget_worker import update_budget_by_campaign_name

# Redis client for budget update websocket/logging
redis_websocket_budget = redis.Redis(
    host="redisAds",
    port=6379,
    db=8,
    decode_responses=True
)

def edit_budget(data):
    data = request.get_json()

    ad_account_id = data.get("ad_account_id")
    campaign_name = data.get("campaign_name")
    new_budget = data.get("new_budget")  # can be 300 or 300.00
    access_token = data.get("access_token")
    user_id = data.get("user_id")
    item_name = data.get("item_name")  # New parameter
    campaign_code = data.get("campaign_code")  # New parameter

    if not all([ad_account_id, campaign_name, new_budget, access_token, user_id]):
        return jsonify({"error": "Missing one or more required fields"}), 400

    websocket_key = f"{user_id}-key"
    if not redis_websocket_budget.exists(websocket_key):
        redis_websocket_budget.set(websocket_key, json.dumps({"message": ["Budget update initiated"]}), ex=3600)

    try:
        task = update_budget_by_campaign_name.apply_async(
            args=[ad_account_id, campaign_name, new_budget, access_token, user_id, item_name, campaign_code],
            countdown=0
        )

        result = task.get(timeout=300)

    except Exception as e:
        return jsonify({"error": f"Task failed or timed out: {str(e)}"}), 500

    updated_at = datetime.now(pytz.timezone("Asia/Manila")).isoformat()

    return jsonify({
        "message": result,
        "data_updated_at": updated_at
    }), 200