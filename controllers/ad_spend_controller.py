import redis
import json
from datetime import datetime
import pytz
from flask import request, jsonify
from workers.ad_spent_worker import fetch_ad_spend_data  # adjust import as needed

# Redis client for websocket messages
redis_websocket_asr = redis.Redis(
    host="redisAds",
    port=6379,
    db=9,
    decode_responses=True
)

def ad_spent(data):
    data = request.get_json()

    access_token = data.get("access_token")
    user_id = data.get("user_id")

    if not (user_id and access_token):
        return jsonify({"error": "Missing required fields"}), 400


    websocket_key = f"{user_id}-key"
    if not redis_websocket_asr.exists(websocket_key):
        # Set with expiry of 1 hour (3600 seconds), adjust if needed
        redis_websocket_asr.set(websocket_key, json.dumps({"message": ["User-Id Created"]}), ex=3600)

    try:
        task = fetch_ad_spend_data.apply_async(args=[user_id, access_token], countdown=0)

        # Wait for the task result, timeout in seconds
        campaign_spending_info = task.get(timeout=300)  

    except Exception as e:
        return jsonify({"error": f"Task failed or timed out: {str(e)}"}), 500

    if isinstance(campaign_spending_info, dict) and campaign_spending_info.get("error"):
        return jsonify({"error": campaign_spending_info["error"]}), 400

    updated_at = datetime.now(pytz.timezone("Asia/Manila")).isoformat()

    return jsonify({
        "campaign_spending_data": campaign_spending_info,
        "data_updated_at": updated_at
    }), 200
