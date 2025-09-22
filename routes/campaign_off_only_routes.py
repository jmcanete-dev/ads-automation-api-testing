from datetime import datetime, timedelta
import json
from flask import Blueprint, request, jsonify
import pytz
import redis
from controllers.campaign_off_only_controller import (
    add_schedule_logic,
    remove_schedule_time_logic,
    append_schedule_logic,
    delete_schedule_logic,
    edit_schedule_logic
)
from models.models import User, db, CampaignOffOnly
manila_tz = pytz.timezone("Asia/Manila")
redis_websocket = redis.Redis(host="redisAds", port=6379, db=11, decode_responses=True)

schedule_campaign_only_bp = Blueprint("schedule", __name__)

@schedule_campaign_only_bp.route("create-campaign-only", methods=["POST"])
def add_schedule():
    data = request.json
    return add_schedule_logic(data)

@schedule_campaign_only_bp.route("remove-schedule", methods=["DELETE"])
def remove_schedule():
    data = request.json
    return remove_schedule_time_logic(data)

@schedule_campaign_only_bp.route("edit-time", methods=["PUT"])
def edit_schedule():
    data = request.json
    return edit_schedule_logic(data)

@schedule_campaign_only_bp.route("add-campaign-only", methods=["PUT"])
def append_schedule():
    data = request.json
    return append_schedule_logic(data)

@schedule_campaign_only_bp.route("delete-campaign-only", methods=["DELETE"])
def delete_schedule():
    data = request.json
    return delete_schedule_logic(data)

@schedule_campaign_only_bp.route("get-campaign-only", methods=["GET"])
def get_campaign_schedules():
    user_id = request.args.get("user_id")

    if not user_id:
        return jsonify({"error": "Missing required query parameter: user_id"}), 400

    user_schedules = CampaignOffOnly.query.filter_by(user_id=user_id).all()

    # Get current time in Manila timezone
    now = datetime.now(manila_tz)
    
    # Get next midnight (12:00 AM Manila time)
    tomorrow_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    
    # Convert to UNIX timestamp
    expire_timestamp = int(tomorrow_midnight.timestamp())

    ad_accounts = []
    
    for schedule in user_schedules:
        ad_account_id = schedule.ad_account_id or "N/A"
        redis_key = f"{user_id}-{ad_account_id}-key"

        # Check if the key exists in Redis
        if not redis_websocket.exists(redis_key):
            # Create default Redis entry with only a "message" field
            redis_websocket.set(redis_key, json.dumps({"message": [f"Last Check Message: {schedule.last_check_message}"] or ["No recent activity"]}))
            redis_websocket.expireat(redis_key, expire_timestamp)  # Expire at 12:00 AM Manila time

        ad_accounts.append({
            "ad_account_id": ad_account_id,
            "schedule_data": schedule.schedule_data or {},
            "access_token": schedule.access_token or "N/A",
            "last_time_checked": schedule.last_time_checked.strftime("%Y-%m-%d %H:%M:%S") if schedule.last_time_checked else "N/A",
            "last_status": schedule.last_check_status or "Scheduled",
            "last_message": schedule.last_check_message or "No recent activity"
        })

    return jsonify({
        "user_id": user_id,
        "ad_accounts": ad_accounts
    }), 200