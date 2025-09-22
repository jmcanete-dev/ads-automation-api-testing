import json
from flask import Blueprint, request, jsonify
import pytz
import redis
from controllers.scheduler_controller import (
    add_schedule_logic, 
    append_schedule_logic, 
    delete_schedule_logic, 
    edit_schedule_campaign_logic, 
    remove_schedule_time_logic, 
    pause_schedule_campaign_logic,
    delete_all_schedules_logic
)
from models.models import User, db, CampaignsScheduled
from datetime import datetime, timedelta

schedule_bp = Blueprint("schedule_bp", __name__)

redis_websocket = redis.Redis(host="redisAds", port=6379, db=10, decode_responses=True)
manila_tz = pytz.timezone("Asia/Manila")

@schedule_bp.route("/create-campaign-schedule", methods=["POST"])
def add_schedule():
    data = request.get_json()
    response, status_code = add_schedule_logic(data)
    return jsonify(response), status_code

@schedule_bp.route("/add-schedule", methods=["PUT"])
def append_schedule():
    data = request.get_json()
    response, status_code = append_schedule_logic(data)
    return jsonify(response), status_code

@schedule_bp.route("/remove-schedule-time", methods=["POST"])
def remove_schedule_time():
    data = request.get_json()
    response, status_code = remove_schedule_time_logic(data)
    return jsonify(response), status_code

@schedule_bp.route("/delete-schedule", methods=["POST"])
def delete_schedule():
    data = request.get_json()
    response, status_code = delete_schedule_logic(data)
    return jsonify(response), status_code

@schedule_bp.route("/delete-all-schedules", methods=["POST"])
def delete_all_schedules():
    data = request.get_json()
    response, status_code = delete_all_schedules_logic(data)
    return jsonify(response), status_code

@schedule_bp.route("/edit-schedule", methods=["PUT"])
def edit_schedule_campaign():
    data = request.get_json()
    response, status_code = edit_schedule_campaign_logic(data)
    return jsonify(response), status_code

@schedule_bp.route("/pause-schedule", methods=["PUT"])
def pause_schedule_campaign():
    data = request.get_json()
    response, status_code = pause_schedule_campaign_logic(data)
    return jsonify(response), status_code

# Get schedule for a specific ad_account_id using query params
@schedule_bp.route("/get-campaign-schedule", methods=["GET"])
def get_schedule():
    ad_account_id = request.args.get("ad_account_id")

    if not ad_account_id:
        return jsonify({"error": "Missing required query parameter: ad_account_id"}), 400

    existing_schedule = CampaignsScheduled.query.filter_by(ad_account_id=ad_account_id).first()
    
    if not existing_schedule:
        return jsonify({"error": f"No schedule found for ad_account_id {ad_account_id}"}), 404

    return jsonify({
        "ad_account_id": ad_account_id,
        "user_id": existing_schedule.user_id,
        "schedule_data": existing_schedule.schedule_data
    }), 200

@schedule_bp.route("/get-user-ad-accounts", methods=["GET"])
def get_user_ad_accounts():
    user_id = request.args.get("user_id")

    if not user_id:
        return jsonify({"error": "Missing required query parameter: user_id"}), 400

    user_schedules = CampaignsScheduled.query.filter_by(user_id=user_id).all()

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