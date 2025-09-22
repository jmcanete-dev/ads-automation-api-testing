import json
from flask import jsonify
import redis
from sqlalchemy.orm.attributes import flag_modified
from models.models import User, db, CampaignOffOnly
from datetime import datetime
import pytz

manila_tz = pytz.timezone("Asia/Manila")

redis_websocket = redis.Redis(
    host="redisAds",
    port=6379,
    db=11,  # Updated to Redis DB 11
    decode_responses=True
)

def add_schedule_logic(data):
    ad_account_id = data.get("ad_account_id")
    user_id = data.get("user_id")
    access_token = data.get("access_token")
    schedule_data = data.get("schedule_data")  # List of campaigns with cpp_metric and on_off

    if not (ad_account_id and user_id and access_token and schedule_data):
        return {"error": "Missing required fields"}, 400

    validated_schedule_data = {}

    # Validate schedule input
    for index, schedule in enumerate(schedule_data, start=1):
        campaign_names = schedule.get("campaign_name")

        # Ensure campaign_names is always a list
        if not isinstance(campaign_names, list):
            campaign_names = [campaign_names]

        # Validate uniqueness within the same schedule entry
        if len(campaign_names) != len(set(campaign_names)):
            return {
                "error": f"Duplicate campaign names within the same schedule entry: {campaign_names}"
            }, 400

        if schedule["on_off"] not in ["ON", "OFF"]:
            return {"error": f"Invalid on_off value for {campaign_names}. Use 'ON' or 'OFF'"}, 400

        validated_schedule_data[f"time{index}"] = {
            "time": schedule["time"],
            "campaign_name": campaign_names,
            "on_off": schedule["on_off"],
            "status": schedule.get("status", "Running")
        }

    # Validate user existence
    user_exists = User.query.filter_by(id=user_id).first()
    if not user_exists:
        return {"error": f"User with user_id {user_id} does not exist"}, 400

    existing_schedule = CampaignOffOnly.query.filter_by(ad_account_id=ad_account_id).first()

    if existing_schedule:
        existing_schedule.schedule_data = validated_schedule_data
        flag_modified(existing_schedule, "schedule_data")  # Notify SQLAlchemy that JSON field changed
        message = "Schedule updated successfully."
    else:
        existing_schedule = CampaignOffOnly(
            ad_account_id=ad_account_id,
            user_id=user_id,
            access_token=access_token,
            schedule_data=validated_schedule_data,
            added_at=datetime.now(manila_tz),
            last_time_checked=datetime.now(manila_tz),
            last_check_status="Success",
            last_check_message="Scheduled",
        )
        db.session.add(existing_schedule)
        message = "New schedule added"

    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return {"error": f"Database error: {str(e)}"}, 500

    return {
        "message": message,
        "updated_schedule": existing_schedule.schedule_data,
    }, 201

def append_schedule_logic(data):
    ad_account_id = data.get("ad_account_id")
    user_id = data.get("id")
    access_token = data.get("access_token")
    new_schedule_data = data.get("schedule_data")

    if not (ad_account_id and user_id and access_token and new_schedule_data):
        return {"error": "Missing required fields"}, 400

    # Prevent duplicate campaigns within the incoming request data
    unique_campaigns = set()
    for schedule in new_schedule_data:
        campaign_names = schedule.get("campaign_name")

        # Ensure campaign_names is always a list
        if not isinstance(campaign_names, list):
            campaign_names = [campaign_names]

        for campaign_name in campaign_names:
            if campaign_name in unique_campaigns:
                return {"error": f"Duplicate campaign name in request: {campaign_name}"}, 400
            unique_campaigns.add(campaign_name)

        if schedule["on_off"] not in ["ON", "OFF"]:
            return {"error": f"Invalid on_off for {campaign_names}. Use 'ON' or 'OFF'"}, 400

    # Fetch existing schedule
    existing_schedule = CampaignOffOnly.query.filter_by(ad_account_id=ad_account_id).first()
    
    if not existing_schedule:
        return {"error": f"No schedule found for ad_account_id {ad_account_id}. Please create a schedule first."}, 404

    current_schedule_data = existing_schedule.schedule_data or {}

    # Convert existing schedule into a dictionary for quick lookup (using tuple for hashability)
    existing_campaigns_map = {
        tuple(entry["campaign_name"]): key for key, entry in current_schedule_data.items()
    }

    filtered_new_campaigns = {}

    for schedule in new_schedule_data:
        campaign_names = schedule.get("campaign_name")

        # Ensure campaign_names is always a list
        if not isinstance(campaign_names, list):
            campaign_names = [campaign_names]

        if tuple(campaign_names) in existing_campaigns_map:
            return {"error": f"Duplicate campaign {campaign_names} already exists."}, 400
        else:
            new_key = f"time{len(current_schedule_data) + len(filtered_new_campaigns) + 1}"
            filtered_new_campaigns[new_key] = {
                "time": schedule["time"],
                "campaign_name": campaign_names,
                "on_off": schedule["on_off"],
                "status": "Running" 
            }

    if not filtered_new_campaigns:
        return {"error": "No new schedule entries added (all are duplicates)."}, 400

    updated_schedule_data = {**current_schedule_data, **filtered_new_campaigns}

    if len(updated_schedule_data) > 20:
        return {"error": f"User {user_id} cannot schedule more than 20 campaigns for ad_account_id {ad_account_id}"}, 400

    existing_schedule.schedule_data = updated_schedule_data
    flag_modified(existing_schedule, "schedule_data")

    try:
        db.session.commit()
        return {
            "message": f"New campaign schedules added successfully: {', '.join(str(v['campaign_name']) for v in filtered_new_campaigns.values())}",
            "updated_schedule": existing_schedule.schedule_data
        }, 201
    except Exception as e:
        db.session.rollback()
        return {"error": f"Database error: {str(e)}"}, 500
    
def remove_schedule_time_logic(data):
    user_id = str(data.get("id"))  # Convert to string to match database format
    ad_account_id = str(data.get("ad_account_id"))  # Ensure consistent string format
    time_to_remove = str(data.get("time"))

    if not user_id or not ad_account_id:
        return jsonify({"error": "Missing required parameters: 'id' and 'ad_account_id'"}), 400

    user = User.query.filter_by(id=user_id).first()
    if not user:
        return jsonify({"error": f"User with id {user_id} not found"}), 404

    if time_to_remove is None:
        return jsonify({"error": "Missing required fields: 'time'"}), 400

    # Fetch the existing schedule
    existing_schedule = CampaignOffOnly.query.filter_by(ad_account_id=ad_account_id).first()
    
    if not existing_schedule or not existing_schedule.schedule_data:
        return jsonify({"error": f"No schedule found for ad_account_id {ad_account_id}"}), 404

    current_schedule_data = existing_schedule.schedule_data

    # Find the matching entry based on `time` and `cpp_metric`
    key_to_remove = None
    for key, entry in current_schedule_data.items():
        if str(entry.get("time")) == time_to_remove:
            key_to_remove = key
            break

    if not key_to_remove:
        return jsonify({"error": f"Schedule entry with time {time_to_remove} not found"}), 404

    # Remove the specific schedule entry
    del current_schedule_data[key_to_remove]

    # Reorder remaining schedules to maintain "time1", "time2", "time3" format
    updated_schedule_data = {
        f"time{index + 1}": entry for index, entry in enumerate(current_schedule_data.values())
    }

    existing_schedule.schedule_data = updated_schedule_data
    flag_modified(existing_schedule, "schedule_data")

    try:
        db.session.commit()
        return jsonify({"message": f"Schedule entry for time {time_to_remove} removed successfully"}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Database error: {str(e)}"}), 500

def delete_schedule_logic(data):
    user_id = data.get("id")  # User ID from request
    ad_account_id = data.get("ad_account_id")  # Ad Account ID from request

    if not user_id or not ad_account_id:
        return {"error": "Missing required parameters: 'id' and 'ad_account_id'"}, 400

    # Verify if the user exists
    user = User.query.filter_by(id=user_id).first()
    if not user:
        return {"error": f"User with id {user_id} not found"}, 404

    # Check if a schedule exists for this user and ad_account_id
    existing_schedule = CampaignOffOnly.query.filter_by(ad_account_id=ad_account_id, user_id=user_id).first()
    if not existing_schedule:
        return {"error": f"No schedule found for ad_account_id {ad_account_id} belonging to user {user_id}"}, 404

    try:
        # Delete the schedule from the database
        db.session.delete(existing_schedule)
        db.session.commit()

        # Construct Redis key and delete it
        redis_key = f"{user_id}-{ad_account_id}-key"
        redis_websocket.delete(redis_key)  # Delete the key from Redis

        return {
            "message": f"Schedule for ad_account_id {ad_account_id} linked to user {user_id} has been deleted, along with Redis key {redis_key}"
        }, 200

    except Exception as e:
        db.session.rollback()
        return {"error": f"Database error: {str(e)}"}, 500
    
def edit_schedule_logic(data):
    user_id = data.get("id")  # User ID from request
    ad_account_id = data.get("ad_account_id")  # Ad Account ID from request
    time_to_edit = data.get("time")  # Existing schedule time to modify
    new_campaign_name = data.get("new_campaign_name")  # New campaign name (optional)
    new_time = data.get("new_time")  # New time (optional)
    new_on_off = data.get("new_on_off")  # ON/OFF status (optional)
    new_status = data.get("new_status")  # Running/Paused status (optional)

    if not user_id or not ad_account_id or not time_to_edit:
        return {"error": "Missing required parameters: 'id', 'ad_account_id', and 'time'"}, 400

    # Verify if the user exists
    user = User.query.filter_by(id=user_id).first()
    if not user:
        return {"error": f"User with id {user_id} not found"}, 404

    # Fetch existing schedule
    existing_schedule = CampaignOffOnly.query.filter_by(ad_account_id=ad_account_id).first()
    if not existing_schedule or not existing_schedule.schedule_data:
        return {"error": f"No schedule found for ad_account_id {ad_account_id}"}, 404

    current_schedule_data = existing_schedule.schedule_data

    # Find the schedule entry by `time`
    key_to_edit = None
    for key, entry in current_schedule_data.items():
        if str(entry.get("time")) == str(time_to_edit):
            key_to_edit = key
            break

    if not key_to_edit:
        return {"error": f"No schedule entry found with time {time_to_edit}"}, 404

    # Apply modifications
    if new_campaign_name:
        current_schedule_data[key_to_edit]["campaign_name"] = (
            new_campaign_name if isinstance(new_campaign_name, list) else [new_campaign_name]
        )
    if new_time:
        current_schedule_data[key_to_edit]["time"] = new_time
    if new_on_off:
        if new_on_off not in ["ON", "OFF"]:
            return {"error": "Invalid 'new_on_off' value. Use 'ON' or 'OFF'."}, 400
        current_schedule_data[key_to_edit]["on_off"] = new_on_off

    # Apply new_status update if provided
    if new_status:
        if new_status not in ["Running", "Paused"]:
            return {"error": "Invalid 'new_status' value. Use 'Running' or 'Paused'."}, 400
        current_schedule_data[key_to_edit]["status"] = new_status

    # Mark schedule_data as modified for SQLAlchemy
    flag_modified(existing_schedule, "schedule_data")

    try:
        db.session.commit()
        return {
            "message": f"Schedule entry for time {time_to_edit} updated successfully.",
            "updated_schedule": existing_schedule.schedule_data
        }, 200
    except Exception as e:
        db.session.rollback()
        return {"error": f"Database error: {str(e)}"}, 500