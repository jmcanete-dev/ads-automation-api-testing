from flask import json
import redis
from models.models import User, db, CampaignsScheduled
from datetime import datetime
import pytz
from sqlalchemy.orm.attributes import flag_modified

manila_tz = pytz.timezone("Asia/Manila")

redis_websocket = redis.Redis(
    host="redisAds",
    port=6379,
    db=10,
    decode_responses=True
)

def check_duplicate_times(ad_account_id, schedule_data):
    existing_schedule = CampaignsScheduled.query.filter_by(ad_account_id=ad_account_id).first()
    if existing_schedule:
        existing_times = existing_schedule.schedule_data
        duplicate_times = set(existing_times.values()) & set(schedule_data)
        if duplicate_times:
            return True, list(duplicate_times)
    return False, []

def check_ad_account_assigned(ad_account_id, user_id):
    existing_schedule = CampaignsScheduled.query.filter_by(ad_account_id=ad_account_id).first()
    if existing_schedule and existing_schedule.user_id != user_id:
        return True, existing_schedule.user_id
    return False, None

def add_schedule_logic(data):
    ad_account_id = data.get("ad_account_id")
    user_id = data.get("user_id")
    access_token = data.get("access_token")
    schedule_data = data.get("schedule_data")

    if not (ad_account_id and user_id and access_token and schedule_data):
        return {"error": "Missing required fields"}, 400

    unique_time_combos = set()
    campaign_code = None  # Initialize campaign_code variable
    
    for schedule in schedule_data:
        time_value = schedule["time"]
        combo_key = (time_value, schedule["campaign_code"], schedule["watch"])

        if combo_key in unique_time_combos:
            return {"error": f"Duplicate time with same campaign_code and watch in request: {time_value}"}, 400
        unique_time_combos.add(combo_key)

        try:
            datetime.strptime(time_value, "%H:%M")
        except ValueError:
            return {"error": f"Invalid time format: {time_value}. Use HH:MM"}, 400

        if not schedule.get("campaign_code"):
            return {"error": f"Missing campaign_code for {time_value}. It is required."}, 400

        if schedule["watch"] not in ["Campaigns", "AdSets"]:
            return {"error": f"Invalid watch for {time_value}. Use 'Campaigns' or 'AdSets'"}, 400

        if schedule["on_off"] not in ["ON", "OFF"]:
            return {"error": f"Invalid on_off for {time_value}. Use 'ON' or 'OFF'"}, 400

        # Check if campaign_code exists in campaign_name and set on_off accordingly
        if not schedule.get("on_off"):  # Only adjust if 'on_off' is not set in the request
            campaign_code = schedule["campaign_code"]
            campaign_name = schedule.get("campaign_name", "")  # Assume the campaign_name is passed in the schedule
            if campaign_code in campaign_name:
                schedule["on_off"] = "ON"
            else:
                schedule["on_off"] = "OFF"

    is_assigned, existing_user_id = check_ad_account_assigned(ad_account_id, user_id)
    if is_assigned:
        return {"error": f"ad_account_id {ad_account_id} is already handled by user {existing_user_id}"}, 400

    user_exists = User.query.filter_by(id=user_id).first()
    if not user_exists:
        return {"error": f"User with user_id {user_id} does not exist"}, 400

    existing_schedule = CampaignsScheduled.query.filter_by(ad_account_id=ad_account_id).first()

    if existing_schedule:
        current_schedule_data = existing_schedule.schedule_data or {}
        existing_times_map = {
            (entry["time"], entry["campaign_code"], entry["watch"]): key
            for key, entry in current_schedule_data.items()
        }

        filtered_new_times = {}
        for schedule in schedule_data:
            time_combo = (schedule["time"], schedule["campaign_code"], schedule["watch"])

            if time_combo in existing_times_map:
                existing_key = existing_times_map[time_combo]
                existing_entry = current_schedule_data[existing_key]
                if (
                    existing_entry["cpp_metric"] != schedule.get("cpp_metric", "")
                    or existing_entry["on_off"] != schedule["on_off"]
                ):
                    return {
                        "error": f"Duplicate time found: {schedule['time']} with same campaign_code and watch. "
                        f"Only `cpp_metric` or `on_off` differ; update the existing slot instead."
                    }, 400
            else:
                new_key = f"time{len(current_schedule_data) + len(filtered_new_times) + 1}"
                filtered_new_times[new_key] = {
                    "time": schedule["time"],
                    "campaign_code": schedule["campaign_code"],
                    "watch": schedule["watch"],
                    "cpp_metric": schedule.get("cpp_metric", ""),
                    "on_off": schedule["on_off"],
                    "status": schedule.get("status", "Running"),
                }

        if not filtered_new_times:
            return {"error": "No new scheduled times to add"}, 400

        updated_schedule_data = {**current_schedule_data, **filtered_new_times}
        if len(updated_schedule_data) > 20:
            return {"error": f"User {user_id} cannot schedule more than 20 times"}, 400

        existing_schedule.schedule_data = updated_schedule_data
        existing_schedule.campaign_code = campaign_code  # Ensure campaign_code is updated
        message = f"Added new times: {', '.join([v['time'] for v in filtered_new_times.values()])}"
    else:
        if len(schedule_data) > 20:
            return {"error": f"User {user_id} cannot schedule more than 20 times"}, 400

        schedule_entries = {
            f"time{index + 1}": {
                "time": schedule["time"],
                "campaign_code": schedule["campaign_code"],
                "watch": schedule["watch"],
                "cpp_metric": schedule.get("cpp_metric", ""),
                "on_off": schedule["on_off"],
                "status": schedule.get("status", "Running"),
            }
            for index, schedule in enumerate(schedule_data)
        }

        existing_schedule = CampaignsScheduled(
            ad_account_id=ad_account_id,
            user_id=user_id,
            access_token=access_token,
            schedule_data=schedule_entries,
            campaign_code=campaign_code,  # Ensure the campaign_code is saved
            added_at=datetime.now(manila_tz),
            last_time_checked=datetime.now(manila_tz),
            last_check_status="Success",
            last_check_message="Scheduled",
        )
        db.session.add(existing_schedule)
        message = "New schedule added"

    try:
        db.session.commit()
        return {
            "message": f"Schedule updated successfully. {message}",
            "updated_schedule": existing_schedule.schedule_data,
        }, 201
    except Exception as e:
        db.session.rollback()
        return {"error": f"Database error: {str(e)}"}, 500

def append_schedule_logic(data):
    ad_account_id = data.get("ad_account_id")
    user_id = data.get("user_id") or data.get("id")
    access_token = data.get("access_token")
    new_schedule_data = data.get("schedule_data")

    if not (ad_account_id and user_id and access_token and new_schedule_data):
        return {"error": "Missing required fields"}, 400

    existing_schedule = CampaignsScheduled.query.filter_by(ad_account_id=ad_account_id).first()
    if not existing_schedule:
        return {"error": f"No schedule found for ad_account_id {ad_account_id}"}, 404

    current_schedule_data = existing_schedule.schedule_data or {}

    existing_times_map = {
        (entry["time"], entry["campaign_code"], entry["watch"]): key
        for key, entry in current_schedule_data.items()
    }

    filtered_new_times = {}
    for schedule in new_schedule_data:
        combo = (schedule["time"], schedule["campaign_code"], schedule["watch"])

        if combo in existing_times_map:
            return {
                "error": f"Duplicate time found: {schedule['time']} with campaign_code {schedule['campaign_code']} and watch {schedule['watch']}"
            }, 400

        # Check if campaign_code exists in campaign_name and set on_off accordingly
        campaign_code = schedule["campaign_code"]
        campaign_name = schedule.get("campaign_name", "")  # Assume the campaign_name is passed in the schedule
        if campaign_code in campaign_name:
            schedule["on_off"] = "ON"
        else:
            schedule["on_off"] = "OFF"

        if schedule["watch"] not in ["Campaigns", "AdSets"]:
            return {"error": f"Invalid watch for {schedule['time']}. Use 'Campaigns' or 'AdSets'"}, 400

        if schedule["on_off"] not in ["ON", "OFF"]:
            return {"error": f"Invalid on_off for {schedule['time']}. Use 'ON' or 'OFF'"}, 400

        new_key = f"time{len(current_schedule_data) + len(filtered_new_times) + 1}"
        filtered_new_times[new_key] = {
            "time": schedule["time"],
            "campaign_code": schedule["campaign_code"],
            "watch": schedule["watch"],
            "cpp_metric": schedule.get("cpp_metric", ""),
            "on_off": schedule["on_off"],
            "status": schedule.get("status", "Running"),
        }

    if not filtered_new_times:
        return {"error": "No new schedule data provided"}, 400

    if len(current_schedule_data) + len(filtered_new_times) > 20:
        return {"error": f"Cannot exceed 20 schedule slots for ad_account_id {ad_account_id}"}, 400

    updated_schedule_data = {**current_schedule_data, **filtered_new_times}
    existing_schedule.schedule_data = updated_schedule_data
    flag_modified(existing_schedule, "schedule_data")

    try:
        db.session.commit()
        return {
            "message": "Schedule successfully appended",
            "updated_schedule": updated_schedule_data,
        }, 200
    except Exception as e:
        db.session.rollback()
        return {"error": f"Database error: {str(e)}"}, 500

def edit_schedule_campaign_logic(data):
    user_id = data.get("id")  # User ID from request
    ad_account_id = data.get("ad_account_id")  # Ad Account ID from request
    time_to_edit = data.get("time")  # Existing schedule time to modify
    new_time = data.get("new_time")  # New time (optional)
    new_on_off = data.get("new_on_off")  # ON/OFF status (optional)
    new_cpp_metric = data.get("new_cpp_metric")  # CPP metric update (optional)
    new_watch = data.get("new_what_to_watch")  # What to watch (optional)
    new_status = data.get("new_status")  # Status update (optional)

    # Validate mandatory fields
    if not user_id or not ad_account_id or not time_to_edit:
        return {"error": "Missing required parameters: 'id', 'ad_account_id', and 'time'"}, 400

    # Verify if the user exists
    user = User.query.filter_by(id=user_id).first()
    if not user:
        return {"error": f"User with id {user_id} not found"}, 404

    # Fetch existing schedule for the user and ad account
    existing_schedule = CampaignsScheduled.query.filter_by(ad_account_id=ad_account_id, user_id=user_id).first()
    if not existing_schedule or not existing_schedule.schedule_data:
        return {"error": f"No schedule found for ad_account_id {ad_account_id} linked to user {user_id}"}, 404

    current_schedule_data = existing_schedule.schedule_data

    # Find the schedule entry by 'time' to edit
    key_to_edit = None
    for key, entry in current_schedule_data.items():
        if str(entry.get("time")) == str(time_to_edit):
            key_to_edit = key
            break

    if not key_to_edit:
        return {"error": f"No schedule entry found with time {time_to_edit}"}, 404

    # Apply modifications to the found entry
    if new_time:
        try:
            # Validate the time format is HH:MM
            datetime.strptime(new_time, "%H:%M")
            current_schedule_data[key_to_edit]["time"] = new_time
        except ValueError:
            return {"error": f"Invalid time format: {new_time}. Use HH:MM"}, 400

    if new_on_off:
        if new_on_off not in ["ON", "OFF"]:
            return {"error": "Invalid 'new_on_off' value. Use 'ON' or 'OFF'."}, 400
        current_schedule_data[key_to_edit]["on_off"] = new_on_off

    if new_cpp_metric:
        try:
            # Ensure the CPP Metric is a valid number (additional validation can be done as needed)
            new_cpp_metric = float(new_cpp_metric)
            current_schedule_data[key_to_edit]["cpp_metric"] = new_cpp_metric
        except ValueError:
            return {"error": "Invalid CPP Metric. It should be a numeric value."}, 400

    if new_watch:
        if new_watch not in ["Campaigns", "AdSets"]:
            return {"error": f"Invalid 'new_watch' value. Use 'Campaigns' or 'AdSets'."}, 400
        current_schedule_data[key_to_edit]["what_to_watch"] = new_watch

    if new_status:
        if new_status not in ["Running", "Paused"]:
            return {"error": "Invalid 'new_status' value. Use 'Running' or 'Paused'."}, 400
        current_schedule_data[key_to_edit]["status"] = new_status

    # Mark the schedule_data as modified for SQLAlchemy
    flag_modified(existing_schedule, "schedule_data")

    # Attempt to commit the changes to the database
    try:
        db.session.commit()
        return {
            "message": f"Schedule entry for time {time_to_edit} updated successfully.",
            "updated_schedule": existing_schedule.schedule_data
        }, 200
    except Exception as e:
        db.session.rollback()
        return {"error": f"Database error: {str(e)}"}, 500
    
def pause_schedule_campaign_logic(data):
    ad_account_id = data.get("ad_account_id")
    user_id = data.get("user_id")
    access_token = data.get("access_token")
    edited_schedule_data = data.get("schedule_data")

    if not (ad_account_id and user_id and access_token and edited_schedule_data):
        return {"error": "Missing required fields"}, 400

    # Retrieve the current schedule
    existing_schedule = CampaignsScheduled.query.filter_by(ad_account_id=ad_account_id).first()
    if not existing_schedule:
        return {"error": f"No schedule found for ad_account_id {ad_account_id}"}, 404

    current_schedule_data = existing_schedule.schedule_data or {}

    updated_schedule_data = {}

    for schedule in edited_schedule_data:
        # Check if the campaign_code exists in the campaign_name and adjust on_off
        campaign_code = schedule["campaign_code"]
        campaign_name = schedule.get("campaign_name", "")
        if campaign_code in campaign_name:
            schedule["on_off"] = "ON"
        else:
            schedule["on_off"] = "OFF"

        # Update the existing schedule entry or return error if not found
        time_combo = (schedule["time"], schedule["campaign_code"], schedule["watch"])
        matched_entry_key = None
        for key, entry in current_schedule_data.items():
            if (entry["time"], entry["campaign_code"], entry["watch"]) == time_combo:
                matched_entry_key = key
                break

        if matched_entry_key:
            updated_schedule_data[matched_entry_key] = schedule
        else:
            return {"error": f"No existing schedule found for time {schedule['time']} and campaign_code {schedule['campaign_code']}"}, 404

    existing_schedule.schedule_data = updated_schedule_data
    db.session.commit()
    return {
        "message": "Schedule updated successfully.",
        "updated_schedule": updated_schedule_data,
    }, 200

def remove_schedule_time_logic(data):
    user_id = data.get("id") 
    ad_account_id = data.get("ad_account_id")
    time_to_remove = data.get("time")
    campaign_code = data.get("campaign_code")  # Changed from 'campaign_type' to 'campaign_code'
    watch = data.get("watch")

    if not user_id or not ad_account_id:
        return {"error": "Missing required parameters: 'id' and 'ad_account_id'"}, 400

    user = User.query.filter_by(id=user_id).first()
    if not user:
        return {"error": f"User with id {user_id} not found"}, 404

    if not (time_to_remove and campaign_code and watch):
        return {"error": "Missing required fields"}, 400

    existing_schedule = CampaignsScheduled.query.filter_by(ad_account_id=ad_account_id).first()
    if not existing_schedule:
        return {"error": f"No schedule found for ad_account_id {ad_account_id}"}, 404

    current_schedule_data = existing_schedule.schedule_data or {}

    schedule_key = None
    for key, entry in current_schedule_data.items():
        if entry["time"] == time_to_remove and entry["campaign_code"] == campaign_code and entry["watch"] == watch:
            schedule_key = key
            break

    if not schedule_key:
        return {"error": f"Schedule entry with time {time_to_remove}, campaign_code {campaign_code}, and watch {watch} not found"}, 404

    del current_schedule_data[schedule_key]

    # Reorder keys to maintain "time1", "time2", "time3" pattern
    updated_schedule_data = {
        f"time{index + 1}": entry for index, entry in enumerate(current_schedule_data.values())
    }

    existing_schedule.schedule_data = updated_schedule_data

    try:
        db.session.commit()
        return {"message": f"Schedule entry {time_to_remove} removed successfully"}, 200
    except Exception as e:
        db.session.rollback()
        return {"error": f"Database error: {str(e)}"}, 500

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
    existing_schedule = CampaignsScheduled.query.filter_by(ad_account_id=ad_account_id, user_id=user_id).first()
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

def delete_all_schedules_logic(data):
    user_id = data.get("id")

    if not user_id:
        return {"error": "Missing required parameter: 'id'"}, 400

    # Verify if the user exists
    user = User.query.filter_by(id=user_id).first()
    if not user:
        return {"error": f"User with id {user_id} not found"}, 404

    # Find all schedules for this user
    user_schedules = CampaignsScheduled.query.filter_by(user_id=user_id).all()
    if not user_schedules:
        return {"error": f"No schedules found for user {user_id}"}, 404

    deleted_count = 0
    try:
        for schedule in user_schedules:
            # Construct Redis key and delete it
            redis_key = f"{user_id}-{schedule.ad_account_id}-key"
            redis_websocket.delete(redis_key)

            # Delete the schedule from the database
            db.session.delete(schedule)
            deleted_count += 1
        
        db.session.commit()

        return {
            "message": f"Successfully deleted {deleted_count} schedules for user {user_id}."
        }, 200

    except Exception as e:
        db.session.rollback()
        return {"error": f"Database error: {str(e)}"}, 500