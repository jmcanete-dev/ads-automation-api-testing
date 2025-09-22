import logging
import time
from flask import Blueprint, request, jsonify
import redis
import json
from workers.on_off_page_worker import fetch_campaign_off

# Initialize Redis connection
redis_websocket_pn = redis.Redis(
    host="redisAds",
    port=6379,
    db=12,  
    decode_responses=True
)

def add_pagename_off(data):
    
    if not isinstance(data, list):
        return jsonify({"error": "Expected an array of schedules."}), 400

    # Loop through each schedule and process it
    for schedule_entry in data:
        ad_account_id = schedule_entry.get("ad_account_id")
        user_id = schedule_entry.get("user_id")
        access_token = schedule_entry.get("access_token")
        schedule_data = schedule_entry.get("schedule_data")  # Each entry contains schedule_data
        logging.info(f"Processing ad_account_id: {ad_account_id} for user_id: {user_id}")

        if not (ad_account_id and user_id and access_token and schedule_data):
            return jsonify({"error": "Missing required fields in one of the schedule entries."}), 400

        # Create WebSocket Redis key if it doesn't exist
        websocket_key = f"{user_id}-key"
        if not redis_websocket_pn.exists(websocket_key):
            redis_websocket_pn.set(websocket_key, json.dumps({"message": ["User-Id Created"]}))
            logging.info(f"Created Redis key for user_id: {user_id} with initial message.")

        # Process each schedule in the schedule_data array
        for schedule in schedule_data:
            page_names = schedule.get("page_name")
            logging.info(f"Processing page_names: {page_names}")

            if not page_names or not isinstance(page_names, list) or len(page_names) == 0:
                return jsonify({"error": "Invalid or missing 'page_name'. It should be a non-empty list of strings."}), 400

            if schedule["on_off"] not in ["ON", "OFF"]:
                return jsonify({"error": f"Invalid on_off value for '{page_names}'. Use 'ON' or 'OFF'."}), 400

            # Log the number of pages being processed
            page_count = len(page_names)
            if page_count > 1:
                logging.info(f"Processing {page_count} page names: {', '.join(page_names)}")
                redis_websocket_pn.set(
                    websocket_key, 
                    json.dumps({"message": [f"Processing {page_count} page names for {ad_account_id}: {', '.join(page_names)}"]})
                )

            # Introduce a delay before calling Celery Task
            logging.info(f"Scheduling fetch_campaign_off for user_id: {user_id}, ad_account_id: {ad_account_id}, page_names: {page_names} with on_off value: {schedule['on_off']}")
            fetch_campaign_off.apply_async(args=[user_id, ad_account_id, access_token, schedule], countdown=0)
            logging.info(f"Scheduled task for page_names: {page_names} with delay.")

    logging.info(f"All schedules processed, responding with success message.")
    return jsonify({"message": "Schedules will be processed after a short delay."}), 201