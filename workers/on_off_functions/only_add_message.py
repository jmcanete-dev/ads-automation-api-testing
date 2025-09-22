import json
import redis
import logging
from datetime import datetime, timedelta

# Set up Redis client
redis_websocket = redis.Redis(
    host="redisAds",
    port=6379,
    db=11,
    decode_responses=True  # Ensures Redis returns strings
)

def append_redis_message2(user_id, ad_account_id, new_message):
    """Append a new message inside a dictionary structure while keeping old messages.
    Ensure Redis key expires at 12 AM the next day.
    """
    redis_key = f"{user_id}-{ad_account_id}-key"

    try:
        # Check if Redis is accessible
        if not redis_websocket.ping():
            logging.error("Redis is not responding.")
            return

        # Retrieve existing messages from Redis
        existing_data = redis_websocket.get(redis_key)

        if existing_data:
            try:
                data_dict = json.loads(existing_data)

                # Ensure proper structure (message should be a list)
                if not isinstance(data_dict, dict) or not isinstance(data_dict.get("message"), list):
                    logging.warning(f"Invalid format at {redis_key}, resetting.")
                    data_dict = {"message": []}  # Reset format
            except json.JSONDecodeError:
                logging.warning(f"Corrupt JSON at {redis_key}, resetting.")
                data_dict = {"message": []}
        else:
            data_dict = {"message": []}

        # Append new message (convert everything to string)
        data_dict["message"].append(str(new_message))

        # Convert entire dictionary to string format and store in Redis
        redis_websocket.set(redis_key, json.dumps(data_dict, ensure_ascii=False))

        # **Set Expiry Time to 12 AM Tomorrow**
        now = datetime.now()
        midnight_tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        expiry_timestamp = int(midnight_tomorrow.timestamp())  # Convert to Unix timestamp

        redis_websocket.expireat(redis_key, expiry_timestamp)  # Set exact expiration

        # Debugging: Verify Redis storage
        stored_data = redis_websocket.get(redis_key)
        if stored_data is None:
            logging.error(f"Failed to write {redis_key} to Redis.")
        else:
            logging.info(f"Redis key {redis_key} successfully updated: {stored_data} (expires at {midnight_tomorrow})")

    except Exception as e:
        logging.error(f"Error updating Redis key {redis_key}: {str(e)}")
