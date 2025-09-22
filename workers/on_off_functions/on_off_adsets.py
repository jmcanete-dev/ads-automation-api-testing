import json
import redis
import logging
from datetime import datetime, timedelta

# Set up Redis client
redis_websocket_as = redis.Redis(
    host="redisAds",
    port=6379,
    db=15,
    decode_responses=True  # Ensures Redis returns strings
)

def append_redis_message_adsets(user_id, new_message):
    """Append a new message inside a dictionary structure while keeping old messages.
    Ensure Redis key expires at 12 AM the next day.
    """
    redis_key = f"{user_id}-key"

    try:
        # Check if Redis is accessible
        if not redis_websocket_as.ping():
            logging.error("Redis is not responding.")
            return

        # Set new message (overwriting any existing message)
        data_dict = {"message": [str(new_message)]}

        # Store in Redis
        redis_websocket_as.set(redis_key, json.dumps(data_dict, ensure_ascii=False))

        # **Set Expiry Time to 12 AM Tomorrow**
        now = datetime.now()
        midnight_tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        expiry_timestamp = int(midnight_tomorrow.timestamp())  # Convert to Unix timestamp

        redis_websocket_as.expireat(redis_key, expiry_timestamp)  # Set exact expiration

        # Debugging: Verify Redis storage
        stored_data = redis_websocket_as.get(redis_key)
        if stored_data is None:
            logging.error(f"Failed to write {redis_key} to Redis.")
        else:
            logging.info(f"Redis key {redis_key} successfully updated: {stored_data} (expires at {midnight_tomorrow})")

    except Exception as e:
        logging.error(f"Error updating Redis key {redis_key}: {str(e)}")
