import logging
import time
import json
from flask import Blueprint, Response, request
import redis

# Configure logging
logging.basicConfig(level=logging.DEBUG)

# Blueprint for SSE events
message_events_blueprint = Blueprint("message_events", __name__)

# Redis connections
redis_websocket = redis.Redis(host="redisAds", port=6379, db=10, decode_responses=True)
redis_websocket_only = redis.Redis(host="redisAds", port=6379, db=11, decode_responses=True)
redis_websocket_off = redis.Redis(host="redisAds", port=6379, db=13, decode_responses=True)  # Redis DB 13
# Redis connection for Create Campaigns
redis_websocket_cc = redis.Redis(host="redisAds", port=6379, db=14, decode_responses=True)
# Redis connection for AdSets
redis_websocket_as = redis.Redis(host="redisAds",port=6379,db=15,decode_responses=True)
# Redis connection for Page name on/off
redis_websocket_pn = redis.Redis(host="redisAds", port=6379, db=12, decode_responses=True)
# Redis connection for Ad Spent
redis_websocket_asr = redis.Redis(host="redisAds", port=6379, db=9, decode_responses=True)
# Redis connection for budget
redis_websocket_budget = redis.Redis(host="redisAds", port=6379, db=8, decode_responses=True)
# Redis connection for location
redis_websocket_location = redis.Redis(host="redisAds", port=6379, db=7, decode_responses=True)

# Ensure Redis keyspace notifications are enabled
redis_websocket.config_set("notify-keyspace-events", "KEA")
redis_websocket_only.config_set("notify-keyspace-events", "KEA")
redis_websocket_off.config_set("notify-keyspace-events", "KEA")
redis_websocket_cc.config_set("notify-keyspace-events", "KEA")
redis_websocket_as.config_set("notify-keyspace-events", "KEA")
redis_websocket_pn.config_set("notify-keyspace-events", "KEA")
redis_websocket_asr.config_set("notify-keyspace-events", "KEA")

def send_initial_data(redis_instance, specific_key):
    """Fetch and send the latest Redis key data when client connects."""
    existing_data = redis_instance.get(specific_key)
    
    if existing_data:
        try:
            parsed_data = json.loads(existing_data)
            logging.info(f"Sending initial data for key {specific_key}: {parsed_data}")
            yield f"data: {json.dumps({'key': specific_key, 'data': f" Last Message: {parsed_data}"})}\n\n"
        except json.JSONDecodeError:
            logging.error(f"Invalid JSON format in Redis key: {specific_key}")
            yield f"data: {json.dumps({'key': specific_key, 'error': 'Invalid JSON format'})}\n\n"
    else:
        logging.warning(f"Key {specific_key} does not exist at connection time.")
        yield f"data: {json.dumps({'key': specific_key, 'error': 'Key does not exist'})}\n\n"


def listen_for_changes(redis_instance, specific_key, db_number):
    """Continuously listens for changes and polls Redis for rapid updates."""
    pubsub = redis_instance.pubsub()
    channel_pattern = f"__keyspace@{db_number}__:{specific_key}"
    pubsub.subscribe(channel_pattern)
    
    last_sent_data = None  
    last_sent_time = 0  
    logging.info(f"Listening for updates on Redis DB {db_number}, key: {specific_key}")

    try:
        while True:
            new_data = redis_instance.get(specific_key)
            if new_data and new_data != last_sent_data:
                try:
                    parsed_data = json.loads(new_data)
                    current_time = time.time()

                    if current_time - last_sent_time >= 0.05:
                        logging.info(f"Sending update for key {specific_key}: {parsed_data}")
                        yield f"data: {json.dumps({'key': specific_key, 'data': parsed_data})}\n\n"
                        last_sent_data = new_data
                        last_sent_time = current_time
                except json.JSONDecodeError:
                    logging.error(f"Invalid JSON format in Redis key: {specific_key}")

            message = pubsub.get_message(timeout=0.1)
            if message and message.get("type") == "message":
                event_type = message.get("data")
                if event_type == "del":
                    logging.warning(f"Key {specific_key} deleted, notifying clients.")
                    yield f"data: {json.dumps({'key': specific_key, 'error': 'Key no longer exists'})}\n\n"
                    break  

            time.sleep(0.1)  

    except Exception as e:
        logging.error(f"Error in Redis listener for {specific_key}: {e}")
    finally:
        pubsub.close()


def send_sse_signal(redis_instance, specific_key, db_number):
    """Generates the SSE stream for a Redis key, sends initial data, and listens for changes."""
    yield from send_initial_data(redis_instance, specific_key)  
    yield from listen_for_changes(redis_instance, specific_key, db_number)


@message_events_blueprint.route("/messageevents")
def message_events():
    """SSE endpoint that streams Redis key updates from DB 10."""
    room = request.args.get("keys")

    if not room:
        return "Missing 'keys' query parameter", 400
    
    logging.info(f"Client connected to SSE for key: {room} on DB 10")
    
    return Response(send_sse_signal(redis_websocket, room, 10), content_type="text/event-stream")


@message_events_blueprint.route("/messageevents-only")
def message_events_only():
    """SSE endpoint that streams Redis key updates from DB 11."""
    room = request.args.get("keys")

    if not room:
        return "Missing 'keys' query parameter", 400
    
    logging.info(f"Client connected to SSE for key: {room} on DB 11")
    
    return Response(send_sse_signal(redis_websocket_only, room, 11), content_type="text/event-stream")


@message_events_blueprint.route("/messageevents-off")
def message_events_off():
    """SSE endpoint that streams Redis key updates from DB 13."""
    room = request.args.get("keys")

    if not room:
        return "Missing 'keys' query parameter", 400
    
    logging.info(f"Client connected to SSE for key: {room} on DB 13")
    
    return Response(send_sse_signal(redis_websocket_off, room, 13), content_type="text/event-stream")

@message_events_blueprint.route("/messageevents-campaign-creations")
def messageevents_campaign_creations():
    """SSE endpoint that streams Redis key updates from DB 14."""
    room = request.args.get("keys")

    if not room:
        return "Missing 'keys' query parameter", 400
    
    logging.info(f"Client connected to SSE for key: {room} on DB 14")
    
    return Response(send_sse_signal(redis_websocket_cc, room, 14), content_type="text/event-stream")

@message_events_blueprint.route("/messageevents-adsets")
def messageevents_adsets():
    """SSE endpoint that streams Redis key updates from DB 15."""
    room = request.args.get("keys")

    if not room:
        return "Missing 'keys' query parameter", 400
    
    logging.info(f"Client connected to SSE for key: {room} on DB 15")
    
    return Response(send_sse_signal(redis_websocket_as, room, 15), content_type="text/event-stream")

@message_events_blueprint.route("/messageevents-pagename")
def messageevents_pagename():
    """SSE endpoint that streams Redis key updates from DB 12."""
    room = request.args.get("keys")

    if not room:
        return "Missing 'keys' query parameter", 400
    
    logging.info(f"Client connected to SSE for key: {room} on DB 12")
    
    return Response(send_sse_signal(redis_websocket_pn, room, 12), content_type="text/event-stream")

@message_events_blueprint.route("/messageevents-adspentreport")
def messageevents_adspentreport():
    """SSE endpoint that streams Redis key updates from DB 9."""
    room = request.args.get("keys")

    if not room:
        return "Missing 'keys' query parameter", 400
    
    logging.info(f"Client connected to SSE for key: {room} on DB 9")
    
    return Response(send_sse_signal(redis_websocket_asr, room, 9), content_type="text/event-stream")

@message_events_blueprint.route("/messageevents-editbudget")
def messageevents_editbudget():
    """SSE endpoint that streams Redis key updates from DB 8."""
    room = request.args.get("keys")

    if not room:
        return "Missing 'keys' query parameter", 400
    
    logging.info(f"Client connected to SSE for key: {room} on DB 8")
    
    return Response(send_sse_signal(redis_websocket_budget, room, 8), content_type="text/event-stream")

@message_events_blueprint.route("/messageevents-editlocation")
def messageevents_editlocation():
    """SSE endpoint that streams Redis key updates from DB 7."""
    room = request.args.get("keys")

    if not room:
        return "Missing 'keys' query parameter", 400
    
    logging.info(f"Client connected to SSE for key: {room} on DB 7")
    
    return Response(send_sse_signal(redis_websocket_location, room, 7), content_type="text/event-stream")