from datetime import datetime, timedelta
import json
import logging
import time
from flask import Blueprint, request, jsonify
import pytz
import redis
from sqlalchemy import or_
from controllers.create_ads_controller import create_campaign
from workers.create_campaig_celery import create_simple_campaign_task
from models.models import PHRegionTable, User, db, Campaign
from sqlalchemy.exc import SQLAlchemyError
from controllers.insert_campaign_controller import upsert_campaign_data

from workers.on_off_functions.create_campaign_message import append_redis_message_create_campaigns

createbp = Blueprint('createbp', __name__)
manila_tz = pytz.timezone("Asia/Manila")
current_time_manila = datetime.now(manila_tz)

# @createbp.route('/create-campaigns-ai', methods=['POST'])
# def create_full_campaign():
#     try:
#         data = request.json
#         campaigns_data = data.get('campaigns')
#         user_id = data.get('user_id')

#         # Validate user_id before proceeding
#         if not user_id:
#             raise ValueError("No user_id provided.")
#         user = db.session.query(User).filter_by(id=user_id).first()
#         if not user:
#             raise ValueError(f"Invalid user_id: {user_id}. User not found.")

#         results = []

#         for campaign_data in campaigns_data:
#             try:
#                 # Extract campaign-specific data
#                 ad_account_id = campaign_data.get('ad_account_id')
#                 access_token = campaign_data.get('access_token')
#                 page_name = campaign_data.get('page_name')
#                 facebook_page_id = campaign_data.get('facebook_page_id')
#                 sku = campaign_data.get('sku')
#                 material_code = campaign_data.get('material_code')
#                 daily_budget = campaign_data.get('daily_budget')
#                 title = campaign_data.get('headline')
#                 message = campaign_data.get('primary_text')
#                 product = campaign_data.get('product')
#                 video_url = campaign_data.get('video_url')
#                 image_url = campaign_data.get('image_url')
#                 campaign_name = f"{page_name}-{sku}-{material_code}"
#                 adjusted_daily_budget = daily_budget * 10

#                 exclude_ph_region = campaign_data.get('exclude_ph_region', [])
#                 # Attempt to create a campaign on Facebook
#                 campaign_response = create_campaign(ad_account_id, access_token, campaign_name, adjusted_daily_budget)

#                 # Extract scheduled_date and scheduled_time, then merge
#                 start_date = campaign_data.get('start_date')  # YYYY-MM-DD
#                 start_time = campaign_data.get('start_time')  # HH:MM:SS

#                 interests_list = campaign_data.get('interests_list', [])  

#                 if not start_date or not start_time:
#                     return jsonify({"error": "Both scheduled_date and scheduled_time are required."}), 400

#                 try:
#                 # Combine date and time into a single datetime object
#                     start_datetime = datetime.strptime(f"{start_date} {start_time}", "%Y-%m-%d %H:%M:%S")
#                 # Convert to Manila timezone
#                     start_datetime = manila_tz.localize(start_datetime)
#                 except ValueError:
#                     return jsonify({"error": "Invalid date or time format. Use YYYY-MM-DD for date and HH:MM:SS for time."}), 400

#                 # Convert to required format: YYYY-MM-DDT00:00:00+0800
#                 start_time = start_datetime.strftime('%Y-%m-%dT%H:%M:%S%z')

#                 # Handle excluded regions logic
#                 global_exclude_regions = exclude_ph_region[0] if len(exclude_ph_region) == 1 and isinstance(exclude_ph_region[0], list) else None
#                 adset_excluded_regions = []

#                 for idx, interest_group in enumerate(interests_list):
#                     region_exclusion = global_exclude_regions if global_exclude_regions else (exclude_ph_region[idx] if idx < len(exclude_ph_region) else [])

#                     excluded_geo_locations = {}
#                     if region_exclusion:
#                         excluded_regions = db.session.query(PHRegionTable).filter(
#                             or_(*[PHRegionTable.region_name.ilike(f"%{region}%") for region in region_exclusion])
#                         ).all()

#                         excluded_geo_locations = {
#                             "regions": [
#                                 {"key": str(region.region_key), "name": region.region_name, "country": "PH"}
#                                 for region in excluded_regions
#                             ]
#                         }

#                     adset_excluded_regions.append(excluded_geo_locations or {"regions": []})

#                 if 'id' not in campaign_response:
#                     error_details = campaign_response.get('error', {})
#                     results.append({
#                         "campaign_name": campaign_name,
#                         "status": "failed",
#                         "error": "Failed to create campaign",
#                         "details": error_details
#                     })
#                     logging.error(f"Failed to create campaign: {campaign_name}, Error: {error_details}")
#                     continue

#                 campaign_id = campaign_response['id']
#                 logging.info(f"Successfully created campaign: {campaign_name} id: {campaign_id}")
#                 append_redis_message_create_campaigns(user_id, f"Successfully created campaign: {campaign_name} id: {campaign_id}")
#                 # Add campaign to the database using SQLAlchemy
#                 campaign_entry = Campaign(
#                     campaign_id=campaign_id,
#                     user_id=user_id,
#                     ad_account_id=ad_account_id,
#                     page_name=page_name,
#                     sku=sku,
#                     material_code=material_code,
#                     daily_budget=daily_budget,
#                     facebook_page_id=facebook_page_id,
#                     video_url=video_url,
#                     headline=title,
#                     primary_text=message,
#                     image_url=image_url,
#                     product=product,
#                     exclude_ph_regions = exclude_ph_region,
#                     is_ai=True,
#                     access_token=access_token,
#                     status='Generating',
#                     created_at=current_time_manila
#                 )
#                 db.session.add(campaign_entry)
#                 db.session.commit()

#                 time.sleep(5)
                
#                 # Save the initial server message
#                 initial_message = f"[{current_time_manila.strftime('%Y-%m-%d %H:%M:%S')}] Campaign created {campaign_name}."
#                 upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=initial_message)

#                 # Celery Task for processing
#                 task = create_full_campaign_task.apply_async(
#                     args=[ad_account_id, user_id, access_token, campaign_id, campaign_name, page_name, facebook_page_id,
#                           sku, material_code, daily_budget, title, message, product, video_url, image_url, interests_list, start_time, adset_excluded_regions]
#                 )

#                 # Log and update the Celery task creation status
#                 task_message = f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Campaign processing task created: {task.id}"
#                 upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=task_message)

#                 results.append({"campaign_name": campaign_name, "task_id": task.id, "campaign_id": campaign_id})

#             except Exception as e:
#                 error_message = f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Error during campaign processing: {str(e)} Campaign: {campaign_name}"
#                 logging.error(error_message)
#                 status = "Failed"
#                 upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=initial_message, status=status)
#                 results.append({
#                     "campaign_name": campaign_data.get('page_name', 'Unknown'),
#                     "status": "failed",
#                     "error": str(e)
#                 })

#         return jsonify({"results": results}), 202

#     except Exception as e:
#         logging.error(f"Critical error during campaign creation: {str(e)}")
#         return jsonify({"error": "An error occurred", "details": str(e)}), 500
        
redis_on_off_websocket = redis.Redis(
    host="redisAds",
    port=6379,
    db=14,  
    decode_responses=True
)

from workers.on_off_functions.create_campaign_message import append_redis_message_create_campaigns

@createbp.route('/create-campaigns', methods=['POST'])
def create_multiple_simple_campaigns():
    try:

        append_redis_message_create_campaigns(request.json.get('user_id'), "[START] Processing campaign creation request.")

        campaigns = request.json.get('campaigns')
        if not campaigns or not isinstance(campaigns, list):
            append_redis_message_create_campaigns(request.json.get('user_id'), "[ERROR] Invalid input. Expected a list of campaigns.")
            return jsonify({"error": "Invalid input. Expected a list of campaigns."}), 400

        user_id = request.json.get('user_id')
        if not user_id:
            append_redis_message_create_campaigns("Unknown", "[ERROR] No user_id provided.")
            raise ValueError("No user_id provided.")
        
        user = db.session.query(User).filter_by(id=user_id).first()
        if not user:
            append_redis_message_create_campaigns(user_id, f"[ERROR] Invalid user_id: {user_id}. User not found.")
            raise ValueError(f"Invalid user_id: {user_id}. User not found.")
        
        # Create WebSocket Redis key if it doesn’t exist
        websocket_key = f"{user_id}-key"
        if not redis_on_off_websocket.exists(websocket_key):
            redis_on_off_websocket.set(websocket_key, json.dumps({"message": ["User-Id Created"]}))
            append_redis_message_create_campaigns(user_id, "[INFO] WebSocket key created.")

        tasks = []

        for campaign_data in campaigns:
            try:
                append_redis_message_create_campaigns(user_id, "[START] Processing individual campaign.")

                # Extract campaign data from the request
                ad_account_id = campaign_data.get('ad_account_id')
                access_token = campaign_data.get('access_token')
                facebook_page_id = campaign_data.get('facebook_page_id')
                page_name = campaign_data.get('page_name')
                sku = campaign_data.get('sku')
                material_code = campaign_data.get('material_code')
                campaign_code = campaign_data.get('campaign_code')
                daily_budget = campaign_data.get('daily_budget')
                headline = campaign_data.get('headline')
                primary_text = campaign_data.get('primary_text')
                product = campaign_data.get('product')
                video_url = campaign_data.get('video_url')
                image_url = campaign_data.get('image_url')
                interests_list = campaign_data.get('interests_list', [])
                exclude_ph_region = campaign_data.get('exclude_ph_region', [])

                # Validate start date and time
                start_date = campaign_data.get('start_date')
                start_time = campaign_data.get('start_time')

                if not start_date or not start_time:
                    append_redis_message_create_campaigns(user_id, "[ERROR] Missing start_date or start_time.")
                    return jsonify({"error": "Both start_date and start_time are required."}), 400

                try:
                    start_datetime = datetime.strptime(f"{start_date} {start_time}", "%Y-%m-%d %H:%M:%S")
                    start_datetime = manila_tz.localize(start_datetime)
                except ValueError:
                    append_redis_message_create_campaigns(user_id, "[ERROR] Invalid date or time format.")
                    return jsonify({"error": "Invalid date or time format."}), 400

                start_time = start_datetime.strftime('%Y-%m-%dT%H:%M:%S%z')

                # Handle excluded regions logic
                global_exclude_regions = exclude_ph_region[0] if len(exclude_ph_region) == 1 and isinstance(exclude_ph_region[0], list) else None
                adset_excluded_regions = []

                for idx, interest_group in enumerate(interests_list):
                    region_exclusion = global_exclude_regions if global_exclude_regions else (exclude_ph_region[idx] if idx < len(exclude_ph_region) else [])

                    excluded_geo_locations = {}
                    if region_exclusion:
                        excluded_regions = db.session.query(PHRegionTable).filter(
                            or_(*[PHRegionTable.region_name.ilike(f"%{region}%") for region in region_exclusion])
                        ).all()

                        excluded_geo_locations = {
                            "regions": [
                                {"key": str(region.region_key), "name": region.region_name, "country": "PH"}
                                for region in excluded_regions
                            ]
                        }

                    # Ensure empty list if no regions are found or excluded
                    adset_excluded_regions.append(excluded_geo_locations or {"regions": []})

                # Create Facebook campaign
                campaign_name = f"{page_name}-{sku}-{material_code}-{campaign_code}"
                adjusted_daily_budget = daily_budget * 10

                append_redis_message_create_campaigns(user_id, f"[INFO] Creating Facebook campaign: {campaign_name}.")

                campaign_response = create_campaign(ad_account_id, access_token, campaign_name, adjusted_daily_budget)

                if 'id' not in campaign_response:
                    error_details = campaign_response.get('error', {})
                    logging.error(f"Failed to create campaign: {campaign_name}, Error: {error_details}")
                    append_redis_message_create_campaigns(user_id, f"[ERROR] Failed to create campaign: {campaign_name}. {error_details}")
                    upsert_campaign_data(user_id, ad_account_id, None, last_server_messages="Failed to create campaign", status="Failed")
                    tasks.append({"campaign_name": campaign_name, "status": "failed", "error": "Failed to create campaign"})
                    continue

                campaign_id = campaign_response['id']
                logging.info(f"Successfully created campaign: {campaign_name} id: {campaign_id}")
                append_redis_message_create_campaigns(user_id, f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Successfully created campaign: {campaign_name} id: {campaign_id}")
                upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=f"Campaign created: {campaign_name}")

                # Save campaign details to DB
                campaign_entry = Campaign(
                    campaign_id=campaign_id,
                    user_id=user_id,
                    ad_account_id=ad_account_id,
                    page_name=page_name,
                    sku=sku,
                    material_code=material_code,
                    campaign_code=campaign_code,
                    daily_budget=daily_budget,
                    facebook_page_id=facebook_page_id,
                    video_url=video_url,
                    headline=headline,
                    primary_text=primary_text,
                    image_url=image_url,
                    product=product,
                    interests_list=interests_list,
                    exclude_ph_regions = exclude_ph_region,
                    is_ai=False,
                    access_token=access_token,
                    status='Generating',
                    created_at=datetime.now(manila_tz)
                )

                db.session.add(campaign_entry)
                db.session.commit()
                append_redis_message_create_campaigns(user_id, f"[INFO] Campaign {campaign_name} saved to database.")

                # Async Task
                task = create_simple_campaign_task.apply_async(
                    args=[ad_account_id, user_id, access_token, campaign_id, campaign_name, page_name, facebook_page_id,
                          sku, material_code, campaign_code, daily_budget, headline, primary_text, product, video_url, image_url, interests_list, start_time, adset_excluded_regions]
                )

                upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=f"Task created: {task.id}")
                append_redis_message_create_campaigns(user_id, f"[INFO] Task {task.id} started for campaign {campaign_name}.")
                tasks.append({"task_id": task.id, "campaign_name": campaign_name, "status": "generating"})

            except Exception as e:
                logging.error(f"Error during campaign processing: {str(e)}")
                append_redis_message_create_campaigns(user_id, f"[ERROR] Campaign processing failed: {str(e)}")
                upsert_campaign_data(user_id, ad_account_id, None, last_server_messages="Error during processing", status="Failed")
                tasks.append({"campaign_name": campaign_data.get('page_name', 'Unknown'), "status": "failed", "error": str(e)})

        return jsonify({"tasks": tasks,}), 202

    except Exception as e:
        logging.error(f"Critical error during campaign creation: {str(e)}")
        append_redis_message_create_campaigns("Unknown", f"[ERROR] Critical error: {str(e)}")
        return jsonify({"error": "An error occurred", "details": str(e)}), 500

@createbp.route('/get-campaigns', methods=['GET'])
def get_user_campaigns():
    try:
        user_id = request.args.get('user_id')
        if not user_id:
            return jsonify({"error": "User ID is required"}), 400

        user = db.session.query(User).filter_by(id=user_id).first()
        if not user:
            return jsonify({"error": f"User with ID {user_id} not found"}), 404

        campaigns = db.session.query(Campaign).filter_by(user_id=user_id).all()
        campaign_list = [
            {
                "campaign_id": campaign.campaign_id,
                "ad_account_id": campaign.ad_account_id,
                "page_name": campaign.page_name,
                "sku": campaign.sku,
                "material_code": campaign.material_code,
                "campaign_code": campaign.campaign_code,
                "daily_budget": campaign.daily_budget,
                "facebook_page_id": campaign.facebook_page_id,
                "video_url": campaign.video_url,
                "headline": campaign.headline,
                "primary_text": campaign.primary_text,
                "image_url": campaign.image_url,
                "product": campaign.product,
                "interests_list": campaign.interests_list,
                "exclude_ph_regions": campaign.exclude_ph_regions,
                "status": campaign.status,
                "created_at": campaign.created_at.strftime('%Y-%m-%d %H:%M:%S')
            }
            for campaign in campaigns
        ]

        return jsonify({"campaigns": campaign_list}), 200

    except Exception as e:
        logging.error(f"Error fetching campaigns: {str(e)}")
        return jsonify({"error": "An error occurred", "details": str(e)}), 500
