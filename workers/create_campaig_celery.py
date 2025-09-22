from datetime import datetime
import logging
import time
import json
from flask import jsonify
import mysql.connector
import os
import pytz
import requests
from celery import shared_task
from controllers.add_video_images import add_ad_image, get_downloadable_drive_url
from controllers.create_ads_controller import create_ad, create_ad_creative, create_ad_usepost, create_adset
# from workers.ai_interest_worker import scrape_website
from celery.result import AsyncResult
import mysql.connector
import json
import time
from controllers.insert_campaign_controller import upsert_campaign_data

manila_tz = pytz.timezone("Asia/Manila")
current_time_manila = datetime.now(manila_tz)

# @shared_task(bind=True)
# def create_full_campaign_task(self, ad_account_id, user_id, access_token, campaign_id, campaign_name, page_name,
#                               facebook_page_id, sku, material_code, daily_budget, headline, primary_text,
#                               product, video_url, image_url, interests_list, start_time, adset_excluded_regions):
#     try:
#         creative_name = f"{campaign_name}-creative"
#         video_id = None
#         image_url_from_fb = None
#         json_adsets_ads_creatives = {"adsets": []}

#         headers = {
#             "Authorization": f"Bearer {access_token}",
#             "Content-Type": "application/json"
#         }

#         # Handle video upload
#         if video_url:
#             downloadable_video_url = get_downloadable_drive_url(video_url)
#             video_data = {"title": headline, "file_url": downloadable_video_url}
#             video_upload_url = f"https://graph.facebook.com/v21.0/act_{ad_account_id}/advideos"

#             video_response = requests.post(video_upload_url, headers=headers, json=video_data)
#             current_time_manila = datetime.now(manila_tz)

#             if video_response.status_code == 200:
#                 video_id = video_response.json().get('id')
#                 status = "Generating"
#                 initial_message = f"[{current_time_manila.strftime('%Y-%m-%d %H:%M:%S')}] Campaign: {campaign_name} Video Uploaded."
#             else:
#                 error_message_message = f"[{current_time_manila.strftime('%Y-%m-%d %H:%M:%S')}] Campaign: {campaign_name} Failed to Upload Video: {video_response.json()}."
#                 status = "Failed"
#                 append_redis_message_create_campaigns(user_id, error_message)
#                 upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=initial_message, status=status)
#                 return {"status": "failed", "error": "Failed to upload video", "details": video_response.json()}

#         upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=initial_message, status=status)

#         # Handle image upload
#         if image_url:
#             result = add_ad_image(ad_account_id, access_token, image_url, f"{campaign_name}-image")
#             if "error" in result:
#                 error_message = f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Campaign: {campaign_name} Thumbnail Failed to Upload: {result}."
#                 status = "Failed"
#                 append_redis_message_create_campaigns(user_id, error_message)
#                 upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=initial_message, status=status)
#                 return {"status": "failed", "error": "Failed to upload image", "details": result}

#             image_url_from_fb = result.get("image_url")

#         # Create creative
#         creative_response = create_ad_creative(
#             ad_account_id, access_token, creative_name, facebook_page_id, video_id, headline, primary_text, image_url_from_fb
#         )
#         if 'id' not in creative_response:
#             error_message = f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Campaign: {campaign_name} Failed to create creative: {creative_response}"
#             status = "Failed"
#             append_redis_message_create_campaigns(user_id, error_message)
#             upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=initial_message, status=status)
#             return {"status": "failed", "error": "Failed to create creative", "details": creative_response}

#         creative_id = creative_response['id']
#         logging.info(f"Creative ID : {creative_id} Campaign: {campaign_name}")
#         upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages="Creative Successfully Generated.", status="Generating")

#         # Fetch object_story_id
#         object_story_id_url = f"https://graph.facebook.com/v21.0/{creative_id}?fields=effective_object_story_id"
#         object_story_id = None
#         time.sleep(30)
#         for attempt in range(3):
#             object_story_response = requests.get(object_story_id_url, headers=headers)
#             response_json = object_story_response.json()

#             logging.info(f"Attempt {attempt+1}: Object Story Response JSON: {response_json}")

#             if object_story_response.status_code == 200 and "effective_object_story_id" in response_json:
#                 object_story_id = response_json["effective_object_story_id"]
#                 break
#             time.sleep(5)

#         if not object_story_id:
#             error_message = f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Failed to get POST ID: {response_json}."
#             upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=error_message, status="Failed")
#             return {"status": "failed", "error": "Object Story ID not found", "details": response_json}

#         # **First AdSet (BR - No Interests)**
#         first_adset_name = "BR"
#         first_adset_excluded_regions = adset_excluded_regions[0]['regions']
#         adset_response = create_adset(ad_account_id, access_token, campaign_id, first_adset_name, start_time, excluded_regions=first_adset_excluded_regions)

#         if 'id' not in adset_response:
#             error_message = f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}Failed to create adset {first_adset_name}, details: {adset_response}"
#             upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Failed to create adset {first_adset_name} details: {adset_response}")
#             append_redis_message_create_campaigns(user_id, error_message)
#             return {"status": "failed", "error": f"Failed to create adset {first_adset_name}", "details": adset_response}

#         first_adset_id = adset_response["id"]

#         # Create first ad for BR adset
#         first_ad_name = "BR-ad"
#         first_ad_response = create_ad(ad_account_id, access_token, first_ad_name, first_adset_id, creative_id)

#         if 'id' in first_ad_response:
#             json_adsets_ads_creatives["adsets"].append({
#                 "adset_name": first_adset_name,
#                 "adset_id": first_adset_id,
#                 "creative_id": creative_id,
#                 "object_story_id": object_story_id,
#                 "ads": {
#                     "ad_name": first_ad_name,
#                     "ad_id": first_ad_response["id"]
#                 }
#             })
#         else:
#             upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Failed to create ad {first_adset_name} details: {first_ad_response}")
#             logging.error(f"Failed to create ad for first adset {first_adset_name}: {first_ad_response}")

#         # **AdSet Creation via Scraping**
#         adset_count = 0
        
#         for i in interests_list:
#             adset_count += 1   # Define the number of adsets to be created
#         # Call scrape_website once and let it return multiple adsets
#         task = scrape_website.apply_async(args=[primary_text, product, ad_account_id, access_token, campaign_id, adset_count, start_time, adset_excluded_regions])

#         while not task.ready():
#             logging.info(f"Waiting for task result...")
#             time.sleep(2)

#         if task.successful():
#             scrape_result = task.result

#             if scrape_result.get("status") != "success":
#                 upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Failed to scrape and create adsets: {scrape_result.get('error')}")
#                 logging.error(f"Failed to scrape and create adsets: {scrape_result.get('error')}")
#                 return {"status": "failed", "error": scrape_result.get("error")}

#             created_adsets = scrape_result.get("created_adsets", [])

#             for idx, adset in enumerate(created_adsets):
#                 adset_id = adset["adset_id"]
#                 adset_name = adset["adset_name"]
#                 ads_name = f"{adset_name}-ad-{idx + 1}"

#                 # Create ad
#                 ad_response = create_ad(ad_account_id, access_token, ads_name, adset_id, creative_id)

#                 if 'id' in ad_response:
#                     json_adsets_ads_creatives["adsets"].append({
#                         "adset_name": adset_name,
#                         "adset_id": adset_id,
#                         "creative_id": creative_id,
#                         "object_story_id": object_story_id,
#                         "ads": {
#                             "ad_name": ads_name,
#                             "ad_id": ad_response["id"]
#                         }
#                     })
#                 else:
#                     logging.error(f"Failed to create ad for adset {adset_name}: {ad_response}")

#         upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] 🤖🎯 Campaign Created with AI Interest.", status="Created", adsets_ads_creatives=json_adsets_ads_creatives)

#         return {
#             "status": "success",
#             "message": f"{adset_count} Adset(s) created successfully.",
#             "campaign_id": campaign_id
#         }

#     except Exception as e:
#         logging.error(f"Unexpected error: {e}")
#         return {"status": "failed", "error": "An unexpected error occurred", "details": str(e)} 


from workers.on_off_functions.create_campaign_message import append_redis_message_create_campaigns

@shared_task(bind=True)
def create_simple_campaign_task(self, ad_account_id, user_id, access_token, campaign_id, campaign_name, page_name,
                                facebook_page_id, sku, material_code, campaign_code, daily_budget, headline, primary_text,
                                product, video_url, image_url, interests_list, start_time, adset_excluded_regions):
    print(f"📦 Received campaign_code: {campaign_code}")
    try:

        logging.info("Fetching campaign data from CSV...")
        append_redis_message_create_campaigns(user_id, f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Fetching campaign data from CSV...")

        # Log campaign details
        logging.info(f"Processing campaign: {campaign_name} (ID: {campaign_id}) for Ad Account: {ad_account_id}")
        append_redis_message_create_campaigns(user_id, f" [{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Processing campaign: {campaign_name} (ID: {campaign_id})")

        creative_name = f"{campaign_name}-creative"
        video_id = None
        image_url_from_fb = None
        json_adsets_ads_creatives = {"adsets": []}  # Initialize the nested structure
        headers = {"Authorization": f"Bearer {access_token}"}

        logging.info("CSV data successfully read & processing started.")
        append_redis_message_create_campaigns(user_id, f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] CSV data successfully read & processing started.")

        # Save the initial processing status
        initial_message = f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Starting campaign processing task for {campaign_name}."
        
        logging.info(f"🚀 {initial_message}")

        append_redis_message_create_campaigns(user_id, initial_message)
        upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages="...", status="Generating")
        upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=initial_message, status="Generating")

        # Handle video upload if a URL is provided
        if video_url:
            logging.info(f" Uploading video for campaign: {campaign_name}...")
            append_redis_message_create_campaigns(user_id, f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] 🎥⬆️ Uploading video for {campaign_name}...")

            downloadable_video_url = get_downloadable_drive_url(video_url)
            video_data = {
                "title": headline,
                "file_url": downloadable_video_url
            }

            video_upload_url = f"https://graph.facebook.com/v23.0/act_{ad_account_id}/advideos"
            video_response = requests.post(video_upload_url, headers=headers, json=video_data)

            logging.info(f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Video uploaded successfully for {campaign_name}")
            append_redis_message_create_campaigns(user_id, f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] 🎥✅ Video uploaded successfully for {campaign_name}")

            if video_response.status_code == 200:
                video_id = video_response.json().get('id')
            else:
                error_message = f"[{current_time_manila.strftime('%Y-%m-%d %H:%M:%S')}][{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] 🎥❌ Failed to upload video for {campaign_name}."
                append_redis_message_create_campaigns(user_id, error_message)
                upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=error_message, status="Failed")
                return {"status": "failed", "error": "Failed to upload video", "details": video_response.json()}

        if image_url:
            logging.info(f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Uploading image for campaign: {campaign_name}...")
            append_redis_message_create_campaigns(user_id, f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] ⬆️ Uploading image for {campaign_name}...")

            result = add_ad_image(ad_account_id, access_token, image_url, f"[{current_time_manila.strftime('%Y-%m-%d %H:%M:%S')}]{campaign_name}-image")
            if "error" in result:
                error_message = f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] ❌ Failed to upload image for {campaign_name}."
                append_redis_message_create_campaigns(user_id, error_message)
                upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=error_message, status="Failed")
                return {"status": "failed", "error": "Failed to upload image", "details": result}

            logging.info(f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Image uploaded successfully for {campaign_name}")
            append_redis_message_create_campaigns(user_id, f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] 📤 Image uploaded successfully for {campaign_name}")

            image_url_from_fb = result.get("image_url")

        # Create the single ad creative to be reused for all ad sets
        logging.info(f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Creating ad creative for campaign: {campaign_name}...")
        append_redis_message_create_campaigns(user_id, f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] 🎨 Creating ad creative for {campaign_name}...")

        creative_response = create_ad_creative(
            ad_account_id, access_token, creative_name,
            facebook_page_id, video_id, headline, primary_text, image_url_from_fb
        )
        if 'id' not in creative_response:
            error_message = f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Failed to create ad creative for {campaign_name}."
            append_redis_message_create_campaigns(user_id, error_message)
            upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=error_message, status="Failed")
            return {"status": "failed", "error": "Failed to create creative", "details": creative_response}

        creative_id = creative_response['id']
        json_adsets_ads_creatives["creative_id"] = creative_id  # Save creative ID

        logging.info(f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Ad creative successfully created for {campaign_name}")
        append_redis_message_create_campaigns(user_id, f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] ✅ Ad creative successfully created for {campaign_name}")

        logging.info(f"[{current_time_manila.strftime('%Y-%m-%d %H:%M:%S')}]Creative ID : {creative_id} Campaign: {campaign_name}")

        time.sleep(45)
        # Fetch object story ID
        logging.info(f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Fetching object story ID for campaign: {campaign_name}...")
        append_redis_message_create_campaigns(user_id, f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] 🔄 Fetching object story ID for {campaign_name}...")

        object_story_id_url = f"https://graph.facebook.com/v23.0/{creative_id}?fields=effective_object_story_id"

        object_story_id = None
        for attempt in range(8):
            object_story_response = requests.get(object_story_id_url, headers=headers)
            response_json = object_story_response.json()

            logging.info(f"Attempt {attempt+1}: Object Story Response JSON: {response_json}")

            if object_story_response.status_code == 200 and "effective_object_story_id" in response_json:
                object_story_id = response_json["effective_object_story_id"]
                break  # Exit loop if found

            time.sleep(10)

        if not object_story_id:
            error_message = f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Failed to get POST ID details: {response_json}."
            append_redis_message_create_campaigns(user_id, error_message)
            upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=error_message, status="Failed")
            return {"status": "failed", "error": "Object Story ID not found after retries", "details": response_json}

        logging.info(f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Object story ID retrieved for {campaign_name}")
        append_redis_message_create_campaigns(user_id, f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Object story ID retrieved for {campaign_name}")

        # Convert interest names into interest IDs
        def get_interest_ids(interest_words):
            logging.info(f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Converting interest words into Facebook interest IDs: {interest_words}")
            """Retrieve interest IDs from Facebook API, using the first ID with a valid 'path' key."""
            interest_ids = []
            FACEBOOK_GRAPH_API_URL = f"https://graph.facebook.com/v23.0/act_{ad_account_id}/targetingsearch"

            for word in interest_words:
                try:
                    params = {"q": word, "type": "adinterest"}
                    response = requests.get(FACEBOOK_GRAPH_API_URL, headers=headers, params=params)
                    response_data = response.json()

                    if "data" in response_data and response_data["data"]:
                        for interest in response_data["data"]:
                            interest_type = interest.get("type", "")

                            if interest_type == "interests" and "Interests" in interest.get("path", []):
                                interest_ids.append({"id": interest["id"], "type": "Interests"})
                                break
                            elif interest_type == "demographics" and "Demographics" in interest.get("path", []):
                                interest_ids.append({"id": interest["id"], "type": "Demographics"})
                                break
                            elif interest_type == "behaviors" and "Behaviors" in interest.get("path", []):
                                interest_ids.append({"id": interest["id"], "type": "Behaviors"})
                                break  # Stop after finding the first valid interest
                except Exception as e:
                    error_message = f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Failed to get Target Audience details: {response}."
                    append_redis_message_create_campaigns(user_id, error_message)
                    upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=error_message, status="Failed")
                    logging.info(f"Error processing interest '{word}': {e}")
            
            return interest_ids

        adset_count = 0  # Initialize counter

        for i in interests_list:
            adset_count += 1  # Increment for each list # Adjust count to include all interest lists

        logging.info(f"Manually Counted Adset Count: {adset_count}")

        # Create multiple ad sets with interests
        logging.info(f"[{current_time_manila.strftime('%Y-%m-%d %H:%M:%S')}] Creating {adset_count} ad sets for campaign: {campaign_name}")
        append_redis_message_create_campaigns(user_id, f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Creating {adset_count} ad sets for campaign: {campaign_name}")

        for adset_index, interest_words in enumerate(interests_list):
            # If no interests, name it "BR", otherwise join interest words as the name
            logging.info(f"🛠 Creating AdSet {adset_index + 1} - {interest_words}")
            append_redis_message_create_campaigns(user_id, f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Creating AdSet {adset_index + 1} - {interest_words}")

            time.sleep(5)

            adset_name = "BR" if not interest_words else ", ".join(interest_words)
            ads_name = f"{adset_name}-ad"
            interest_ids = get_interest_ids(interest_words)

            logging.info(f"Target Audiences: {interest_ids}")
            
            # Get the excluded regions for the current adset by using the adset index
            excluded_regions = adset_excluded_regions[adset_index]['regions']
            # Create the ad set with interests
            adset_response = create_adset(ad_account_id, access_token, campaign_id, adset_name, start_time, interest_ids, excluded_regions)
            if 'id' not in adset_response:
                error_message = f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}Failed to create adset {adset_name}, details: {adset_response}"
                upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=error_message, status="Failed")
                append_redis_message_create_campaigns(user_id, error_message)
                return {"status": "failed", "error": f"Failed to create adset {adset_name}", "details": adset_response}
            
                
            adset_id = adset_response['id']

            if adset_index == 0:
                # First AdSet - Use create_ad function
                ad_response = create_ad(ad_account_id, access_token, ads_name, adset_id, creative_id)
            else:
                # Subsequent AdSets - Use create_ad_usepost function
                ad_response = create_ad_usepost(ad_account_id, access_token, ads_name, adset_id, object_story_id)

            if 'id' not in ad_response:
                error_message = f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}]Failed to create ad for adset {adset_name}, details: {ad_response}"
                upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=error_message, status="Failed")
                append_redis_message_create_campaigns(user_id, error_message)
                return {"status": "failed", "error": f"Failed to create ad for adset {adset_name}", "details": ad_response}

            ad_id = ad_response['id']
            logging.info(f"AD ID : {ad_id} ADSET: {adset_name}")

            # Append the adset and ad details to the JSON structure
            json_adsets_ads_creatives["adsets"].append({
                "adset_name": adset_name,
                "adset_id": adset_id,
                "creative_id": creative_id,
                "ads": {
                    "ad_name": ads_name,
                    "ad_id": ad_id
                }
            })

            logging.info(f"AD ID : {ad_id} ADSET: {adset_name}")

            logging.info(f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] AdSet {adset_index + 1} successfully created for {campaign_name}")
            append_redis_message_create_campaigns(user_id, f" [{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] adSet {adset_index + 1} successfully created for {campaign_name}")

        success_message = f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Campaign: {campaign_name} Created with {adset_count} Adset(s) and Ad(s)"
        append_redis_message_create_campaigns(user_id, success_message)
        upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=success_message, status="Created", adsets_ads_creatives=json_adsets_ads_creatives)

        return {
            "status": "success",
            "message": f"{adset_count} Adset(s) and Ad(s) created successfully using a single creative.",
            "campaign_id": campaign_id
        }
    except Exception as e:
        error_message = f"[{datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')}] Unexpected error for campaign {campaign_name}: {str(e)}."
        append_redis_message_create_campaigns(user_id, error_message)
        logging.error(error_message)
        upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=error_message, status="Failed")
        return {"status": "failed", "error": "An unexpected error occurred", "details": str(e)}
