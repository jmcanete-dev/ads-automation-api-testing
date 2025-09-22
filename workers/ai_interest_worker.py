# import logging
# import re
# import pytgpt.ai4chat as ai
# import requests
# import json
# from celery import shared_task
# from controllers.create_ads_controller import create_adset


# ACCESS_TOKEN = "EAAeTILCMsl8BO2NpuWfXmyXwZB15eGy9f7cqZCdSby5SNBdW5bhtfSsfwjRPdeTsBPAZCtVzaytK6lSZCHYwhAYQcPYuV0Qq8SyAXxUjWwb1j3VZBnHzQe3nc5iGiZBZAdjvRxd1ODZCeBYt4QaaZAeVtZBim7HQnXqfPaCv90Rfo6DzNmyDTkKCTsJ61oKh47XLQQEBXL5yQm"

# def extract_keywords_from_ai(caption, product, prev_interest=None):
#     system_content = """{
#         "instructions": [
#             "Strictly Create new Keywords and dont create keywords based on the previous interest data"
#             "Create different keywords"
#             "You will receive product and captions for advertisement data.",
#             "Analyze the content and generate the best relevant keywords.",
#             "Strictly in 1 or 2 words related words per keywords ONLY.",
#             "Generate keywords based on product use, problems, and what the product based off",
#             "Focus on meaningful and specific keywords related to the product and caption.",
#             "Generate keywords based on product use, category, problem it solve.",
#             "The keywords are in English only.",
#             "The caption is in Tagalog, English, or Taglish, so analyze the keywords that can be extracted from Tagalog words.",
#             "The product name is general words, so extract keywords that best advertise the product.",
#             "Remove unrelated keywords based on the product.",
#             "Output in JSON format only without any explanations or additional text."
#         ],
#         "response_format": "JSON",
#         "output_format": {
#             "interests": [
#                 "keyword1",
#                 "keyword2",
#                 "keyword3"
#             ]
#         }
#     }"""

#     messages = [
#         {"role": "system", "content": system_content},
#         {"role": "user", "content": f"Product: {product}, Caption: {caption}, Previous_Interest: {prev_interest}"}
#     ]

#     bot = ai.AI4CHAT()
#     max_retries = 3
#     for attempt in range(max_retries):
#         response = bot.chat(messages)

#         try:
#             response_dict = json.loads(response)
#             return response_dict.get("interests", [])
#         except json.JSONDecodeError as e:
#             logging.warning(f"Attempt {attempt + 1}/{max_retries}: Error decoding AI response: {e}")
#             logging.warning(f"Response: {response}")

#     logging.error("Max retries reached. Returning empty keywords.")
#     return []


# def refine_best_interests_with_ai(all_interests, initial_keywords, caption, product, prev_adset_interest=None):
#     bot = ai.AI4CHAT()
    
#     # Remove previously selected interests from available list
#     available_interests = [i for i in all_interests if i["id"] not in (prev_adset_interest or [])]

#     if len(available_interests) < 3:
#         logging.warning("Not enough unique interests left to select 3 new ones.")
#         return []

#     # AI prompt to refine interests, demographics, and behaviors
#     ai_prompt = {
#         "instructions": [
#             "You will receive a list of Facebook Ad Interests, Demographics, and Behaviors with their IDs and types.",
#             "STRICTLY SELECT ONLY FROM THE GIVEN INTEREST_DATA. DO NOT GENERATE YOUR OWN INTEREST, DEMOGRAPHIC, OR BEHAVIOR.",
#             "Exclude any IDs that are already in prev_adset_interest.",
#             "Focus only on relevant interests, demographics, and behaviors that match the product.",
#             "STRICTLY DO NOT CHOOSE INTERESTS THAT DO NOT CATEGORIZE WITH THE PRODUCT.",
#             "I TOLD YOU DONT CHOOSE UNRELATED TO PRODUCT.",
#             "Do not select brands, specific business names, or unrelated terms.",
#             "Prioritize interests with higher audience relevance but ensure they are product-related.",
#             "Ensure the response contains exactly 3 selected items.",
#             "Return the selected items in JSON format with 'id', 'name', and 'type'.",
#             "No additional text or explanations.",
#             "Output in strict JSON format only."
#         ],
#         "interests_data": available_interests,  # Pass only unselected interests
#         "initial_keywords": initial_keywords,
#         "previous_interest": prev_adset_interest or [],
#         "caption": caption,
#         "product": product,
#         "response_format": "JSON",
#         "output_format": {
#             "selected_interests": [
#                 {"id": "123456", "name": "Interest Name", "type": "Interest"},
#                 {"id": "654321", "name": "Demographics Name", "type": "Demographics"},
#                 {"id": "789012", "name": "Behavior Name", "type": "Behavior"}
#             ]
#         }
#     }

#     messages = [
#         {"role": "system", "content": json.dumps(ai_prompt)}
#     ]

#     response = bot.chat(messages)

#     try:
#         refined_response = json.loads(response)
#         if isinstance(refined_response, dict):
#             selected_interests = refined_response.get("selected_interests", [])[:3]

#             # Ensure AI response contains exactly 3 interests
#             if len(selected_interests) == 3:
#                 return selected_interests
#             else:
#                 logging.error(f"AI returned insufficient interests: {selected_interests}")
#                 return []
#         return []
#     except json.JSONDecodeError as e:
#         logging.error(f"Error decoding AI response: {e}")
#         return []
    
# def fetch_facebook_ad_interests(keyword, ad_account_id, access_token):
#     """Fetch Facebook interests, demographics, and behaviors for a keyword."""
#     FACEBOOK_GRAPH_API_URL = f"https://graph.facebook.com/v20.0/act_{ad_account_id}/targetingsearch"
#     headers = {"Authorization": f"Bearer {access_token}"}
#     keywords_to_try = [keyword] + keyword.split()
    
#     def fetch_interest_for_keyword(term):
#         """Retrieve interest IDs from Facebook API with proper categorization."""
#         params = {"q": term.strip(), "type": "adinterest", "limit": 10}

#         try:
#             response = requests.get(FACEBOOK_GRAPH_API_URL, headers=headers, params=params)
#             response_data = response.json()

#             if response.status_code != 200:
#                 logging.error(f"Error fetching data for keyword '{term}': {response.text}")
#                 return []

#             interest_ids = []
#             if "data" in response_data and response_data["data"]:
#                 for interest in response_data["data"]:
#                     interest_type = interest.get("type", "")
#                     interest_path = interest.get("path", [])

#                     if interest_type == "interests" and "Interests" in interest_path:
#                         interest_ids.append({"id": interest["id"], "type": "Interests", "name": interest["name"]})
#                     elif interest_type == "demographics" and "Demographics" in interest_path:
#                         interest_ids.append({"id": interest["id"], "type": "Demographics", "name": interest["name"]})
#                     elif interest_type == "behaviors" and "Behaviors" in interest_path:
#                         interest_ids.append({"id": interest["id"], "type": "Behaviors", "name": interest["name"]})

#             return interest_ids

#         except requests.RequestException as e:
#             logging.error(f"Request error for keyword '{term}': {e}")
#             return []

#     # Try each keyword until valid interests are found
#     all_interests = []
#     for term in keywords_to_try:
#         logging.info(f"Trying keyword '{term}' for fetching interests.")
#         interests = fetch_interest_for_keyword(term)
#         if interests:
#             all_interests.extend(interests)

#     if not all_interests:
#         logging.warning(f"No interests found for keyword '{keyword}' or its components.")

#     return all_interests

# @shared_task
# def scrape_website(caption, product, ad_account_id, access_token, campaign_id, adset_count, start_time, adset_excluded_regions):
#     try:
#         all_fetched_interests = []
#         used_interests = set()
#         created_adsets = []

#         logging.info(f"PRODUCT: {product}")
#         logging.info(f"CAPTION: {caption}")
#         logging.info(f"Generating {adset_count} unique AdSets")

#         # Step 1: Extract keywords from AI
#         logging.info("Getting Keywords")
#         keywords = extract_keywords_from_ai(caption, product)
#         logging.info(f"Extracted Keywords: {keywords}")

#         if not keywords:
#             logging.error("No valid keywords extracted from AI.")
#             return {"status": "failed", "error": "No valid keywords extracted from AI."}

#         # Step 2: Fetch and store interests for all keywords
#         logging.info("Fetching Facebook Interests for Keywords")
#         for keyword in keywords:
#             logging.info(f"Processing keyword: {keyword}")
#             interests = fetch_facebook_ad_interests(keyword, ad_account_id, access_token)
#             logging.info(f"Fetched Interests: {interests}")

#             if interests:
#                 all_fetched_interests.extend(interests)

#         if not all_fetched_interests:
#             logging.warning("No valid interests found for any keyword.")
#             return {"status": "failed", "error": "No valid interests found."}

#         logging.info(f"All fetched Interests: {all_fetched_interests}")

#         # Step 3: Generate AdSets
#         for i in range(2, adset_count + 1):  # Start from 2 (index 1)
#             adset_index = i - 1  # Start the exclusion index from 1 (not 0)

#             # Retrieve excluded regions from the provided list
#             excluded_regions = adset_excluded_regions[adset_index]['regions'] if adset_index < len(adset_excluded_regions) else []

#             logging.info(f"Generating AdSet {i}/{adset_count} with excluded regions: {excluded_regions}")

#             # AI Refinement to get 3 unique interests
#             refined_interests = refine_best_interests_with_ai(
#                 all_fetched_interests, keywords, caption, product, list(used_interests)
#             )
#             if not refined_interests or len(refined_interests) < 3:
#                 logging.warning(f"Skipping AdSet {i} due to insufficient unique interests.")
#                 continue

#             # Ensure interests are unique across adsets
#             unique_interests = [interest for interest in refined_interests if interest["id"] not in used_interests]
            
#             if len(unique_interests) < 3:
#                 logging.warning(f"Skipping AdSet {i} due to lack of enough distinct interests.")
#                 continue

#             # Mark these interests as used
#             used_interests.update([interest["id"] for interest in unique_interests])

#             # Remove selected interests from `all_fetched_interests`
#             all_fetched_interests = [i for i in all_fetched_interests if i["id"] not in used_interests]

#             interest_names = [interest["name"] for interest in unique_interests]
#             interest_ids = [interest["id"] for interest in unique_interests]

#             # Construct AdSet name based on interest names
#             adset_name = f"{', '.join(interest_names)}"
#             adset_name = re.sub(r"\s*\(.*?\)", "", adset_name).strip()

#             logging.info(f"Creating AdSet '{adset_name}' with interests: {interest_names} and excluded regions: {excluded_regions}")

#             # Step 4: Create AdSet with excluded regions
#             adset_response = create_adset(
#                 ad_account_id,
#                 access_token,
#                 campaign_id,
#                 adset_name,
#                 start_time,
#                 interest_ids,  # Pass interest IDs, not refined_interests
#                 excluded_regions  # Pass the excluded regions
#             )

#             if "id" in adset_response:
#                 created_adsets.append({
#                     "adset_name": adset_name,
#                     "adset_id": adset_response["id"],
#                     "interest_ids": interest_ids,
#                     "interest_names": interest_names,
#                     "excluded_regions": excluded_regions
#                 })
#             else:
#                 logging.error(f"Failed to create AdSet {adset_name}: {adset_response}")

#         if not created_adsets:
#             logging.error("Failed to create any AdSets.")
#             return {"status": "failed", "error": "No AdSets were successfully created."}

#         return {"status": "success", "created_adsets": created_adsets}

#     except Exception as e:
#         logging.error(f"Error occurred: {str(e)}", exc_info=True)
#         return {"status": "failed", "error": "An unexpected error occurred", "details": str(e)}
