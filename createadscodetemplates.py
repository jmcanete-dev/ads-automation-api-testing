# @createbp.route('/create-campaign', methods=['POST'])
# def create_campaign_route():
#     try:
#         # Get data from the request body
#         data = request.json
#         ad_account_id = data.get('ad_account_id')
#         access_token = data.get('access_token')
#         campaign_name = data.get('campaign_name')
#         daily_budget = data.get('daily_budget')

#         # Validate required fields
#         if not all([ad_account_id, access_token, campaign_name, daily_budget]):
#             return jsonify({"error": "Missing required fields"}), 400

#         # Validate daily_budget is a positive integer
#         if not isinstance(daily_budget, int) or daily_budget <= 0:
#             return jsonify({"error": "The daily budget must be a positive integer in cents (e.g., 5000 for $50)."}), 400

#         # Multiply the daily budget by 10
#         adjusted_daily_budget = daily_budget * 10

#         # Call the create_campaign function with the adjusted daily budget
#         response = create_campaign(ad_account_id, access_token, campaign_name, adjusted_daily_budget)
#         return jsonify(response), 200

#     except ValueError as e:
#         # Return validation-related errors
#         return jsonify({"error": str(e)}), 400
#     except Exception as e:
#         # Return unexpected errors
#         return jsonify({"error": "An error occurred", "details": str(e)}), 500
    
# @createbp.route('/create-adset', methods=['POST'])
# def create_adset_route():
#     try:
#         # Get data from the request body
#         data = request.json
#         ad_account_id = data.get("ad_account_id")
#         access_token = data.get("access_token")
#         campaign_id = data.get("campaign_id")
#         adset_name = data.get("adset_name")
#         interests = data.get("interests", [])  # Default to an empty list if not provided

#         # Validate required fields
#         if not all([ad_account_id, access_token, campaign_id, adset_name]):
#             return jsonify({"error": "Missing required fields"}), 400

#         # Get the current date in the Asia/Manila timezone
#         manila_tz = pytz.timezone("Asia/Manila")
#         current_time_manila = datetime.now(manila_tz)

#         # Calculate the start_time (next day in YYYY-MM-DD format)
#         start_time = (current_time_manila + timedelta(days=1)).strftime('%Y-%m-%d')

#         # Validate interests if provided
#         if not isinstance(interests, list):
#             return jsonify({"error": "'interests' must be a list of key-value pairs"}), 400

#         # Format interests into key-value pairs if provided
#         formatted_interests = [{"id": interest.get("id"), "name": interest.get("name")} for interest in interests]

#         # Call the create_adset function with all parameters
#         response = create_adset(ad_account_id, access_token, campaign_id, adset_name, start_time, formatted_interests)
#         return jsonify(response), 200

#     except Exception as e:
#         # Return unexpected errors
#         return jsonify({"error": "An error occurred", "details": str(e)}), 500


# @createbp.route('/create-ad-creative', methods=['POST'])
# def create_ad_creatives():
#     try:
#         # Extract data from the incoming request body
#         data = request.json
#         ad_account_id = data.get('ad_account_id')
#         access_token = data.get('access_token')
#         name = data.get('name')
#         page_id = data.get('page_id')
#         video_id = data.get('video_id')
#         title = data.get('title')
#         message = data.get('message')
#         image_hash = data.get('image_hash')

#         # Validate that all required fields are present
#         if not all([ad_account_id, access_token, name, page_id, video_id, title, message, image_hash]):
#             return jsonify({"error": "Missing required fields"}), 400

#         # Call the function to create the ad creative
#         response = create_ad_creative(ad_account_id, access_token, name, page_id, video_id, title, message, image_hash)

#         # Return the response from Facebook API
#         return jsonify(response), 200

#     except Exception as e:
#         # Handle any errors that occur
#         return jsonify({"error": "An error occurred", "details": str(e)}), 500
    
# @createbp.route('/create_ad', methods=['POST'])
# def create_ad_route():
#     try:
#         # Get the input data from the request body
#         data = request.get_json()

#         # Extract the parameters
#         ad_account_id = data.get('ad_account_id')
#         access_token = data.get('access_token')
#         name = data.get('name')
#         adset_id = data.get('adset_id')
#         creative_id = data.get('creative_id')

#         # Call the function to create the ad
#         response = create_ad(ad_account_id, access_token, name, adset_id, creative_id)

#         # Return the response as JSON
#         return jsonify(response), 200
#     except Exception as e:
#         # Handle errors and return a bad request response
#         return jsonify({"error": str(e)}), 400