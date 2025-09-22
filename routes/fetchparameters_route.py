from flask import Blueprint, json, request, jsonify
import requests

# Create a new Blueprint for parameters-related functionality
parameters_bp = Blueprint('parameters', __name__)

@parameters_bp.route('/get_interests', methods=['GET'])
def get_interests():
    """
    Function to fetch a list of suggested interests based on a keyword from Facebook's Graph API.
    This will return the id and name of the interests to use in ad targeting.

    :return: JSON response with interest IDs and names
    """
    try:
        # Extract the access token from the headers (as a Bearer token)
        access_token = request.headers.get('Authorization')

        if not access_token:
            return jsonify({"error": "Authorization Bearer token is required"}), 400

        # Strip 'Bearer ' from the token if it's present
        if access_token.startswith('Bearer '):
            access_token = access_token[7:]

        # Get the list of interests from query parameters
        interest_list = request.args.get('interest_list', '')

        if not interest_list:
            return jsonify({"error": "interest_list is required"}), 400

        # Parse the interest list
        interests = [interest.strip() for interest in interest_list.split(',') if interest.strip()]

        if not interests:
            return jsonify({"error": "Valid interests are required"}), 400

        # Define the interest type
        interest_type = "adinterestsuggestion"

        # List to hold all interest suggestions
        interest_suggestions = []

        # Loop through each interest in the provided list and fetch suggestions
        for interest in interests:
            # Construct the URL to fetch interest suggestions
            url = "https://graph.facebook.com/v23.0/search"
            params = {
                'interest_list': f'["{interest}"]',  # JSON-encoded string
                'type': interest_type
            }

            # Set headers with the Bearer token for Facebook's API
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }

            # Make the GET request to Facebook's Graph API
            response = requests.get(url, headers=headers, params=params)

            # Check for any errors in the response
            if response.status_code != 200:
                return jsonify({"error": f"Failed to retrieve interests for '{interest}'"}), response.status_code

            data = response.json()

            # If the response contains data, append the interests to the list
            if 'data' in data:
                for interest_data in data['data']:
                    interest_suggestions.append({
                        "interest_name": interest_data.get('name'),
                        "interest_id": interest_data.get('id')
                    })

        return jsonify({"interests": interest_suggestions}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
   
@parameters_bp.route('/get_ad_videos', methods=['GET'])
def get_ad_videos():
    """
    Function to fetch ad videos from a Facebook Ad Account or Business Account.
    It returns video details like title, ID, created time, and length.

    :return: JSON response with video details
    """
    try:
        # Extract the access token from the request headers (Bearer token)
        access_token = request.headers.get('Authorization')

        if not access_token:
            return jsonify({"error": "Authorization Bearer token is required"}), 400

        # Strip 'Bearer ' from the token if it's present
        if access_token.startswith('Bearer '):
            access_token = access_token[7:]

        # Extract the ad_account_id from query parameters
        ad_account_id = request.args.get('ad_account_id')

        if not ad_account_id:
            return jsonify({"error": "ad_account_id is required"}), 400

        # Define the API URL with the ad_account_id
        url = f"https://graph.facebook.com/v23.0/act_{ad_account_id}/advideos"

        # Define the fields parameter to get the video details
        fields = 'title,id,created_time,length'

        # Define the parameters for the API request
        params = {
            'access_token': access_token,
            'fields': fields
        }

        # Make the GET request to the Facebook API
        response = requests.get(url, params=params)

        # Check for any errors in the response
        if response.status_code != 200:
            return jsonify({"error": f"Failed to retrieve ad videos for account '{ad_account_id}'"}), 500

        # Parse the response JSON
        data = response.json()

        # If the response contains video data, return the videos
        if 'data' in data:
            return jsonify({"videos": data['data']}), 200
        else:
            return jsonify({"message": "No videos found"}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

@parameters_bp.route('/get_ad_images', methods=['GET'])
def get_ad_images():
    """
    Function to fetch ad images from a Facebook Ad Account or Business Account.
    It returns image details like name, ID, hash, and URL.

    :return: JSON response with image details
    """
    try:
        # Extract the access token from the request headers (Bearer token)
        access_token = request.headers.get('Authorization')

        if not access_token:
            return jsonify({"error": "Authorization Bearer token is required"}), 400

        # Strip 'Bearer ' from the token if it's present
        if access_token.startswith('Bearer '):
            access_token = access_token[7:]

        # Extract the ad_account_id from query parameters
        ad_account_id = request.args.get('ad_account_id')

        if not ad_account_id:
            return jsonify({"error": "ad_account_id is required"}), 400

        # Define the API URL with the ad_account_id
        url = f"https://graph.facebook.com/v23.0/act_{ad_account_id}/adimages"

        # Define the fields parameter to get the image details
        fields = 'name,id,hash,url'

        # Define the parameters for the API request
        params = {
            'access_token': access_token,
            'fields': fields
        }

        # Make the GET request to the Facebook API
        response = requests.get(url, params=params)

        # Check for any errors in the response
        if response.status_code != 200:
            return jsonify({"error": f"Failed to retrieve ad images for account '{ad_account_id}'"}), 500

        # Parse the response JSON
        data = response.json()

        # If the response contains image data, return the images
        if 'data' in data:
            return jsonify({"images": data['data']}), 200
        else:
            return jsonify({"message": "No images found"}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@parameters_bp.route('/get_best_interests', methods=['POST'])
def get_best_interests():
    """
    Function to fetch the best matching interest for up to three provided interest keywords.
    If a keyword doesn't return data, retries with another keyword to find a suitable match.
    Ensures no duplicate interest ID-name pairs are returned.

    :return: JSON response with the best-matching interest ID and name for each keyword.
    """
    try:
        # Extract the access token from the headers (as a Bearer token)
        access_token = request.headers.get('Authorization')

        if not access_token:
            return jsonify({"error": "Authorization Bearer token is required"}), 400

        # Strip 'Bearer ' from the token if it's present
        if access_token.startswith('Bearer '):
            access_token = access_token[7:]

        # Parse the JSON body to get the interest keywords
        request_data = request.get_json()
        if not request_data or 'interest_keywords' not in request_data:
            return jsonify({"error": "interest_keywords is required in the JSON body"}), 400

        interest_keywords = request_data.get('interest_keywords', [])

        # Ensure we have up to three interest keywords
        if not isinstance(interest_keywords, list) or len(interest_keywords) > 3:
            return jsonify({"error": "Provide up to 3 interest keywords as a list"}), 400

        # Prepare the result dictionary to hold the best match for each keyword
        best_matches = []
        seen_ids = set()  # To avoid duplicate interest ID-name pairs

        # Define the interest type
        interest_type = "adinterestsuggestion"

        # Helper function to call the API and get interest suggestions
        def fetch_interests_for_keyword(keyword):
            url = "https://graph.facebook.com/v23.0/search"
            params = {
                'interest_list': f'[{json.dumps(keyword)}]',  # JSON-encoded string
                'type': interest_type
            }
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                return response.json().get('data', [])
            return []

        # Loop through each interest keyword
        for keyword in interest_keywords:
            # Fetch suggestions for the current keyword
            suggestions = fetch_interests_for_keyword(keyword)

            # If no suggestions are found, try fetching suggestions for another keyword
            if not suggestions:
                for fallback_keyword in interest_keywords:
                    if fallback_keyword != keyword:
                        suggestions = fetch_interests_for_keyword(fallback_keyword)
                        if suggestions:
                            break

            # If suggestions are available, find the best match not already selected
            for suggestion in suggestions:
                interest_id = suggestion.get('id')
                interest_name = suggestion.get('name')
                if interest_id not in seen_ids:
                    seen_ids.add(interest_id)  # Mark as seen
                    best_matches.append({
                        "input_keyword": keyword,
                        "interest_name": interest_name,
                        "interest_id": interest_id
                    })
                    break  # Add only one match per keyword

        return jsonify({"best_matches": best_matches}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
