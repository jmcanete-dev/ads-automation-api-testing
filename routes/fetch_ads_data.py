from flask import Blueprint, request, jsonify
from controllers.fetch_ads_controller import fetch_campaigns_with_insights  # Updated import

# Create the Blueprint for the route
fetch_campaign_adsets_ads_creatives_bp = Blueprint('fetch_campaign_adsets_ads_creatives_bp', __name__)

# Route to fetch campaigns, adsets, ads, and creatives with insights in one call
@fetch_campaign_adsets_ads_creatives_bp.route('/fetch_campaign_adsets_ads_creatives', methods=['GET'])
def fetch_campaign_adsets_ads_creatives_route():
    # Get parameters from the request
    ad_account_id = request.args.get('ad_account_id')
    access_token = request.args.get('access_token')

    # Validate input parameters
    if not ad_account_id or not access_token:
        return jsonify({"error": "Missing required parameters: ad_account_id and access_token"}), 400

    # Call the controller function to fetch data
    result = fetch_campaigns_with_insights(ad_account_id, access_token)

    # Return the result as JSON
    return jsonify(result)
