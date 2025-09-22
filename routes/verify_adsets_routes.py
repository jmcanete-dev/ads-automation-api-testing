from flask import Blueprint, request, jsonify
from controllers.verify_adsets_controller import verify_ad_accounts

verify_adsets_accounts_bp = Blueprint('verify_adsets_accounts', __name__)

@verify_adsets_accounts_bp.route('/adsets', methods=['POST'])
def verify_adsets():
    data = request.get_json()

    print("Received Data:", data)  # 🔹 Debugging log

    # 🔹 Extract 'campaigns' list if it's wrapped inside a dictionary
    if isinstance(data, dict) and "campaigns" in data:
        data = data["campaigns"]

    # 🔹 Validate that data is now a list
    if not isinstance(data, list):
        return jsonify({"error": "Expected a JSON list but got something else"}), 400

    # 🔹 Proceed with account verification
    return verify_ad_accounts(data)