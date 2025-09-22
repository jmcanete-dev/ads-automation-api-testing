from flask import Blueprint, request, jsonify
from controllers.verify_ad_accounts_controllers import verify_ad_accounts, verify_ad_account_id

verify_ad_accounts_bp = Blueprint('verify_ad_accounts', __name__)

@verify_ad_accounts_bp.route('/verify', methods=['POST'])
def verify():
    data = request.json
    return verify_ad_accounts(data)

@verify_ad_accounts_bp.route('/verify/adaccount', methods=['POST'])
def verify_ad_account():
    data = request.json
    if not isinstance(data, list):
        return jsonify({"error": "Request body must be a JSON array"}), 400
    return verify_ad_account_id(data)
