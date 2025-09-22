from flask import Blueprint, request, jsonify
from controllers.campaign_code_controller import create_campaign_code, get_campaign_code, update_campaign_code, delete_campaign_code
from controllers.access_token_controller import create_access_token, get_access_tokens, update_access_token, delete_access_token
from controllers.invite_code_controller import generate_invite_code, get_invite_codes, use_invite_code, verify_invite_code
from controllers.user_relationship_controller import get_relationships, delete_relationship, check_relationship
from controllers.ad_account_controller import fetch_ad_accounts, refetch_ad_accounts, get_ad_accounts_from_db
from models.models import User, UserRelationship, manila_tz, db
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime

user_routes = Blueprint('user_routes', __name__)

# GET campaign codes for a specific user (by user_id)
@user_routes.route('/user/<string:user_id>/campaign-codes', methods=['GET'])
def fetch_campaign_codes(user_id):
    # Call the controller function to get campaign code for the user
    return get_campaign_code(user_id)

# POST a new campaign code for the user
@user_routes.route('/user/campaign-codes', methods=['POST'])
def add_campaign_code():
    user_id = request.json.get('user_id')
    campaign_code = request.json.get('campaign_code')

    # Validate input
    if not user_id or not campaign_code:
        return jsonify({"error": "Missing data"}), 400

    try:
        # Call the controller function which handles DB logic
        return create_campaign_code(user_id=user_id, campaign_code=campaign_code)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# PUT endpoint to update a campaign code
@user_routes.route('/user/campaign-codes/<int:code_id>', methods=['PUT'])
def put_campaign_code(code_id):
    return update_campaign_code(code_id)

@user_routes.route('/user/campaign-codes/<int:code_id>', methods=['DELETE'])
def delete_campaign_code_route(code_id):
    user_id = request.args.get('user_id')  # Get the user_id from the query parameter
    
    if not user_id:
        return jsonify({'error': 'user_id is required'}), 400

    try:
        return delete_campaign_code(code_id, user_id)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    

# === Access Token Routes ===

@user_routes.route('/user/<int:user_id>/access-tokens', methods=['GET'])
def fetch_access_tokens(user_id):
    return get_access_tokens(user_id)

@user_routes.route('/user/access-tokens', methods=['POST'])
def add_access_token():
    user_id = request.json.get('user_id')
    token = request.json.get('access_token')

    if not user_id or not token:
        return jsonify({"error": "Missing user_id or access_token"}), 400

    return create_access_token(user_id, token)

@user_routes.route('/user/access-tokens/<int:token_id>', methods=['DELETE'])
def remove_access_token(token_id):
    user_id = request.args.get('user_id')

    if not user_id:
        return jsonify({'error': 'user_id is required'}), 400

    return delete_access_token(token_id, int(user_id))

# === Ad Accounts Routes ===

@user_routes.route('/user/ad-accounts', methods=['GET','POST'])
def fetch_user_ad_accounts():
    """Fetch ad accounts from Facebook API and save to database"""
    return fetch_ad_accounts()

@user_routes.route('/user/ad-accounts/refetch', methods=['POST'])
def refetch_user_ad_accounts():
    """Refetch ad accounts from Facebook API, replacing all existing records"""
    return refetch_ad_accounts()

@user_routes.route('/user/ad-accounts/local', methods=['GET'])
def get_local_ad_accounts():
    """Get ad accounts from local database"""
    return get_ad_accounts_from_db()

# === Invite Code Routes ===

@user_routes.route('/user/<int:superadmin_id>/invite-codes', methods=['GET'])
def fetch_invite_codes(superadmin_id):
    return get_invite_codes(superadmin_id)

@user_routes.route('/user/invite-codes', methods=['POST'])
def create_invite_code():
    superadmin_id = request.json.get('superadmin_id')
    if not superadmin_id:
        return jsonify({"error": "Missing superadmin_id"}), 400
    return generate_invite_code(superadmin_id)

@user_routes.route('/user/invite-codes/use', methods=['POST'])
def redeem_invite_code():
    invite_code = request.json.get('invite_code')
    user_id = request.json.get('user_id')
    
    if not invite_code or not user_id:
        return jsonify({"error": "Missing invite_code or user_id"}), 400
        
    return use_invite_code(invite_code, user_id)

@user_routes.route('/user/invite-codes/verify', methods=['POST'])
def verify_invite_code_route():
    invite_code = request.json.get('invite_code')
    
    if not invite_code:
        return jsonify({"error": "Missing invite_code"}), 400
        
    return verify_invite_code(invite_code)

# === User Relationship Routes ===

@user_routes.route('/user/relationships', methods=['GET'])
def fetch_relationships():
    try:
        # Get superadmin_id from query parameters
        superadmin_id = request.args.get('superadmin_id')
        if not superadmin_id:
            return jsonify({'error': 'superadmin_id is required'}), 400
            
        return get_relationships(int(superadmin_id))
    except Exception as e:
        print("[EXCEPTION]", str(e))
        return jsonify({'error': 'Internal Server Error'}), 500

@user_routes.route('/user/relationships/<int:relationship_id>', methods=['DELETE'])
def remove_relationship(relationship_id):
    try:
        # Get superadmin_id from query parameters
        superadmin_id = request.args.get('superadmin_id')
        if not superadmin_id:
            return jsonify({'error': 'superadmin_id is required'}), 400
            
        return delete_relationship(relationship_id, int(superadmin_id))
    except Exception as e:
        print("[EXCEPTION]", str(e))
        return jsonify({'error': 'Internal Server Error'}), 500

@user_routes.route('/relationships', methods=['GET'])
def get_user_relationships():
    superadmin_id = request.args.get('superadmin_id')
    if not superadmin_id:
        return jsonify({'error': 'superadmin_id is required'}), 400
    return get_relationships(superadmin_id)

@user_routes.route('/relationships/<int:relationship_id>', methods=['DELETE'])
def delete_user_relationship(relationship_id):
    superadmin_id = request.args.get('superadmin_id')
    if not superadmin_id:
        return jsonify({'error': 'superadmin_id is required'}), 400
    return delete_relationship(relationship_id, superadmin_id)

@user_routes.route('/check-relationship', methods=['GET'])
def check_user_relationship():
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({'error': 'user_id is required'}), 400
    return check_relationship(user_id)