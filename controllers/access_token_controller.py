from venv import logger
from flask import jsonify, request
from models.models import db, User, AccessToken
from datetime import datetime, timedelta
import requests
import pytz
import logging

manila_tz = pytz.timezone("Asia/Manila")

FACEBOOK_API_VERSION = "v23.0"
FACEBOOK_GRAPH_URL = f"https://graph.facebook.com/{FACEBOOK_API_VERSION}"

def create_access_token(user_id, access_token):
    try:
        print(f"[DEBUG] Received user_id: {user_id} ({type(user_id)})")
        print(f"[DEBUG] Received access_token: {access_token[:10]}...")

        user = User.query.filter_by(id=int(user_id)).first()

        if not user:
            print("[ERROR] User not found with ID:", user_id)
            return jsonify({'error': 'User not found'}), 404

        # Check if user is a superadmin
        if user.user_level != 1 or user.user_role != 'superadmin':
            return jsonify({'error': 'Only superadmins can create access tokens'}), 403

        # Check if token exists
        existing_token = AccessToken.query.filter_by(access_token=access_token).first()
        if existing_token:
            return jsonify({'error': 'Token already exists in database'}), 400

        # Fetch Facebook information using the token
        fb_info = fetch_facebook_info(access_token)

        if not fb_info:
            return jsonify({'error': 'Invalid Facebook access token or unable to fetch data from Facebook'}), 400

        # Get Facebook name from API
        facebook_name = fb_info.get('name')

        # Get expiration information
        is_expire = fb_info.get('is_expire', False)
        expiry_date = fb_info.get('expiring_at')

        if not expiry_date:
            # Default expiration time (60 days from now) if couldn't fetch
            expiry_date = datetime.now(manila_tz) + timedelta(days=60)

        new_token = AccessToken(
            user_id=user.id,
            access_token=access_token,
            facebook_name=facebook_name,
            is_expire=is_expire,
            expiring_at=expiry_date
        )

        db.session.add(new_token)
        db.session.commit()

        return jsonify({
            'message': 'Access token added successfully',
            'data': {
                'id': new_token.id,
                'user_id': user.id,
                'facebook_name': new_token.facebook_name,
                'is_expire': new_token.is_expire,
                'expiring_at': new_token.expiring_at.isoformat()
            }
        }), 201

    except Exception as e:
        print("[EXCEPTION]", str(e))
        return jsonify({'error': 'Internal Server Error'}), 500

def get_access_tokens(user_id):
    # Convert user_id to integer (if it's passed as a string)
    try:
        user_id = int(user_id)
    except ValueError:
        return jsonify({'error': 'Invalid user_id format'}), 400

    # Find the user by user_id
    user = User.query.filter_by(id=user_id).first()

    if not user:
        return jsonify({'error': 'User not found'}), 404

    # If user is a superadmin (level 1), return their own tokens
    if user.user_level == 1 and user.user_role == 'superadmin':
        tokens = AccessToken.query.filter_by(user_id=user.id).all()
    else:
        # For non-superadmins, get tokens from their managing superadmin
        tokens = AccessToken.get_client_accessible_tokens(user.id)

    # Return empty array instead of error when no tokens found
    return jsonify({
        'message': 'Access tokens retrieved successfully',
        'data': [
            {
                'id': token.id,
                'user_id': token.user_id,
                'access_token': token.access_token,
                'facebook_name': token.facebook_name,
                'is_expire': token.is_expire,
                'expiring_at': token.expiring_at.isoformat(),
                'last_used_at': token.last_used_at.isoformat()
            } for token in tokens
        ]
    }), 200

def get_access_token(token_id):
    try:
        # Find the access token by ID
        token = AccessToken.query.filter_by(id=token_id).first()

        if not token:
            return jsonify({'error': 'Access token not found'}), 404

        return jsonify({
            'message': 'Access token retrieved successfully',
            'data': {
                'id': token.id,
                'user_id': token.user_id,
                'access_token': token.access_token,
                'facebook_name': token.facebook_name,
                'is_expire': token.is_expire,
                'expiring_at': token.expiring_at.isoformat(),
                'created_at': token.created_at.isoformat(),
                'last_used_at': token.last_used_at.isoformat()
            }
        }), 200

    except Exception as e:
        print("[EXCEPTION]", str(e))
        return jsonify({'error': 'Internal Server Error'}), 500

def update_access_token(token_id):
    try:
        data = request.get_json()

        # Find the access token by ID
        token = AccessToken.query.filter_by(id=token_id).first()

        if not token:
            return jsonify({'error': 'Access token not found'}), 404

        # Update the access token fields
        if 'access_token' in data:
            token.access_token = data['access_token']

        if 'facebook_name' in data:
            token.facebook_name = data['facebook_name']

        if 'is_expire' in data:
            token.is_expire = data['is_expire']

        if 'expiring_at' in data:
            # Convert string to datetime object
            try:
                expiring_at = datetime.fromisoformat(data['expiring_at'])
                token.expiring_at = expiring_at
            except ValueError:
                return jsonify({'error': 'Invalid expiring_at date format'}), 400

        # Update last_used_at timestamp
        token.last_used_at = datetime.now(manila_tz)

        db.session.commit()

        return jsonify({
            'message': 'Access token updated successfully',
            'data': {
                'id': token.id,
                'user_id': token.user_id,
                'facebook_name': token.facebook_name,
                'is_expire': token.is_expire,
                'expiring_at': token.expiring_at.isoformat(),
                'last_used_at': token.last_used_at.isoformat()
            }
        }), 200

    except Exception as e:
        print("[EXCEPTION]", str(e))
        return jsonify({'error': 'Internal Server Error'}), 500

def delete_access_token(token_id, user_id):
    try:
        # Find the access token by ID and user_id
        token = AccessToken.query.filter_by(id=token_id, user_id=user_id).first()

        if not token:
            return jsonify({'error': 'Access token not found'}), 404

        # Delete the access token
        db.session.delete(token)
        db.session.commit()

        return jsonify({
            'message': 'Access token deleted successfully',
            'data': {
                'id': token.id,
                'user_id': token.user_id
            }
        }), 200

    except Exception as e:
        print("[EXCEPTION]", str(e))
        return jsonify({'error': 'Internal Server Error'}), 500


def fetch_facebook_info(access_token):
    """
    Fetch information from Facebook using the provided access token.
    Returns a dictionary with user information and token expiration details.
    """
    try:
        logger.info("Fetching Facebook user information")

        # Fetch user info
        user_info = fetch_facebook_user_info(access_token)
        if not user_info or 'id' not in user_info:
            logger.warning("Failed to fetch user info or invalid token")
            return None

        # Get user name
        facebook_name = user_info.get('name')

        # Fetch token debug info
        token_info = fetch_token_debug_info(access_token)

        # Default values
        is_expire = False
        expiring_at = None

        if token_info and 'data' in token_info:
            data = token_info['data']

            # Check if token is valid
            if 'is_valid' in data and not data['is_valid']:
                logger.warning("Token is invalid according to Facebook")
                is_expire = True

            # Get expiration timestamp
            if 'expires_at' in data:
                expiry_timestamp = data['expires_at']
                expiring_at = datetime.fromtimestamp(expiry_timestamp, manila_tz)

                # Set is_expire flag if token expires within 7 days
                seven_days_from_now = datetime.now(manila_tz) + timedelta(days=7)
                is_expire = expiring_at <= seven_days_from_now

        return {
            'id': user_info.get('id'),
            'name': facebook_name,
            'is_expire': is_expire,
            'expiring_at': expiring_at
        }

    except Exception as e:
        logger.error(f"Error fetching Facebook information: {str(e)}")
        return None

def fetch_facebook_user_info(access_token):
    """Fetch user information from Facebook Graph API"""
    try:
        url = f"{FACEBOOK_GRAPH_URL}/me"
        params = {
            'access_token': access_token,
            'fields': 'id,name'
        }

        response = requests.get(url, params=params)

        if response.status_code == 200:
            return response.json()
        else:
            logger.warning(f"Failed to fetch user info. Status code: {response.status_code}, Response: {response.text}")
            return None

    except Exception as e:
        logger.error(f"Error fetching Facebook user info: {str(e)}")
        return None

def fetch_token_debug_info(access_token):
    """
    Fetch detailed information about the access token.
    
    In a real implementation with app credentials, you would use:
    url = f"{GRAPH_API_BASE_URL}/debug_token"
    params = {
        'input_token': access_token,
        'access_token': f"{app_id}|{app_secret}"  # App access token
    }
    
    For now, this function simulates what you would get from the debug_token endpoint
    by making a simple API call to check validity and estimating expiration.
    """
    try:
        # Check if the token works by making a simple API call
        test_response = requests.get(f"{FACEBOOK_GRAPH_URL}/me", params={'access_token': access_token})

        if test_response.status_code == 200:
            # Token is valid - simulate token info
            # Setting expiry to 60 days from now (typical FB token validity)
            expiry_timestamp = int((datetime.now() + timedelta(days=60)).timestamp())

            return {
                'data': {
                    'app_id': '123456789',  # Simulated app ID
                    'type': 'USER',
                    'application': 'Marketing App',
                    'expires_at': expiry_timestamp,
                    'is_valid': True,
                    'scopes': ['email', 'public_profile', 'ads_management']
                }
            }
        else:
            # Token is invalid
            return {
                'data': {
                    'is_valid': False,
                    'error': {
                        'code': 190,
                        'message': 'Invalid OAuth access token.'
                    }
                }
            }

    except Exception as e:
        logger.error(f"Error fetching token debug info: {str(e)}")
        return None