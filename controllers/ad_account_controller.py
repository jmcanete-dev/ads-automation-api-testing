from venv import logger
from flask import jsonify, request
from models.models import db, User, AdAccount
from datetime import datetime, timedelta
import requests
import pytz
import logging

manila_tz = pytz.timezone("Asia/Manila")

FACEBOOK_API_VERSION = "v23.0"
FACEBOOK_GRAPH_URL = f"https://graph.facebook.com/{FACEBOOK_API_VERSION}"

def fetch_ad_accounts():
    """
    Fetch ad accounts from Facebook Graph API using the me/adaccounts endpoint
    Requires user_id and access_token
    """
    try:
        # Try to get data from JSON body first
        user_id = request.json.get('user_id') if request.is_json else request.args.get('user_id')
        access_token = None
        
        # Check for token in header
        auth_header = request.headers.get('Authorization')
        if auth_header and auth_header.startswith('Bearer '):
            access_token = auth_header[7:]
        
        # Fallback to query params or JSON body
        if not access_token:
            access_token = request.args.get('access_token') or (request.json.get('access_token') if request.is_json else None)

        # Validate required params
        if not user_id:
            logger.error("No user_id provided")
            return jsonify({'error': 'user_id is required', 'status': 'error'}), 400
        
        if not access_token:
            logger.error("No access token provided")
            return jsonify({'error': 'access_token is required', 'status': 'error'}), 401
        
        # Construct the API endpoint URL
        endpoint_url = f"{FACEBOOK_GRAPH_URL}/me/adaccounts"
        
        params = {
            'fields': 'name',
            'limit': 1000,
            'access_token': access_token
        }
        
        logger.info(f"[User {user_id}] Fetching ad accounts from: {endpoint_url}")
        
        response = requests.get(endpoint_url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            ad_accounts = data.get('data', [])
            
            saved_count = save_ad_accounts_to_db(ad_accounts, user_id)
            
            return jsonify({
                'status': 'success',
                'user_id': user_id,
                'count': len(ad_accounts),
                'saved_to_db': saved_count,
                'data': ad_accounts,
                'timestamp': datetime.now(manila_tz).isoformat()
            }), 200
        
        else:
            error_data = response.json() if response.content else {}
            return jsonify({
                'error': 'Failed to fetch ad accounts from Facebook API',
                'status': 'error',
                'facebook_error': error_data,
                'status_code': response.status_code
            }), response.status_code
    
    except Exception as e:
        logger.error(f"Unexpected error in fetch_ad_accounts: {str(e)}")
        return jsonify({
            'error': 'An unexpected error occurred',
            'status': 'error',
            'details': str(e)
        }), 500

def save_ad_accounts_to_db(ad_accounts_data, user_id):
    saved_count = 0
    
    try:
        for account_data in ad_accounts_data:
            account_id = account_data.get('account_id') or account_data.get('id')
            account_name = account_data.get('name')
            
            if not account_id:
                continue
            
            existing_account = AdAccount.query.filter_by(account_id=account_id, user_id=user_id).first()
            
            if existing_account:
                existing_account.name = account_name
                existing_account.updated_at = datetime.now(manila_tz)
            else:
                new_account = AdAccount(
                    account_id=account_id,
                    name=account_name,
                    user_id=user_id,
                    created_at=datetime.now(manila_tz),
                    updated_at=datetime.now(manila_tz)
                )
                db.session.add(new_account)
            
            saved_count += 1
        
        db.session.commit()
    
    except Exception as e:
        db.session.rollback()
        raise e
    
    return saved_count

def refetch_ad_accounts():
    """
    Refetch ad accounts from Facebook Graph API and replace all existing records
    This will delete all existing ad accounts and save the newly fetched ones
    """
    try:
        # Get access token from request headers or query parameters
        access_token = request.headers.get('Authorization')
        if access_token and access_token.startswith('Bearer '):
            access_token = access_token[7:]  # Remove 'Bearer ' prefix
        else:
            access_token = request.args.get('access_token')
        
        if not access_token:
            logger.error("No access token provided")
            return jsonify({
                'error': 'Access token is required',
                'status': 'error'
            }), 401
        
        # Construct the API endpoint URL
        endpoint_url = f"{FACEBOOK_GRAPH_URL}/me/adaccounts"
        
        # Parameters for the API call
        params = {
            'fields': 'name',
            'limit': 1000,
            'access_token': access_token
        }
        
        logger.info(f"Refetching ad accounts from: {endpoint_url}")
        
        # Make the API request
        response = requests.get(endpoint_url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            ad_accounts = data.get('data', [])
            
            logger.info(f"Successfully fetched {len(ad_accounts)} ad accounts for refetch")
            
            # Delete all existing ad accounts and save new ones
            deleted_count, saved_count = replace_ad_accounts_in_db(ad_accounts)
            
            return jsonify({
                'status': 'success',
                'message': 'Ad accounts refetched successfully',
                'deleted_count': deleted_count,
                'saved_count': saved_count,
                'data': ad_accounts,
                'timestamp': datetime.now(manila_tz).isoformat()
            }), 200
            
        else:
            error_data = response.json() if response.content else {}
            logger.error(f"Facebook API error during refetch: {response.status_code} - {error_data}")
            
            return jsonify({
                'error': 'Failed to refetch ad accounts from Facebook API',
                'status': 'error',
                'facebook_error': error_data,
                'status_code': response.status_code
            }), response.status_code
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Request exception during refetch: {str(e)}")
        return jsonify({
            'error': 'Network error occurred while refetching ad accounts',
            'status': 'error',
            'details': str(e)
        }), 500
        
    except Exception as e:
        logger.error(f"Unexpected error in refetch_ad_accounts: {str(e)}")
        return jsonify({
            'error': 'An unexpected error occurred during refetch',
            'status': 'error',
            'details': str(e)
        }), 500

def replace_ad_accounts_in_db(ad_accounts_data):
    """
    Replace all ad accounts in the database with new data
    First deletes all existing records, then saves the new ones
    Returns tuple of (deleted_count, saved_count)
    """
    deleted_count = 0
    saved_count = 0
    
    try:
        # Delete all existing ad accounts
        existing_accounts = AdAccount.query.all()
        deleted_count = len(existing_accounts)
        
        if deleted_count > 0:
            AdAccount.query.delete()
            logger.info(f"Deleted {deleted_count} existing ad accounts")
        
        # Save new ad accounts
        for account_data in ad_accounts_data:
            account_id = account_data.get('id')
            account_name = account_data.get('name')
            
            if not account_id:
                continue
            
            # Create new ad account
            new_account = AdAccount(
                account_id=account_id,
                name=account_name,
                created_at=datetime.now(manila_tz),
                updated_at=datetime.now(manila_tz)
            )
            db.session.add(new_account)
            saved_count += 1
            logger.info(f"Added new ad account: {account_id} - {account_name}")
        
        db.session.commit()
        logger.info(f"Successfully replaced ad accounts: deleted {deleted_count}, saved {saved_count}")
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error replacing ad accounts in database: {str(e)}")
        raise e
    
    return deleted_count, saved_count

def get_ad_accounts_from_db():
    """
    Retrieve ad accounts from the local database
    Returns JSON response with stored ad accounts
    """
    try:
        ad_accounts = AdAccount.query.all()
        
        accounts_data = []
        for account in ad_accounts:
            accounts_data.append({
                'id': account.account_id,
                'name': account.name,
                'created_at': account.created_at.isoformat() if account.created_at else None,
                'updated_at': account.updated_at.isoformat() if account.updated_at else None
            })
        
        logger.info(f"Retrieved {len(accounts_data)} ad accounts from database")
        
        return jsonify({
            'status': 'success',
            'count': len(accounts_data),
            'data': accounts_data,
            'timestamp': datetime.now(manila_tz).isoformat()
        }), 200
        
    except Exception as e:
        logger.error(f"Error retrieving ad accounts from database: {str(e)}")
        return jsonify({
            'error': 'Failed to retrieve ad accounts from database',
            'status': 'error',
            'details': str(e)
        }), 500