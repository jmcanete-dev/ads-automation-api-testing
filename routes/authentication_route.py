from flask import Blueprint, request, jsonify
from flask_jwt_extended import create_access_token
from datetime import timedelta, datetime
from controllers.authentication_controller import register, login, get_user_data_by_id

# Create Blueprint
auth_bp = Blueprint('auth', __name__)

# Route to call register function
@auth_bp.route('/register', methods=['POST'])
def register_user():
    return register()

# Route to call login function
@auth_bp.route('/login', methods=['POST'])
def login_user():
    return login()

@auth_bp.route('/get-user-data', methods=['GET'])
def get_user_data():
    return get_user_data_by_id()

