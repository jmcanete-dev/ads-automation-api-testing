from flask import Blueprint, render_template, request, jsonify
import os
import redis
from flask_bcrypt import Bcrypt
from dotenv import load_dotenv
import uuid
from sqlalchemy.exc import SQLAlchemyError
from models.models import db, User
from workers.send_email import send_email_task  # Import shared task

# Load environment variables
load_dotenv()

# Initialize Flask extensions
bcrypt = Bcrypt()
password_reset_bp = Blueprint('password_reset', __name__)

# Redis client for token management
redis_client_password = redis.Redis(host='redis', port=6379, db=5, decode_responses=True)


# Generate a unique reset token (UUID)
def generate_reset_token():
    return str(uuid.uuid4())

# Verify the token by checking if it exists in Redis
def verify_reset_token(token):
    passtoken = redis_client_password.get(f"reset_token:{token}")
    if not passtoken:
        return None
    return passtoken

# Endpoint to request a password reset email
@password_reset_bp.route('/reset-password', methods=['POST'])
def reset_password():
    data = request.get_json()
    if not data or not data.get('email') or not data.get('domain'):
        return jsonify({'message': 'Email and domain are required'}), 400

    email = data['email']
    domain = data['domain']

    try:
        # Verify user existence
        user = db.session.query(User).filter_by(email=email, userdomain=domain).first()
        if not user:
            return jsonify({'message': 'User not found or domain mismatch'}), 404

        # Generate reset token and store in Redis (expires in 5 minutes)
        token = generate_reset_token()
        reset_link = f"http://{domain}/#/reset-password/{token}"
        redis_client_password.setex(f"reset_token:{token}", 300, email)  # Set TTL for 5 minutes

        # Prepare email content before passing to Celery
        html_content = render_template('reset-template.html', reset_link=reset_link, domain=domain, year=2025)

       # Call Celery shared task asynchronously
        send_email_task.apply_async(args=[email, "Password Reset Request", html_content])

        return jsonify({'message': 'Password reset email sent successfully'}), 200

    except SQLAlchemyError as e:
        return jsonify({'message': 'Database error', 'error': str(e)}), 500

# Endpoint to verify the reset token
@password_reset_bp.route('/reset-password/<token>', methods=['GET'])
def verify_reset_link(token):
    email = verify_reset_token(token)
    if email is None:
        return jsonify({'message': 'Invalid or expired reset token'}), 400

    return jsonify({'message': 'Token is valid. You can reset your password.'}), 200

# Endpoint to reset the user's password
@password_reset_bp.route('/new-password/<token>', methods=['POST'])
def reset_user_password(token):
    email = verify_reset_token(token)
    if email is None:
        return jsonify({'message': 'Invalid or expired reset token'}), 400

    data = request.get_json()
    new_password = data.get('new_password')

    if not new_password:
        return jsonify({'message': 'New password is required'}), 400

    if len(new_password) < 8:
        return jsonify({'message': 'Password must be at least 8 characters long'}), 400

    try:
        # Hash and update the user's password
        hashed_password = bcrypt.generate_password_hash(new_password).decode('utf-8')
        user = db.session.query(User).filter_by(email=email).first()
        if user:
            user.password = hashed_password
            db.session.commit()

        # Delete the token from Redis after successful reset
        redis_client_password.delete(f"reset_token:{token}")

        return jsonify({'message': 'Password reset successfully'}), 200

    except SQLAlchemyError as e:
        return jsonify({'message': 'Database error', 'error': str(e)}), 500
