from flask import request, jsonify
from flask_jwt_extended import create_access_token
from datetime import timedelta, datetime
from flask_bcrypt import Bcrypt
import base64
import random
import pytz
import redis
from werkzeug.utils import secure_filename
from PIL import Image
from models.models import User, db, InviteCode, UserRelationship


bcrypt = Bcrypt()
redis_db = redis.Redis(host='redis', port=6379, db=1, decode_responses=True)


def register():
    data = request.get_json()

    # Validate input fields
    required_fields = ['username', 'password', 'email', 'gender', 'domain', 'full_name']
    if not all(data.get(field) for field in required_fields):
        return jsonify({'message': f'{", ".join(required_fields)} are required'}), 400

    # Gender validation
    gender = data['gender'].lower()
    if gender not in ['male', 'female']:
        return jsonify({'message': 'Gender must be either male or female'}), 400

    # Default image path based on gender
    image_path = 'assets/male.png' if gender == 'male' else 'assets/female.png'

    # Check if the username or email already exists
    if User.query.filter((User.username == data['username']) | (User.email == data['email'])).first():
        return jsonify({'message': 'Username or email already exists'}), 400

    # Hash the password
    hashed_password = bcrypt.generate_password_hash(data['password']).decode('utf-8')

    # Generate a unique 10-digit user_id
    user_id = str(random.randint(1000000000, 9999999999))
    while User.query.filter_by(user_id=user_id).first():
        user_id = str(random.randint(1000000000, 9999999999))

    # Read the profile image as binary data
    with open(image_path, 'rb') as image_file:
        image_data = image_file.read()

    manila_tz = pytz.timezone("Asia/Manila")
    current_time_manila = datetime.now(manila_tz)

    # Get user_level and user_role from data or set defaults
    user_level = data.get('user_level', 3)
    user_role = data.get('user_role', 'staff')

    # Start a transaction
    try:
        # Create a new user instance
        new_user = User(
            user_id=user_id,
            username=data['username'],
            email=data['email'],
            password=hashed_password,
            gender=gender,
            userdomain=data['domain'],
            profile_image=image_data,
            full_name=data['full_name'],
            user_status='active',
            created_at=current_time_manila,
            user_level=user_level,
            user_role=user_role
        )

        # Save the user to the database
        db.session.add(new_user)
        db.session.flush()  # This will get us the new user's ID without committing

        # If invite code is provided, validate and use it
        invite_code = data.get('invite_code')
        if invite_code:
            # Find and validate invite code
            invite = InviteCode.query.filter_by(invite_code=invite_code).first()
            if not invite:
                db.session.rollback()
                return jsonify({'message': 'Invalid invite code'}), 400
                
            if invite.is_used:
                db.session.rollback()
                return jsonify({
                    'message': 'Invite code has already been used',
                    'details': {
                        'used_by': invite.used_by,
                        'used_at': invite.used_at.isoformat() if invite.used_at else None
                    }
                }), 400
                
            if datetime.now(manila_tz) > invite.expires_at:
                db.session.rollback()
                return jsonify({
                    'message': 'Invite code has expired',
                    'details': {
                        'expired_at': invite.expires_at.isoformat()
                    }
                }), 400

            # Create relationship
            relationship = UserRelationship(
                superadmin_id=invite.superadmin_id,
                client_id=new_user.id
            )

            # Mark invite as used
            invite.is_used = True
            invite.used_by = new_user.id
            invite.used_at = datetime.now(manila_tz)

            db.session.add(relationship)

        # Commit all changes
        db.session.commit()

        return jsonify({
            'message': 'User registered successfully',
            'user_id': user_id,
            'invite_code_used': bool(invite_code)
        }), 201

    except Exception as e:
        db.session.rollback()
        print("[EXCEPTION]", str(e))
        return jsonify({'message': 'Internal Server Error'}), 500


def login():
    data = request.get_json()

    if not data or (not data.get('username') and not data.get('email')) or not data.get('password') or not data.get('domain'):
        return jsonify({'message': 'Username/email, password, and domain are required'}), 400

    username_or_email = data.get('username') or data.get('email')
    domain = data.get('domain')

    # Query user by username or email
    user = User.query.filter(
        (User.username == username_or_email) | (User.email == username_or_email)
    ).first()

    if not user:
        return jsonify({'message': 'Invalid username/email'}), 401

    # Validate password
    if not bcrypt.check_password_hash(user.password, data['password']):
        return jsonify({'message': 'Password does not match'}), 401

    # Check domain
    if user.userdomain != domain:
        return jsonify({'message': 'Your account cannot be found in this domain. Please log in to your dedicated domain.'}), 403

    # Check user status
    if user.user_status == 'pending':
        return jsonify({'message': 'Your account is still waiting for activation.'}), 403
    elif user.user_status == 'inactive':
        return jsonify({'message': 'Your account is deactivated. Proceed with payment to continue using the service.'}), 403
    elif user.user_status == 'banned':
        return jsonify({'message': 'Your account is banned.'}), 403

    # Generate JWT token
    access_token = create_access_token(identity=str(user.id), expires_delta=timedelta(days=2))

    redis_key = f"{user.id}-access-key"
    keys = redis_db.keys(f"*{redis_key}*")
    if keys:
        print(f"Redis key(s) found for user ID '{user.id}'")
    else:
        print(f"No Redis key(s) found for user ID '{user.id}'")
    
    manila_tz = pytz.timezone("Asia/Manila")
    current_time_manila = datetime.now(manila_tz)
    user.last_active = current_time_manila
    db.session.commit()

    existing_redis_key = keys
    if existing_redis_key:
        return jsonify({
            'message': 'Login successful',
            'user_id': user.user_id,
            'id': user.id,
            'access_token': access_token,
            'redis_key': redis_key
        }), 200
    
    else:
        redis_value = user.username
        redis_db.setex(redis_key, timedelta(days=7), redis_value)

        # Update the user's last_active timestamp
        manila_tz = pytz.timezone("Asia/Manila")
        user.last_active = current_time_manila
        db.session.commit()

        return jsonify({
            'message': 'Login successful',
            'user_id': user.id,
            'access_token': access_token,
            'redis_key': redis_key
        }), 200


import base64
from flask import request, jsonify
from models.models import User
from datetime import datetime

def get_user_data_by_id():
    user_id = request.args.get('user_id')

    if not user_id:
        return jsonify({'message': 'User ID required'}), 400

    user = User.query.get(user_id)

    if user:
        # Convert profile_image (binary) to Base64
        profile_image_base64 = base64.b64encode(user.profile_image).decode('utf-8') if user.profile_image else None

        # Ensure last_active is formatted in +08:00 timezone
        last_active = user.last_active.strftime('%Y-%m-%d %H:%M:%S.%f') if user.last_active else None

        user_data = {
            'id': user.id,
            'username': user.username,
            'email': user.email,
            'gender': user.gender,
            'last_active': last_active,  # Already in local time (+08:00)
            'status': user.user_status,
            'profile_image': profile_image_base64,
            'user_level': user.user_level,
            'user_role': user.user_role
        }

        return jsonify({
            'message': 'User authenticated successfully',
            'user_data': user_data
        }), 200

    return jsonify({'message': 'User not found'}), 404
