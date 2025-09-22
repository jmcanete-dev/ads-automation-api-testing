from flask import jsonify
from models.models import db, User, InviteCode, UserRelationship
import random
import string
from datetime import datetime, timedelta
import pytz

manila_tz = pytz.timezone("Asia/Manila")

def generate_invite_code(superadmin_id):
    try:
        # Verify superadmin
        superadmin = User.query.get(superadmin_id)
        if not superadmin:
            return jsonify({'error': 'Superadmin not found'}), 404
        
        # Check if user is level 1 and has superadmin role
        if superadmin.user_level != 1 or superadmin.user_role != 'superadmin':
            print(f"[ERROR] User {superadmin_id} is level {superadmin.user_level} ({superadmin.user_role}) - only level 1 superadmins can generate codes")
            return jsonify({
                'error': 'Only level 1 superadmins can generate invite codes',
                'details': {
                    'user_level': superadmin.user_level,
                    'user_role': superadmin.user_role,
                    'expected_level': 1,
                    'expected_role': 'superadmin'
                }
            }), 403

        # Generate a unique 8-character code
        while True:
            code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            if not InviteCode.query.filter_by(invite_code=code).first():
                break

        # Check for existing expired code
        expired_code = InviteCode.query.filter(
            InviteCode.superadmin_id == superadmin_id,
            InviteCode.is_used == False,
            InviteCode.expires_at < datetime.now(manila_tz)
        ).first()
        
        if expired_code:
            # Update the expired code with new values
            expired_code.invite_code = code
            expired_code.created_at = datetime.now(manila_tz)
            expired_code.expires_at = datetime.now(manila_tz) + timedelta(days=7)
            db.session.commit()
        else:
            # Create new invite code
            invite = InviteCode(
                superadmin_id=superadmin_id,
                invite_code=code
            )
            db.session.add(invite)
            db.session.commit()

        return jsonify({
            'message': 'Invite code generated successfully',
            'data': {
                'invite_code': code,
                'expires_at': (datetime.now(manila_tz) + timedelta(days=7)).isoformat()
            }
        }), 201

    except Exception as e:
        print("[EXCEPTION]", str(e))
        return jsonify({'error': 'Internal Server Error'}), 500

def use_invite_code(invite_code, user_id):
    try:
        print(f"[DEBUG] Attempting to use invite code: {invite_code} by user: {user_id}")
        
        # Verify user exists
        user = User.query.get(user_id)
        if not user:
            print(f"[ERROR] User with ID {user_id} not found")
            return jsonify({'error': 'User not found'}), 404

        print(f"[DEBUG] User found - Level: {user.user_level}, Role: {user.user_role}")

        # Check user level and role based on the mapping
        if user.user_level == 1 or user.user_level == 2:  # superadmin or admin
            print(f"[ERROR] User {user_id} is level {user.user_level} ({user.user_role}) - cannot use invite code")
            return jsonify({
                'error': 'Superadmins and admins cannot use invite codes',
                'details': {
                    'user_level': user.user_level,
                    'user_role': user.user_role
                }
            }), 403

        # Verify user has correct role for their level
        if user.user_level == 3 and user.user_role != 'staff':
            print(f"[ERROR] User {user_id} has incorrect role mapping - Level 3 should be staff")
            return jsonify({
                'error': 'Invalid role mapping',
                'details': {
                    'user_level': user.user_level,
                    'user_role': user.user_role,
                    'expected_role': 'staff'
                }
            }), 400

        if user.user_level == 4 and user.user_role != 'client':
            print(f"[ERROR] User {user_id} has incorrect role mapping - Level 4 should be client")
            return jsonify({
                'error': 'Invalid role mapping',
                'details': {
                    'user_level': user.user_level,
                    'user_role': user.user_role,
                    'expected_role': 'client'
                }
            }), 400

        # Check if user already has a relationship
        existing_relationship = UserRelationship.query.filter_by(client_id=user_id).first()
        if existing_relationship:
            print(f"[ERROR] User {user_id} already has a relationship with superadmin {existing_relationship.superadmin_id}")
            return jsonify({
                'error': 'User already has a relationship with a superadmin',
                'details': {
                    'superadmin_id': existing_relationship.superadmin_id
                }
            }), 400

        # Find and validate invite code
        invite = InviteCode.query.filter_by(invite_code=invite_code).first()
        if not invite:
            print(f"[ERROR] Invite code {invite_code} not found")
            return jsonify({'error': 'Invalid invite code'}), 404
            
        if invite.is_used:
            print(f"[ERROR] Invite code {invite_code} already used by user {invite.used_by}")
            return jsonify({
                'error': 'Invite code has already been used',
                'details': {
                    'used_by': invite.used_by,
                    'used_at': invite.used_at.isoformat() if invite.used_at else None
                }
            }), 400
            
        if datetime.now(manila_tz) > invite.expires_at:
            print(f"[ERROR] Invite code {invite_code} expired at {invite.expires_at}")
            return jsonify({
                'error': 'Invite code has expired',
                'details': {
                    'expired_at': invite.expires_at.isoformat()
                }
            }), 400

        print(f"[DEBUG] Creating relationship between superadmin {invite.superadmin_id} and user {user_id}")
        print(f"[DEBUG] Relationship Details:")
        print(f"  - Superadmin ID: {invite.superadmin_id}")
        print(f"  - User ID: {user_id}")
        print(f"  - User Level: {user.user_level}")
        print(f"  - User Role: {user.user_role}")
        print(f"  - Invite Code: {invite_code}")

        # Create relationship
        relationship = UserRelationship(
            superadmin_id=invite.superadmin_id,
            client_id=user_id
        )

        # Mark invite as used
        invite.is_used = True
        invite.used_by = user_id
        invite.used_at = datetime.now(manila_tz)

        db.session.add(relationship)
        db.session.commit()

        print(f"[DEBUG] Successfully created relationship and marked invite code as used")
        print(f"[DEBUG] Final Relationship Details:")
        print(f"  - Relationship ID: {relationship.id}")
        print(f"  - Created At: {relationship.created_at}")
        print(f"  - Superadmin ID: {relationship.superadmin_id}")
        print(f"  - User ID: {relationship.client_id}")

        return jsonify({
            'message': 'Invite code used successfully',
            'data': {
                'superadmin_id': invite.superadmin_id,
                'user_id': user_id,
                'user_level': user.user_level,
                'user_role': user.user_role,
                'relationship_created_at': relationship.created_at.isoformat()
            }
        }), 200

    except Exception as e:
        print(f"[EXCEPTION] Error in use_invite_code: {str(e)}")
        db.session.rollback()  # Rollback any failed transaction
        return jsonify({
            'error': 'Internal Server Error',
            'details': str(e)
        }), 500

def get_invite_codes(superadmin_id):
    try:
        # Verify superadmin
        superadmin = User.query.get(superadmin_id)
        if not superadmin:
            return jsonify({'error': 'Superadmin not found'}), 404
        if superadmin.user_level != 1 or superadmin.user_role != 'superadmin':
            return jsonify({'error': 'Only superadmins can view invite codes'}), 403

        # Get all invite codes for this superadmin
        invites = InviteCode.query.filter_by(superadmin_id=superadmin_id).all()

        return jsonify({
            'message': 'Invite codes retrieved successfully',
            'data': [
                {
                    'invite_code': invite.invite_code,
                    'is_used': invite.is_used,
                    'used_by': invite.used_by,
                    'created_at': invite.created_at.isoformat(),
                    'expires_at': invite.expires_at.isoformat(),
                    'used_at': invite.used_at.isoformat() if invite.used_at else None
                } for invite in invites
            ]
        }), 200

    except Exception as e:
        print("[EXCEPTION]", str(e))
        return jsonify({'error': 'Internal Server Error'}), 500

def regenerate_expired_code(superadmin_id, old_code):
    try:
        # Verify superadmin
        superadmin = User.query.get(superadmin_id)
        if not superadmin:
            return jsonify({'error': 'Superadmin not found'}), 404
        if superadmin.user_level != 1 or superadmin.user_role != 'superadmin':
            return jsonify({'error': 'Only superadmins can regenerate invite codes'}), 403

        # Find the old code
        old_invite = InviteCode.query.filter_by(
            superadmin_id=superadmin_id,
            invite_code=old_code
        ).first()

        if not old_invite:
            return jsonify({'error': 'Invite code not found'}), 404

        # Check if code is expired
        if datetime.now(manila_tz) <= old_invite.expires_at:
            return jsonify({'error': 'Cannot regenerate non-expired code'}), 400

        # Generate new code
        while True:
            new_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            if not InviteCode.query.filter_by(invite_code=new_code).first():
                break

        # Update the old code with new values
        old_invite.invite_code = new_code
        old_invite.is_used = False
        old_invite.used_by = None
        old_invite.used_at = None
        old_invite.created_at = datetime.now(manila_tz)
        old_invite.expires_at = datetime.now(manila_tz) + timedelta(days=7)

        db.session.commit()

        return jsonify({
            'message': 'Invite code regenerated successfully',
            'data': {
                'old_code': old_code,
                'new_code': new_code,
                'expires_at': old_invite.expires_at.isoformat()
            }
        }), 200

    except Exception as e:
        print("[EXCEPTION]", str(e))
        return jsonify({'error': 'Internal Server Error'}), 500

def verify_invite_code(invite_code):
    try:
        print(f"[DEBUG] Verifying invite code: {invite_code}")
        
        # Find the invite code
        invite = InviteCode.query.filter_by(invite_code=invite_code).first()
        if not invite:
            print(f"[ERROR] Invite code {invite_code} not found")
            return jsonify({
                'valid': False,
                'message': 'Invalid invite code'
            }), 404
            
        if invite.is_used:
            print(f"[ERROR] Invite code {invite_code} already used by user {invite.used_by}")
            return jsonify({
                'valid': False,
                'message': 'Invite code has already been used',
                'details': {
                    'used_by': invite.used_by,
                    'used_at': invite.used_at.isoformat() if invite.used_at else None
                }
            }), 400
            
        if datetime.now(manila_tz) > invite.expires_at:
            print(f"[ERROR] Invite code {invite_code} expired at {invite.expires_at}")
            return jsonify({
                'valid': False,
                'message': 'Invite code has expired',
                'details': {
                    'expired_at': invite.expires_at.isoformat()
                }
            }), 400

        # If we get here, the code is valid
        print(f"[DEBUG] Invite code {invite_code} is valid")
        return jsonify({
            'valid': True,
            'message': 'Invite code is valid',
            'data': {
                'superadmin_id': invite.superadmin_id,
                'created_at': invite.created_at.isoformat(),
                'expires_at': invite.expires_at.isoformat()
            }
        }), 200

    except Exception as e:
        print(f"[EXCEPTION] Error in verify_invite_code: {str(e)}")
        return jsonify({
            'valid': False,
            'message': 'Internal Server Error',
            'details': str(e)
        }), 500

def use_invite_code_during_signup(invite_code, user_data):
    try:
        print(f"[DEBUG] Attempting to use invite code during signup: {invite_code}")
        
        # Find and validate invite code
        invite = InviteCode.query.filter_by(invite_code=invite_code).first()
        if not invite:
            print(f"[ERROR] Invite code {invite_code} not found")
            return jsonify({'error': 'Invalid invite code'}), 404
            
        if invite.is_used:
            print(f"[ERROR] Invite code {invite_code} already used by user {invite.used_by}")
            return jsonify({
                'error': 'Invite code has already been used',
                'details': {
                    'used_by': invite.used_by,
                    'used_at': invite.used_at.isoformat() if invite.used_at else None
                }
            }), 400
            
        if datetime.now(manila_tz) > invite.expires_at:
            print(f"[ERROR] Invite code {invite_code} expired at {invite.expires_at}")
            return jsonify({
                'error': 'Invite code has expired',
                'details': {
                    'expired_at': invite.expires_at.isoformat()
                }
            }), 400

        # Create the user
        new_user = User(
            username=user_data['username'],
            full_name=user_data['full_name'],
            email=user_data['email'],
            password=user_data['password'],  # Make sure this is hashed in your actual implementation
            gender=user_data['gender'],
            user_level=user_data['user_level'],
            user_role=user_data['user_role']
        )
        db.session.add(new_user)
        db.session.flush()  # This will get us the new user's ID without committing

        print(f"[DEBUG] Creating relationship between superadmin {invite.superadmin_id} and new user {new_user.id}")
        
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
        db.session.commit()

        print(f"[DEBUG] Successfully created user, relationship and marked invite code as used")
        
        return jsonify({
            'message': 'User registered and invite code used successfully',
            'data': {
                'user_id': new_user.id,
                'superadmin_id': invite.superadmin_id,
                'user_level': new_user.user_level,
                'user_role': new_user.user_role,
                'relationship_created_at': relationship.created_at.isoformat()
            }
        }), 201

    except Exception as e:
        print(f"[EXCEPTION] Error in use_invite_code_during_signup: {str(e)}")
        db.session.rollback()  # Rollback any failed transaction
        return jsonify({
            'error': 'Internal Server Error',
            'details': str(e)
        }), 500 