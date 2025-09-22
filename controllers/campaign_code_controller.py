from flask import jsonify, request
from models.models import db, User, CampaignCode

def create_campaign_code(user_id, campaign_code):
    try:
        print(f"[DEBUG] Received user_id: {user_id} ({type(user_id)})")
        print(f"[DEBUG] Received campaign_code: {campaign_code}")

        user = User.query.filter_by(id=int(user_id)).first()

        if not user:
            print("[ERROR] User not found with ID:", user_id)
            return jsonify({'error': 'User not found'}), 404

        new_code = CampaignCode(
            user_id=user.id,
            campaign_code=campaign_code
        )

        db.session.add(new_code)
        db.session.commit()

        return jsonify({
            'message': 'Campaign code added successfully',
            'data': {
                'id': new_code.id,
                'user_id': user.user_id,
                'campaign_code': new_code.campaign_code
            }
        }), 201

    except Exception as e:
        print("[EXCEPTION]", str(e))
        return jsonify({'error': 'Internal Server Error'}), 500

def get_campaign_code(user_id):
    # Convert user_id to integer (if it's passed as a string)
    try:
        user_id = int(user_id)
    except ValueError:
        return jsonify({'error': 'Invalid user_id format'}), 400

    # Find the user by user_id
    user = User.query.filter_by(id=user_id).first()

    if not user:
        return jsonify({'error': 'User not found'}), 404

    # Query the CampaignCode table for the given user_id
    campaign_codes = CampaignCode.query.filter_by(user_id=user.id).all()

    if not campaign_codes:
        return jsonify({'error': 'Campaign code not found for this user'}), 404

    # Return the list of campaign codes
    return jsonify({
        'message': 'Campaign codes retrieved successfully',
        'data': [
            {
                'id': code.id,
                'user_id': user.user_id,
                'campaign_code': code.campaign_code
            } for code in campaign_codes
        ]
    }), 200

def update_campaign_code(code_id):
    try:
        data = request.get_json()
        user_id = data.get("user_id")
        new_campaign_code = data.get("campaign_code")

        if not user_id or not new_campaign_code:
            return jsonify({'error': 'user_id and campaign_code are required'}), 400

        # Find the campaign code by ID and user_id
        campaign_code = CampaignCode.query.filter_by(id=code_id, user_id=user_id).first()

        if not campaign_code:
            return jsonify({'error': 'Campaign code not found'}), 404

        # Update the campaign code
        campaign_code.campaign_code = new_campaign_code
        db.session.commit()

        return jsonify({
            'message': 'Campaign code updated successfully',
            'data': {
                'id': campaign_code.id,
                'user_id': campaign_code.user_id,
                'campaign_code': campaign_code.campaign_code
            }
        }), 200

    except Exception as e:
        print("[EXCEPTION]", str(e))
        return jsonify({'error': 'Internal Server Error'}), 500
    
def delete_campaign_code(code_id, user_id):
    try:
        # Find the campaign code by ID and user_id
        campaign_code = CampaignCode.query.filter_by(id=code_id, user_id=user_id).first()

        if not campaign_code:
            return jsonify({'error': 'Campaign code not found'}), 404

        # Delete the campaign code
        db.session.delete(campaign_code)
        db.session.commit()

        return jsonify({
            'message': 'Campaign code deleted successfully',
            'data': {
                'id': campaign_code.id,
                'user_id': campaign_code.user_id,
                'campaign_code': campaign_code.campaign_code
            }
        }), 200

    except Exception as e:
        print("[EXCEPTION]", str(e))
        return jsonify({'error': 'Internal Server Error'}), 500