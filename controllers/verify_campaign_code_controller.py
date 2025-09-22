# controllers/verify_campaign_code_controller.py
from flask import request, jsonify
from models.models import CampaignCode, db

def validate_campaign_code():
    data = request.get_json()
    user_id = data.get("user_id")
    campaign_codes = data.get("campaign_codes", [])

    if not user_id or not campaign_codes:
        return jsonify({"error": "Missing user_id or campaign_codes"}), 400

    existing_codes = (
        db.session.query(CampaignCode.campaign_code)
        .filter(CampaignCode.user_id == user_id)
        .filter(CampaignCode.campaign_code.in_(campaign_codes))
        .all()
    )

    existing_codes = [code[0] for code in existing_codes]
    missing_codes = list(set(campaign_codes) - set(existing_codes))

    return jsonify({
        "existing_codes": existing_codes,
        "missing_codes": missing_codes
    })
