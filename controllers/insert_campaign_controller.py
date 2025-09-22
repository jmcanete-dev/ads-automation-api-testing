import logging
import requests
from sqlalchemy.exc import SQLAlchemyError
from models.models import db, Campaign
from datetime import datetime
import pytz

manila_tz = pytz.timezone("Asia/Manila")
current_time_manila = datetime.now(manila_tz)

def upsert_campaign_data(user_id, ad_account_id, campaign_id, last_server_messages=None, adsets_ads_creatives=None, status=None):
    """
    Update or insert campaign data for a specific user_id, ad_account_id, and campaign_id.

    Args:
        user_id (int): ID of the user.
        ad_account_id (str): Facebook Ad account ID.
        campaign_id (str): Campaign ID.
        last_server_messages (str, optional): Latest server messages.
        adsets_ads_creatives (dict, optional): Details of adsets, ads, and creatives.
        status (str, optional): Status of the campaign.
    
    Returns:
        dict: Result of the operation with status and message.
    """
    try:
        # Find the campaign based on user_id, ad_account_id, and campaign_id
        campaign = db.session.query(Campaign).filter_by(
            user_id=user_id,
            ad_account_id=ad_account_id,
            campaign_id=campaign_id
        ).first()

        if not campaign:
            return {
                "status": "failed",
                "message": f"Campaign with ID {campaign_id} not found for user_id {user_id} and ad_account_id {ad_account_id}."
            }

        # Update fields if provided
        if last_server_messages is not None:
            campaign.last_server_message = last_server_messages

        if adsets_ads_creatives is not None:
            campaign.adsets_ads_creatives = adsets_ads_creatives

        if status is not None:
            campaign.status = status

        # Update the modified timestamp
        campaign.updated_at = current_time_manila

        # Commit changes
        db.session.commit()

        return {
            "status": "success",
            "message": f"Campaign with ID {campaign_id} updated successfully."
        }

    except SQLAlchemyError as e:
        db.session.rollback()
        logging.error(f"Database error while updating campaign: {str(e)}")
        return {
            "status": "failed",
            "message": f"Database error: {str(e)}"
        }
    except Exception as e:
        logging.error(f"Error during campaign upsert operation: {str(e)}")
        return {
            "status": "failed",
            "message": f"An unexpected error occurred: {str(e)}"
        }
