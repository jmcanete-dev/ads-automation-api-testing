from celery import shared_task
from datetime import datetime, timedelta
from pytz import timezone
from sqlalchemy.orm import joinedload
from models.models import db, Campaign
import logging

manila_tz = timezone("Asia/Manila")

@shared_task
def delete_old_campaigns():
    try:

        two_days_ago = datetime.now(manila_tz) - timedelta(days=2)

        # Fetch campaigns that were created 2 or more days ago
        old_campaigns = db.session.query(Campaign).filter(Campaign.created_at <= two_days_ago).all()

        if not old_campaigns:
            logging.info("[INFO] No old campaigns found for deletion.")
            return {"status": "success", "message": "No old campaigns found."}

        deleted_campaigns = []
        
        for campaign in old_campaigns:
            logging.info(f"[INFO] Deleting campaign ID: {campaign.campaign_id}, created at: {campaign.created_at}")
            deleted_campaigns.append(campaign.campaign_id)
            db.session.delete(campaign)

        # Commit deletion
        db.session.commit()
        logging.info(f"[INFO] Successfully deleted {len(deleted_campaigns)} old campaigns.")

        return {"status": "success", "deleted_campaigns": deleted_campaigns}

    except Exception as e:
        db.session.rollback()
        logging.error(f"[ERROR] Failed to delete old campaigns: {str(e)}")
        return {"status": "error", "message": str(e)}
