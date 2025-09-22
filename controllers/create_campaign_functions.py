from models.models import db, Campaign
from sqlalchemy.exc import SQLAlchemyError
import requests
import logging



def delete_facebook(entity_id, access_token):
    """
    Delete a Facebook Ads entity (campaign, adset, adcreative, or ad).

    :param entity_id: The ID of the entity to delete.
    :param access_token: Facebook API Access Token.
    """
    try:
        # Graph API delete endpoint
        delete_url = f"https://graph.facebook.com/v23.0/{entity_id}"
        headers = {"Authorization": f"Bearer {access_token}"}

        # Make DELETE request
        response = requests.delete(delete_url, headers=headers)

        # Check response status
        if response.status_code == 200:
            logging.info(f"Successfully deleted entity with ID {entity_id}.")
            return {"status": "success", "message": "Entity deleted successfully."}
        else:
            logging.error(f"Failed to delete entity. Error: {response.text}")
            return {"status": "failed", "error": response.text}

    except Exception as e:
        logging.error(f"An error occurred during deletion: {str(e)}")
        return {"status": "error", "details": str(e)}
