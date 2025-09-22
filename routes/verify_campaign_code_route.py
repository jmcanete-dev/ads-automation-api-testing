from flask import Blueprint
from controllers.verify_campaign_code_controller import validate_campaign_code

verify_campaign_code = Blueprint("verify_campaign_code", __name__)

# Route for CSV validation
@verify_campaign_code.route("/campaign-code", methods=["POST"])
def validate_campaign_code_route():
    return validate_campaign_code()