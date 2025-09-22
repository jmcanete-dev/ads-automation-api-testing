from flask import Blueprint, request
import redis
from controllers.ad_spend_controller import ad_spent

ad_spent_bp = Blueprint("ad-spent", __name__)

@ad_spent_bp.route("/adspent", methods=["POST"])
def adspent():
    data = request.json
    return ad_spent(data)