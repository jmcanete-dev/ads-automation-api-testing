from flask import Blueprint, request
import redis
from controllers.verify_scheduled_onoff_controller import verify_schedule

verify_scheduled_bp= Blueprint("verify_scheduled", __name__)

@verify_scheduled_bp.route("/schedule", methods=["POST"])
def verify_page_name():
    data = request.json
    return verify_schedule(data)