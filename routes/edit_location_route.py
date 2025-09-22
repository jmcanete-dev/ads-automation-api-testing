from flask import Blueprint, request
import redis
from controllers.edit_location_controller import edit_locations

edit_location_bp = Blueprint("edit-location", __name__)

@edit_location_bp.route("/editlocation", methods=["POST"])
def editlocation():
    data = request.json
    return edit_locations(data)