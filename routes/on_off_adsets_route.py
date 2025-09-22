from flask import Blueprint, request
import redis
from controllers.on_off_adsets_controller import add_adset_off

adsets_on_off = Blueprint("adsets_on_off", __name__)

@adsets_on_off.route("/adsets", methods=["POST"])
def add_adsets_to_off():
    data = request.json
    return add_adset_off(data)