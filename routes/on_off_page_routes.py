from flask import Blueprint, request
from controllers.on_off_page_controller import add_pagename_off


pagename_on_off = Blueprint("pagename_on_off", __name__)

@pagename_on_off.route("/pagename", methods=["POST"])
def add_page_to_off():
    data = request.json
    return add_pagename_off(data)