from flask import Blueprint, request
import redis
from controllers.verify_campaignV2_controller import verify_pagename

verify_page_name_bp= Blueprint("verify_page_name", __name__)

@verify_page_name_bp.route("/pagename", methods=["POST"])
def verify_page_name():
    data = request.json
    return verify_pagename(data)