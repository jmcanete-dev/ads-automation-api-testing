from flask import Blueprint, request
from controllers.on_off_campaign_name_controller import add_campaign_off


campaign_on_off = Blueprint("campaign_on_off", __name__)

@campaign_on_off.route("/campaigns", methods=["POST"])
def add_campaign_to_off():
    data = request.json
    return add_campaign_off(data)

# @campaign_on_off.route("/get-campaigns", methods=["GET"])
# def get_campaign_to_off():
#     user_id = request.args.get("user_id")

#     return get_campaign_off(user_id)

# @campaign_on_off.route("/delete-campaigns", methods=["DELETE"])
# def delete_campaign_to_off():
#     data = request.json
#     user_id = data.get("user_id")
#     ad_account_ids = data.get("ad_account_ids", [])
#     return delete_campaign_off(user_id, ad_account_ids)