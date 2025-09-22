from flask import Blueprint, request
import redis
from controllers.edit_budget_controller import edit_budget

edit_budget_bp = Blueprint("edit-budget", __name__)

@edit_budget_bp.route("/editbudget", methods=["POST"])
def editbudget():
    data = request.json
    return edit_budget(data)