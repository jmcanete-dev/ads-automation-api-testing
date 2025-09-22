from flask import Blueprint, request, jsonify
from controllers.sheets_controller import update_budget

sheets_bp = Blueprint("sheets", __name__)

@sheets_bp.route("/update-budget", methods=["POST"])
def update_budget_route():
    """Route to handle updating the budget in Google Sheets."""
    data = request.get_json()
    response, status_code = update_budget(data)
    return jsonify(response), status_code 