from flask import Blueprint, jsonify, send_file
from io import BytesIO
import json
from models.models import db, PHRegionTable

export_region_bp = Blueprint('export_region', __name__)

@export_region_bp.route('/regions', methods=['GET'])
def export_regions_json():
    """Exports PH Region data as a JSON file."""
    regions = PHRegionTable.query.all()
    
    # Convert data to JSON format
    regions_data = [
        {
            "id": region.id,
            "region_name": region.region_name,
            "region_key": region.region_key,
            "country_code": region.country_code
        }
        for region in regions
    ]

    # Create a JSON file in memory
    json_data = json.dumps(regions_data, indent=4)
    buffer = BytesIO()
    buffer.write(json_data.encode('utf-8'))
    buffer.seek(0)

    return send_file(buffer, as_attachment=True, download_name="ph_regions.json", mimetype="application/json")

