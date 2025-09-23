from urllib.parse import quote_plus
from flask import Flask, jsonify, request
from dotenv import load_dotenv
import os
from routes.create_ads_routes import createbp
from routes.fetch_ads_data import fetch_campaign_adsets_ads_creatives_bp
from routes.authentication_route import auth_bp
from flask_cors import CORS
from flask_jwt_extended import JWTManager
from celery_config import celery_init_app
from routes.verifyemail_routes import email_verification_bp
from routes.forgotpassword_routes import password_reset_bp
from routes.fetchparameters_route import parameters_bp
from routes.scheduler_routes import schedule_bp
from routes.export_region import export_region_bp
from routes.verify_accounts import verify_ad_accounts_bp
from routes.verify_adsets_routes import verify_adsets_accounts_bp
from routes.verify_page_name_routes import verify_page_name_bp
from routes.verify_schedule_routes import verify_scheduled_bp
from routes.campaign_off_only_routes import schedule_campaign_only_bp
from routes.on_off_campaign_name import campaign_on_off
from routes.on_off_adsets_route import adsets_on_off
from routes.on_off_page_routes import pagename_on_off
from routes.ad_spend_route import ad_spent_bp
from routes.user_settings_route import user_routes
from routes.verify_campaign_code_route import verify_campaign_code
from routes.sheets_route import sheets_bp
from routes.edit_budget_route import edit_budget_bp
from routes.edit_location_route import edit_location_bp
import logging
from flask_mail import Mail
from models.models import db, PHRegionTable, PHCityTable  # Import PHRegionTable
from app.on_off_sse import message_events_blueprint
from workers.on_off_functions.account_message import append_redis_message
# from workers.scheduler_celery import check_scheduled_adaccounts
# from workers.only_campaign_fetcher import check_campaign_off_only

def seed_regions():
    """Seed the database with region data if not already present."""
    regions_data = [
        {"id": 1, "region_name": "Ilocos Region", "region_key": 4181, "country_code": "PH"},
        {"id": 2, "region_name": "Cagayan Valley", "region_key": 4182, "country_code": "PH"},
        {"id": 3, "region_name": "Central Luzon", "region_key": 4183, "country_code": "PH"},
        {"id": 4, "region_name": "Calabarzon", "region_key": 4184, "country_code": "PH"},
        {"id": 5, "region_name": "Mimaropa", "region_key": 4185, "country_code": "PH"},
        {"id": 6, "region_name": "Bicol Region", "region_key": 4186, "country_code": "PH"},
        {"id": 7, "region_name": "Western Visayas", "region_key": 4187, "country_code": "PH"},
        {"id": 8, "region_name": "Central Visayas", "region_key": 4188, "country_code": "PH"},
        {"id": 9, "region_name": "Eastern Visayas", "region_key": 4189, "country_code": "PH"},
        {"id": 10, "region_name": "Zamboanga Peninsula", "region_key": 2932, "country_code": "PH"},
        {"id": 11, "region_name": "Northern Mindanao", "region_key": 4190, "country_code": "PH"},
        {"id": 12, "region_name": "Davao Region", "region_key": 2825, "country_code": "PH"},
        {"id": 13, "region_name": "Soccsksargen", "region_key": 4191, "country_code": "PH"},
        {"id": 14, "region_name": "Caraga", "region_key": 4192, "country_code": "PH"},
        {"id": 15, "region_name": "Metro Manila", "region_key": 4179, "country_code": "PH"},
        {"id": 16, "region_name": "Cordillera Administrative Region", "region_key": 4180, "country_code": "PH"},
        {"id": 17, "region_name": "Autonomous Region in Muslim Mindanao", "region_key": 4193, "country_code": "PH"},
    ]

    # Check existing region keys to avoid duplicates
    existing_keys = {region.region_key for region in PHRegionTable.query.all()}
    
    new_regions = [
        PHRegionTable(**region) for region in regions_data if region["region_key"] not in existing_keys
    ]

    if new_regions:
        db.session.bulk_save_objects(new_regions)
        db.session.commit()
        print("PH_REGION_TABLES seeded successfully!")
    else:
        print("PH_REGION_TABLES already contains all regions.")

def seed_cities():
    """Seed the database with city data if not already present."""
    cities_data = [
        {"city_name": "Cebu","city_key": 1735759,"country_code": "PH"},
        
    ]

    # Check existing city keys to avoid duplicates
    existing_keys = {city.city_key for city in PHCityTable.query.all()}
    
    new_cities = [
        PHCityTable(**city) for city in cities_data if city["city_key"] not in existing_keys
    ]

    if new_cities:
        db.session.bulk_save_objects(new_cities)
        db.session.commit()
        print("PH_CITY_TABLES seeded successfully!")


def create_app():
    app = Flask(__name__)
    load_dotenv()

    allowed_origins = os.getenv("ALLOWED_ORIGINS", "").split(",")
    CORS(app, resources={r"/api/*": {"origins": allowed_origins}})

    mail = Mail()

    app.logger.setLevel(logging.DEBUG)
    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY')

    # Configure Database for PostgreSQL using SQLAlchemy
    password = quote_plus(os.getenv('POSTGRES_PASSWORD'))

    app.config['SQLALCHEMY_DATABASE_URI'] = f"postgresql://{os.getenv('POSTGRES_USER')}:{password}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    db.init_app(app)  # Initialize SQLAlchemy with the app

    # Initialize JWT
    jwt = JWTManager(app)

    app.config["CELERY"] = {
        "broker_url": os.getenv('CELERY_BROKER_URL', 'redis://redisAds:6379/0'),
        "result_backend": os.getenv('CELERY_RESULT_BACKEND', 'redis://redisAds:6379/0'),
        'timezone': 'Asia/Manila',
    }

    celery = celery_init_app(app)
    celery.set_default()

    # Configure Flask-Mail
    def configure_mail(app):
        app.config['MAIL_SERVER'] = os.getenv('MAIL_SERVER', 'smtp.gmail.com')
        app.config['MAIL_PORT'] = int(os.getenv('MAIL_PORT', 587))
        app.config['MAIL_USERNAME'] = os.getenv('MAIL_USERNAME')
        app.config['MAIL_PASSWORD'] = os.getenv('MAIL_PASSWORD')
        app.config['MAIL_USE_TLS'] = os.getenv('MAIL_USE_TLS', 'True') == 'True'
        app.config['MAIL_USE_SSL'] = os.getenv('MAIL_USE_SSL', 'False') == 'True'
        mail.init_app(app)

    # Create database tables if they don't exist and seed regions
    with app.app_context():
        db.create_all()
        configure_mail(app)
        seed_regions()  # Call the seed function after creating tables
        seed_cities()

    # healthcheck endpoint
    @app.route("/health", methods=["GET"])
    def health():
        return jsonify({"status": "ok"}), 200

    @app.route("/")
    def home():
        return "Welcome to the Ads Manager API PGOC"
    
    @app.route("/append_message", methods=["POST"])
    def append_message_route():
        """Route to append a message to Redis and emit via WebSocket."""
        data = request.json
        user_id = data.get("user_id")
        ad_account_id = data.get("ad_account_id")
        new_message = data.get("message")

        if not user_id or not ad_account_id or not new_message:
            return jsonify({"error": "Missing required fields"}), 400

        try:
            append_redis_message(user_id, ad_account_id, new_message)
            return jsonify({"success": True, "message": "Message added to Redis and WebSocket emitted."})
        except Exception as e:
            logging.error(f"Error processing request: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    # Register the blueprints with prefixed routes
    app.register_blueprint(auth_bp, url_prefix='/api/v1/auth')
    app.register_blueprint(createbp, url_prefix='/api/v1/campaign')
    app.register_blueprint(email_verification_bp, url_prefix='/api/v1/auth')
    app.register_blueprint(password_reset_bp, url_prefix='/api/v1/auth')
    app.register_blueprint(parameters_bp, url_prefix='/api/v1/parameters')
    app.register_blueprint(fetch_campaign_adsets_ads_creatives_bp, url_prefix='/api/v1/fetch')
    app.register_blueprint(schedule_bp, url_prefix="/api/v1/schedule")
    app.register_blueprint(verify_ad_accounts_bp, url_prefix="/api/v1/verify-ads-account")
    app.register_blueprint(verify_adsets_accounts_bp, url_prefix="/api/v1/verify")
    app.register_blueprint(verify_page_name_bp, url_prefix="/api/v1/verify")
    app.register_blueprint(verify_scheduled_bp, url_prefix="/api/v1/verify")
    app.register_blueprint(verify_campaign_code, url_prefix="/api/v1/verify")
    app.register_blueprint(export_region_bp)
    app.register_blueprint(message_events_blueprint, url_prefix="/api/v1")
    app.register_blueprint(campaign_on_off, url_prefix="/api/v1/onoff")
    app.register_blueprint(adsets_on_off, url_prefix="/api/v1/onoff")
    app.register_blueprint(pagename_on_off, url_prefix="/api/v1/onoff")
    app.register_blueprint(schedule_campaign_only_bp, url_prefix="/api/v1/campaign-only")
    app.register_blueprint(ad_spent_bp, url_prefix='/api/v1')
    app.register_blueprint(user_routes, url_prefix='/api/v1')
    app.register_blueprint(sheets_bp, url_prefix='/api/v1/sheets')
    app.register_blueprint(edit_budget_bp, url_prefix='/api/v1/campaign')
    app.register_blueprint(edit_location_bp, url_prefix='/api/v1/campaign')

    return app

if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=5095, debug=True, threaded=True)
