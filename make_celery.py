from app import create_app


# Create the Flask app instance
flask_app = create_app()

# Access the Celery app instance from Flask's extensions
celery_app = flask_app.extensions["celery"]

