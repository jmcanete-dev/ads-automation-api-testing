from celery import shared_task
from flask_mail import Mail, Message
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Flask-Mail Configuration
mail = Mail()

def configure_mail(app):
    app.config['MAIL_SERVER'] = os.getenv('MAIL_SERVER', 'smtp.gmail.com')
    app.config['MAIL_PORT'] = int(os.getenv('MAIL_PORT', 587))
    app.config['MAIL_USERNAME'] = os.getenv('MAIL_USERNAME')
    app.config['MAIL_PASSWORD'] = os.getenv('MAIL_PASSWORD')
    app.config['MAIL_USE_TLS'] = os.getenv('MAIL_USE_TLS', 'True') == 'True'
    app.config['MAIL_USE_SSL'] = os.getenv('MAIL_USE_SSL', 'False') == 'True'
    mail.init_app(app)

@shared_task(bind=True)
def send_email_task(self, email, subject, html_content):
    """Shared Celery task to send email asynchronously with preformatted HTML content."""
    try:
        msg = Message(
            subject=subject,
            sender=os.getenv('MAIL_USERNAME'),
            recipients=[email],
            html=html_content  # Now passing preformatted HTML content
        )
        mail.send(msg)
        return {'status': 'success', 'message': f'Email sent to {email}'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}
