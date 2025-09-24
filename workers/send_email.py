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

@shared_task(bind=True, max_retries=3, default_retry_delay=60)  # retry up to 3 times, wait 60s each
def send_email_task_with_resend(self, email, subject, html_content):
    """Celery task with automatic retries if sending fails."""
    try:
        msg = Message(
            subject=subject,
            sender=os.getenv('MAIL_USERNAME'),
            recipients=[email],
            html=html_content
        )
        mail.send(msg)
        return {'status': 'success', 'message': f'Email sent to {email}'}
    except Exception as e:
        try:
            self.retry(exc=e)  # Celery will automatically retry
        except self.MaxRetriesExceededError:
            return {'status': 'error', 'message': f'Failed after max retries: {str(e)}'}

