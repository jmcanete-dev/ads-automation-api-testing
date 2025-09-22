from celery import Celery, Task
from flask import Flask
from celery.schedules import crontab


def celery_init_app(app: Flask) -> Celery:
    class FlaskTask(Task):
        """Ensures Celery tasks run within Flask app context and retry on failure."""
        autoretry_for = (Exception,)  # Retries on any exception
        retry_kwargs = {"max_retries": 5, "countdown": 10}  # 5 retries, 10 sec delay

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery_app = Celery(
        app.name,
        task_cls=FlaskTask,
        broker=app.config.get("CELERY_BROKER_URL", "redis://redisAds:6379/0"),
        backend=app.config.get("CELERY_RESULT_BACKEND", "redis://redisAds:6379/0"),
        include=["workers.scheduler_celery", "workers.only_campaign_fetcher", "workers.delete_campaign_data_auto"],  # Auto-discover tasks
    )

    celery_app.conf.update(
        timezone="Asia/Manila",
        enable_utc=False,
        worker_prefetch_multiplier=3,
        broker_connection_retry_on_startup=True,  # Ensure Redis reconnects if down
        worker_max_tasks_per_child=100,  # Restart workers after processing 100 tasks (avoids stale DB connections)
        beat_schedule={
            "check_campaigns_every_minute": {
                "task": "workers.scheduler_celery.check_scheduled_adaccounts",
                "schedule": crontab(minute="*"),
            },
            "check_only_campaigns_every_minute": {
                "task": "workers.only_campaign_fetcher.check_campaign_off_only",
                "schedule": crontab(minute="*"),
            },
            "delete_campaign_data": {
                "task": "workers.delete_campaign_data_auto.delete_old_campaigns",
                "schedule": crontab(hour=0, minute=0),
            },
            # "fetch_campaigns_every_3_minutes": {
            #     "task": "workers.ad_spent_worker.fetch_all_accounts_campaigns",
            #     "schedule": crontab(minute="*/3"),  # Run every 3 minutes
            # },
        },
    )

    app.extensions["celery"] = celery_app
    return celery_app
