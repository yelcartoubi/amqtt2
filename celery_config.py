# from celery import Celery
from flask import Flask
import redis
from celery import Celery

def create_celery_app(flask_app):
    celery = Celery(
        flask_app.import_name,
        broker=flask_app.config['broker_url'],
        backend=flask_app.config['result_backend']
    )
    celery.conf.update(flask_app.config)


    class ContextTask(celery.Task):

        def __call__(self, *args, **kwargs):
            with flask_app.app_context():
                return super().__call__(*args, **kwargs)

    celery.Task = ContextTask
    return celery


# def make_celery(app):
#     celery = Celery(
#         app.import_name,
#         broker=app.config['broker_url'],
#         backend=app.config['result_backend']
#     )
#     celery.conf.update(app.config)
#
#     class ContextTask(celery.Task):
#         def __call__(self, *args, **kwargs):
#             with app.app_context():
#                 return super().__call__(*args, **kwargs)
#
#     celery.Task = ContextTask
#     return celery

def make_redis_client():
    redis_client = redis.StrictRedis(host="localhost", port=6379, db=0)

def create_app():
    app = Flask(__name__)
    app.config.update(
        # broker_url='redis://localhost:6379/0',
        # result_backend='redis://localhost:6379/0',
        broker_url='redis://localhost:6379/0',
        result_backend='redis://localhost:6379/0',
        SECRET_KEY = 'secret_key'
    )
    return app

app = create_app()
redis_client = make_redis_client()
celery = create_celery_app(app)

# Import the tasks to register them with Celery
# import tasks
