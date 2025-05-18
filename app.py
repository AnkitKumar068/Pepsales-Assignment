from flask import Flask, request, jsonify
import pika
import threading
import time
import logging
from functools import wraps
from pymongo import MongoClient
from bson import ObjectId  # just keep here, maybe need later

# logging setup - simple is best
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# MongoDB connect - hope work!
try:
    mongo_client = MongoClient("mongodb://localhost:27017/", connectTimeoutMS=2000)
    db = mongo_client["notification_service"]
    collection = db["notifications"]
    logger.info("MongoDB connect success!")
except Exception as e:
    logger.error(f"MongoDB no connect: {e}")
    # app maybe work without? who knows

# RabbitMQ things
RABBITMQ_HOST = 'localhost'
QUEUE_NAME = 'notifications'
MAX_RETRIES = 3  # 3 try good number i think

# global vars - i know bad practice but work fast
rabbit_connection = None
rabbit_channel = None

def connect_rabbitmq():
    global rabbit_connection, rabbit_channel
    try_count = 0
    while try_count < MAX_RETRIES:
        try:
            # try make RabbitMQ connect
            rabbit_connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    heartbeat=600,  # 10 min ok?
                    connection_attempts=3,
                    retry_delay=5  # wait 5 sec between try
                )
            )
            
            rabbit_channel = rabbit_connection.channel()
            
            # make main queue with dead letter
            rabbit_channel.queue_declare(
                queue=QUEUE_NAME,
                durable=True,  # want message stay after restart
                arguments={
                    'x-dead-letter-exchange': 'dlx',
                    'x-dead-letter-routing-key': 'retry_route'
                }
            )
            
            # setup dead letter exchange for retry
            rabbit_channel.exchange_declare(exchange='dlx', exchange_type='direct')
            rabbit_channel.queue_declare(
                queue='retry_queue',
                durable=True,
                arguments={
                    'x-dead-letter-exchange': '',
                    'x-dead-letter-routing-key': QUEUE_NAME,
                    'x-message-ttl': 60000  # wait 1 min before retry
                }
            )
            rabbit_channel.queue_bind(
                exchange='dlx',
                queue='retry_queue',
                routing_key='retry_route'
            )
            
            logger.info("RabbitMQ connect work!")
            return
        except Exception as e:
            try_count += 1
            wait_time = 2 ** try_count  # wait longer each time
            logger.error(f"RabbitMQ no connect (try {try_count}): {str(e)}")
            logger.info(f"Wait {wait_time} sec then try again...")
            time.sleep(wait_time)
    
    raise Exception("RabbitMQ connect fail after many try. So sad.")

# decorator for when RabbitMQ break
def handle_rabbitmq_failure(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.StreamLostError) as e:
            logger.warning(f"Oh no! RabbitMQ break: {e}. Try reconnect...")
            connect_rabbitmq()
            return func(*args, **kwargs)  # try again now
    return wrapper

@handle_rabbitmq_failure
def send_to_queue(notification_data, retry_count=0):
    try:
        rabbit_channel.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME,
            body=str(notification_data),  # make string simple way
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message stay
                headers={'retry_count': retry_count}
            )
        )
        logger.info(f"Put in queue for user {notification_data['user_id']}")
    except Exception as e:
        if retry_count < MAX_RETRIES:
            logger.warning(f"Fail send to queue (will retry {retry_count + 1}): {e}")
            time.sleep(2 ** retry_count)
            send_to_queue(notification_data, retry_count + 1)
        else:
            logger.error(f"Too many fail ({MAX_RETRIES} try). Error: {e}")
            raise

# worker eat notifications
def process_notifications():
    def callback(ch, method, properties, body):
        try:
            # fast way make string to dict
            notification_data = eval(body.decode())  # eval scary but work for now
            
            logger.info(f"Got notify for user {notification_data['user_id']}")
            logger.info(f"Type: {notification_data['type']}, Msg: {notification_data['message']}")
            
            # save to MongoDB
            collection.insert_one({
                "user_id": notification_data['user_id'],
                "type": notification_data['type'],
                "message": notification_data['message'],
                "timestamp": time.time()  # simple time ok
            })
            
            # fake some work time
            time.sleep(0.5)
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info("Notify process good!")
        except Exception as e:
            retry_count = properties.headers.get('retry_count', 0)
            if retry_count < MAX_RETRIES:
                logger.warning(f"Process fail (retry {retry_count + 1}): {e}")
                ch.basic_publish(
                    exchange='dlx',
                    routing_key='retry_route',
                    body=body,
                    properties=pika.BasicProperties(
                        headers={'retry_count': retry_count + 1}
                    )
                )
            ch.basic_ack(delivery_tag=method.delivery_tag)  # say done even if fail
            logger.error(f"Process notify fail: {e}")

    # keep eating messages forever
    while True:
        try:
            rabbit_channel.basic_qos(prefetch_count=1)  # eat one by one
            rabbit_channel.basic_consume(
                queue=QUEUE_NAME,
                on_message_callback=callback,
                auto_ack=False  # say done by self
            )
            logger.info("Start eat messages...")
            rabbit_channel.start_consuming()
        except Exception as e:
            logger.error(f"Consumer broke: {e}. Restart in 5 sec...")
            time.sleep(5)
            connect_rabbitmq()

# API things
@app.route('/notifications', methods=['POST'])
def send_notification():
    data = request.get_json()
    
    # check have needed things
    if not data or 'user_id' not in data or 'type' not in data or 'message' not in data:
        return jsonify({'error': 'Need user_id, type, message'}), 400
    
    notification_data = {
        'user_id': data['user_id'],
        'type': data['type'],
        'message': data['message']
    }
    
    try:
        send_to_queue(notification_data)
        return jsonify({'status': 'Notify in queue now'}), 201
    except Exception as e:
        return jsonify({'error': f"Fail put in queue: {str(e)}"}), 500

@app.route('/users/<user_id>/notifications', methods=['GET'])
def get_user_notifications(user_id):
    try:
        # get all notify for this user
        notifications = list(collection.find({'user_id': user_id}))
        
        # fix _id for json
        for n in notifications:
            n['_id'] = str(n['_id'])
        
        return jsonify({'user_id': user_id, 'notifications': notifications})
    except Exception as e:
        return jsonify({'error': f"Fail get notify: {str(e)}"}), 500

# start consumer in background
def start_consumer_thread():
    connect_rabbitmq()
    consumer_thread = threading.Thread(target=process_notifications, daemon=True)
    consumer_thread.start()
    logger.info("Background worker start eat!")

# clean when app stop
@app.teardown_appcontext
def cleanup(exception=None):
    if rabbit_connection and rabbit_connection.is_open:
        rabbit_connection.close()
        logger.info("RabbitMQ connect close")

if __name__ == '__main__':
    start_consumer_thread()
    app.run(host='0.0.0.0', port=3000, debug=True)