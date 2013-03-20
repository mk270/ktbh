import sys
import time
import pika
import json

class PipeRouter(object):
    class NoConnection(Exception): pass

    def __init__(self, amqp_host):
        self.amqp_host = amqp_host

    def stop(self):
        self.connection.close()
        
    def delete_queue(self, queue):
        connection = self.get_connection()
        channel = connection.channel()
        channel.queue_delete(queue=queue)
        connection.close()

    def hand_off_json(self, queue, args):
        return self.hand_off(queue, json.dumps(args))

    def hand_off(self, queue, body):
        connection = self.get_connection()
        try:
            channel = connection.channel()
            channel.queue_declare(queue=queue, durable=True)
            channel.basic_publish(exchange='',
                                  routing_key=queue,
                                  body=body,
                                  properties=pika.BasicProperties(delivery_mode=2))
        finally:
            connection.close()

    def get_connection(self):
        count = 0.4
        while count < 60:
            try:            
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=self.amqp_host))
                return connection
            except:
                time.sleep(count)
                count *= 1.7
        raise NoConnection

    def handle_queue(self, queue_name, callback_fn):
        connection = self.get_connection()
        try:
            channel = connection.channel()
            channel.queue_declare(queue=queue_name, durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(callback_fn, queue=queue_name)
            channel.start_consuming()
        except:
            print sys.exc_info()
        finally:
            connection.close()

    def handle_queue_forever(self, queue_name, callback_fn):
        while True:
            self.handle_queue(queue_name, callback_fn)

    def create_handler(self, f, errors_queue):
        def callback(ch, method, properties, body):
            try:
                results = f(body)
            except:
                error_msg = {
                    "error": {
                        "type": str(sys.exc_info()[0]),
                        "err_str": str(sys.exc_info()[1]),
                        "orig_body": body
                        }
                    }
                results = [ (errors_queue, error_msg) ]
            ch.basic_ack(delivery_tag = method.delivery_tag)
            try:
                for r in results:
                    queue, msg = r
                    self.hand_off_json(queue, msg)
            finally:
                pass
        return callback

    def route(self, callback=None, input_queue=None, error_queue=None):
        cb = self.create_handler(callback, error_queue)
        self.handle_queue_forever(input_queue, cb)
