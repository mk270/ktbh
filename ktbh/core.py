import sys
import time
import psycopg2
import pika
import json
import landing_page

class PipeRouter(object):
    def __init__(self, amqp_host):
        self.amqp_host = amqp_host

    def hand_off_json(self, queue, args):
        return self.hand_off(queue, json.dumps(args))

    def hand_off(self, queue, body):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.amqp_host))
        channel = connection.channel()
        channel.queue_declare(queue=queue, durable=True)
        channel.basic_publish(exchange='',
                              routing_key=queue,
                              body=body,
                              properties=pika.BasicProperties(delivery_mode=2))
        connection.close()

    def get_connection(self, host):
        count = 0.4
        while count < 60:
            try:            
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=host))
                return connection
            except:
                time.sleep(count)
                count *= 1.7
        sys.exit(1)

    def handle_queue(self, queue_name, callback_fn):
        connection = self.get_connection(self.amqp_host)
        try:
            channel = connection.channel()
            channel.queue_declare(queue=queue_name, durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(callback_fn, queue=queue_name)
            channel.start_consuming()
        except:
            pass
        finally:
            connection.close()

    def handle_queue_forever(self, queue_name, callback_fn):
        while True:
            self.handle_queue(queue_name, callback_fn)

    def make_callback(self, f, errors_queue):
        def callback(ch, method, properties, body):
            try:
                result = f(body)
            except:
                error_msg = {
                    "error": {
                        "type": sys.exc_info()[0],
                        "err_str": sys.exc_info()[1],
                        "orig_body": body
                        }
                    }
                result = (errors_queue, error_msg)
            try:
                if result is not None:
                    queue, msg = result
                    self.hand_off_json(queue, msg)
            finally:
                ch.basic_ack(delivery_tag = method.delivery_tag)
        return callback

class KTBH(object):
    def __init__(self, config):
        self.router = PipeRouter(config.get("main", "amqp_host"))
        self.out_queue = config.get("main", "lp_queue")
        self.broken_queue = config.get("main", "broken_lp_queue")
        self.url_queue = config.get("main", "url_queue")
        self.database_name = config.get("database", "name")

    def add_landing_page(self, url):
        payload = {
            "url": url
            }
        self.router.hand_off_json(self.out_queue, payload)

    def examine_landing_pages(self):
        def callback(ch, method, properties, body):
            try:
                args = json.loads(body)
                if "url" not in args:
                    return
                url = args["url"]
                count = 0
                for text, href in landing_page.scrape(url):
                    payload = {
                        "link_text": text,
                        "link_href": href
                        }
                    self.router.hand_off_json(self.url_queue, payload)
                    count += 1
                if count == 0:
                    self.router.hand_off_json(self.broken_queue, {"url": url})
            except:
                self.router.hand_off_json(self.broken_queue, {"url": url})
            finally:
                ch.basic_ack(delivery_tag = method.delivery_tag)

        self.router.handle_queue_forever(self.out_queue, callback)
    
    def stash_unscrapables(self):
        def handle_unscrapable(body):
            args = json.loads(body)
            url = args["url"]
            sql = "insert into unscrapable_url (url) values (%(url)s);"
            sql2 = "select url from unscrapable_url where url = %(url)s;"
            
            db = psycopg2.connect(database=self.database_name)
            c = db.cursor()
            c.execute(sql2, { "url": url })
            if c.rowcount == 0:
                c.execute(sql, { "url": url })
                db.commit()
            return None

        errors_queue = "errors"
        cb = self.router.make_callback(handle_unscrapable, errors_queue)
        self.router.handle_queue_forever(self.broken_queue, cb)
