import sys
import time
import psycopg2
import pika
import json
import landing_page

class KTBH(object):
    def __init__(self, config):
        self.config = config
        self.amqp_host = config.get("main", "amqp_host")

    def hand_off(self, out_queue, body):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.amqp_host))
        channel = connection.channel()
        channel.queue_declare(queue=out_queue, durable=True)
        channel.basic_publish(exchange='',
                              routing_key=out_queue,
                              body=body,
                              properties=pika.BasicProperties(delivery_mode=2))
        connection.close()

    def add_landing_page(self, url):
        out_queue = self.config.get("main", "lp_queue")

        payload = json.dumps({
                "url": url
                })
        self.hand_off(out_queue, payload)

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

    def examine_landing_pages(self):
        out_queue = self.config.get("main", "lp_queue")
        url_queue = self.config.get("main", "url_queue")
        broken_queue = self.config.get("main", "broken_lp_queue")

        def callback(ch, method, properties, body):
            try:
                args = json.loads(body)
                url = args["url"]
                count = 0
                for text, href in landing_page.scrape(url):
                    payload = json.dumps({
                            "link_text": text,
                            "link_href": href
                            })
                    self.hand_off(url_queue, payload)
                    count += 1
                if count == 0:
                    self.hand_off(broken_queue, json.dumps({"url": url}))
            finally:
                ch.basic_ack(delivery_tag = method.delivery_tag)

        while True:
            self.handle_queue(out_queue, callback)
    
    def stash_unscrapables(self):
        broken_queue = self.config.get("main", "broken_lp_queue")

        def callback(ch, method, properties, body):
            try:
                args = json.loads(body)
                url = args["url"]
                sql = "insert into unscrapable_url (url) values (%(url)s);"
                sql2 = "select url from unscrapable_url where url = %(url)s;"

                db = psycopg2.connect(database="ktbh")
                c = db.cursor()
                c.execute(sql2, { "url": url })
                if c.rowcount > 0:
                    ch.basic_ack(delivery_tag = method.delivery_tag)
                    return
                c.execute(sql, { "url": url })
                db.commit()
                ch.basic_ack(delivery_tag = method.delivery_tag)
            except:
                print sys.exc_info()

        while True:
            self.handle_queue(broken_queue, callback)
