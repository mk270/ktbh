import sys
import time
import psycopg2
import pika
import json
import landing_page

def hand_off(amqp_host, out_queue, body):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=amqp_host))
    channel = connection.channel()
    channel.queue_declare(queue=out_queue, durable=True)
    channel.basic_publish(exchange='',
                          routing_key=out_queue,
                          body=body,
                          properties=pika.BasicProperties(delivery_mode=2))
    connection.close()

def add_landing_page(url, config):
    amqp_host = config.get("main", "amqp_host")
    out_queue = config.get("main", "lp_queue")

    payload = json.dumps({
            "url": url
            })
    hand_off(amqp_host, out_queue, payload)

def get_connection(host):
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

def handle_queue(amqp_host, queue_name, callback_fn):
    connection = get_connection(amqp_host)
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

def examine_landing_pages(config):
    out_queue = config.get("main", "lp_queue")
    url_queue = config.get("main", "url_queue")
    broken_queue = config.get("main", "broken_lp_queue")
    amqp_host = config.get("main", "amqp_host")

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
                hand_off(amqp_host, url_queue, payload)
                count += 1
            if count == 0:
                hand_off(amqp_host, broken_queue, json.dumps({"url": url}))
        finally:
            ch.basic_ack(delivery_tag = method.delivery_tag)

    while True:
        handle_queue(amqp_host, out_queue, callback)
    
def stash_unscrapables(config):
    broken_queue = config.get("main", "broken_lp_queue")
    amqp_host = config.get("main", "amqp_host")

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
        handle_queue(amqp_host, broken_queue, callback)
