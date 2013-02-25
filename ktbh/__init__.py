import pika
import json

OUT_QUEUE = "ktbh_landing_pages"

def hand_off(out_queue, body):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=out_queue, durable=True)
    channel.basic_publish(exchange='',
                          routing_key=out_queue,
                          body=body,
                          properties=pika.BasicProperties(delivery_mode=2))
    connection.close()

def add_landing_page(url):
    payload = json.dumps({
            "url": url
            })
    hand_off(OUT_QUEUE, payload)
    
