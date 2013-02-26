import sys
import psycopg2
import json
import landing_page
import waterfall

class KTBH(object):
    def __init__(self, config):
        self.router = waterfall.PipeRouter(config.get("main", "amqp_host"))
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
        def callback(body):
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
                    yield (self.url_queue, payload)
                    count += 1
                if count == 0:
                    yield (self.broken_queue, {"url": url})
            except:
                yield (self.broken_queue, {"url": url})

        errors_queue = "errors"
        self.router.route(callback=callback,
                          input_queue=self.out_queue,
                          error_queue=errors_queue)
    
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
            return []

        errors_queue = "errors"
        self.router.route(callback=handle_unscrapable,
                          input_queue=self.broken_queue,
                          error_queue=errors_queue)

