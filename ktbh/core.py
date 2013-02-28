import sys
import psycopg2
import json
import landing_page
import waterfall
import requests
import unicodecsv
import csvddf
import urlparse
import schema

class KTBH(object):
    def __init__(self, config):
        self.router = waterfall.PipeRouter(config.get("main", "amqp_host"))
        self.database_name = config.get("database", "name")

        self.queues = {}
        for q_name, conf_section, conf_item in [
            ("out", "main", "lp_queue"),
            ("broken", "main", "broken_lp_queue"),
            ("url", "main", "url_queue"),
            ("schema", "main", "schema_queue"),
            ("download", "main", "download_queue")
            ]:
            self.queues[q_name] = config.get(conf_section, conf_item)
        self.queues["error"] = "errors"

    def add_landing_page(self, url):
        payload = {
            "url": url
            }
        self.router.hand_off_json(self.queues["out"], payload)

    def examine_landing_pages(self):
        def callback(body):
            try:
                args = json.loads(body)
                if "url" not in args:
                    return []
                url = args["url"]
                count = 0

                def collect_links(text, href):
                    new_url = urlparse.urljoin(url, href)
                    payload = {
                        "link_text": text,
                        "link_href": new_url
                        }
                    return (self.queues["url"], payload)
                    
                results = [ collect_links(text, href) 
                            for text, href in landing_page.scrape(url) ]
                if len(results) > 0:
                    return results
            except:
                pass
            return [ (self.queues["broken"], {"url": url}) ]

        self.router.route(callback=callback,
                          input_queue=self.queues["out"],
                          error_queue=self.queues["error"])
    
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

        self.router.route(callback=handle_unscrapable,
                          input_queue=self.queues["broken"],
                          error_queue=self.queues["error"])

    def preview(self, url):
        preview_size = 10000

        r = requests.get(url, stream=True)
        return r.iter_content(chunk_size=preview_size).next()

    def infer_dialect(self):
        def callback(body):
            args = json.loads(body)
            url = args["link_href"]

            data = self.preview(url)
            
            d = unicodecsv.Sniffer().sniff(data, delimiters=['\t', ','])
            dialect = csvddf.CSVDDF(dialect=d)
            return [ (self.queues["schema"], { "url": url,
                                               "csvddf": dialect.as_dict()
                                               }) ]

        self.router.route(callback=callback,
                          input_queue=self.queues["url"],
                          error_queue=self.queues["error"])

    def infer_schema(self):
        def callback(body):
            args = json.loads(body)

            data = self.preview(args["url"])
            args["schema"] = schema.infer_schema(data, 
                                                 args["csvddf"]["dialect"])

            return [ (self.queues["download"], args) ]

        self.router.route(callback=callback,
                          input_queue=self.queues["schema"],
                          error_queue=self.queues["error"])
