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
        self.out_queue = config.get("main", "lp_queue")
        self.broken_queue = config.get("main", "broken_lp_queue")
        self.url_queue = config.get("main", "url_queue")
        self.database_name = config.get("database", "name")
        self.schema_queue = config.get("main", "schema_queue")
        self.download_queue = config.get("main", "download_queue")
        
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
                    return []
                url = args["url"]
                count = 0

                def collect_links(text, href):
                    new_url = urlparse.urljoin(url, href)
                    payload = {
                        "link_text": text,
                        "link_href": new_url
                        }
                    return (self.url_queue, payload)
                    
                results = [ collect_links(text, href) 
                            for text, href in landing_page.scrape(url) ]
                if len(results) > 0:
                    return results
            except:
                pass
            return [ (self.broken_queue, {"url": url}) ]

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
            return [ (self.schema_queue, { "url": url,
                                           "csvddf": dialect.as_dict()
                                           }) ]

        errors_queue = "errors"
        self.router.route(callback=callback,
                          input_queue=self.url_queue,
                          error_queue=errors_queue)

    def infer_schema(self):
        def callback(body):
            args = json.loads(body)

            data = self.preview(args["url"])
            args["schema"] = schema.infer_schema(data, 
                                                 args["csvddf"]["dialect"])

            return [ (self.download_queue, args) ]

        errors_queue = "errors"
        self.router.route(callback=callback,
                          input_queue=self.schema_queue,
                          error_queue=errors_queue)
