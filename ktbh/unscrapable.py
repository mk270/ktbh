import json
import psycopg2

def handle_unscrapable_callback(database_name):
    def handle_unscrapable(body):
        args = json.loads(body)
        url = args["url"]
        sql = "insert into unscrapable_url (url) values (%(url)s);"
        sql2 = "select url from unscrapable_url where url = %(url)s;"
            
        db = psycopg2.connect(database=database_name)
        c = db.cursor()
        c.execute(sql2, { "url": url })
        if c.rowcount == 0:
            c.execute(sql, { "url": url })
            db.commit()
        return []
    return handle_unscrapable
