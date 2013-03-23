
import requests
import json
import unicodecsv
import csvddf
import schema

def preview(url):
    preview_size = 10000

    r = requests.get(url, stream=True)
    return r.iter_content(chunk_size=preview_size).next()

def infer_dialects_callback(body):
    args = json.loads(body)
    url = args["link_href"]

    data = preview(url)
            
    d = unicodecsv.Sniffer().sniff(data, delimiters=['\t', ','])
    dialect = csvddf.CSVDDF(dialect=d)
    return [ ("schema", { "url": url,
                          "csvddf": dialect.as_dict()
                          }) ]

def infer_schema_callback(body):
    args = json.loads(body)

    data = preview(args["url"])

    schema, date_formats = schema.infer_schema(data, 
                                               args["csvddf"]["dialect"]))

    args["schema"] = json.loads(schema)
    args["date_format"] = date_formats

    return [ ("download", args) ]
