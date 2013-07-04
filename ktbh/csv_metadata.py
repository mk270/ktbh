
#  KTBH, a framework for pipelined data handling, by Martin Keegan
#
#  Copyright (C) 2012  Martin Keegan
#
#  This programme is free software; you may redistribute and/or modify
#  it under the terms of the GNU Affero General Public License

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

    args["url"] = url
    args["csvddf"] = dialect.as_dict()
    
    return [ ("schema", args) ]

def infer_schema_callback(body):
    args = json.loads(body)

    data = preview(args["url"])

    sch, date_formats = schema.infer_schema(data, 
                                            args["csvddf"]["dialect"])
    
    args["schema"] = json.loads(sch)
    args["date_format"] = date_formats

    return [ ("download", args) ]
