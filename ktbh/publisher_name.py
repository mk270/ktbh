
#  KTBH, a framework for pipelined data handling, by Martin Keegan
#
#  Copyright (C) 2012  Martin Keegan
#
#  This programme is free software; you may redistribute and/or modify
#  it under the terms of the GNU Affero General Public License

import json

def infer_publisher_code_callback(body):
    args = json.loads(body)

    url = args["link_href"]
    code = url.split('/')[2].split('.')[-3]
    title = code.title()

    args["publisher_name"] = {
        "code": code,
        "title": title
        }

    return [ ("pubnamed", args) ]
