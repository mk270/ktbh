import json

def infer_publisher_code_callback(body):
    args = json.loads(body)

    url = args["url"]
    code = url.split('/')[2].split('.')[-3]
    title = code.title()

    args["publisher_name"] = {
        "code": code,
        "title": title
        }

    return [ ("pubnamed", args) ]
