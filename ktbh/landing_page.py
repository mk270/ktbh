
import sys
import lxml.html           
import requests
import socket
import json
import urlparse

class ParseUnretrievable(Exception): pass

def link_data(root):
    for a in root.cssselect("a"):
        text = a.text
        href = a.get("href")

        if href is None:
            continue
        yield (text, href)

def csv_links(root):
    for text, href in link_data(root):
        if "csv" in href:
            yield (text, href)

def csv_text_links(root):
    for text, href in link_data(root):
        if text is None:
            continue
        if "csv" in text.lower():
            yield (text, href)

def none(root):
    if False:
        yield ("","")

def scrape(url):
    url = url.strip()
    try:
        html = requests.get(url, timeout=5.0).text
    except socket.timeout:
        raise ParseUnretrievable
    except requests.exceptions.Timeout:
        raise ParseUnretrievable
    try:
        root = lxml.html.fromstring(html)
    except ValueError:
        raise ParseUnretrievable

    scrape_schemes = [ csv_links, csv_text_links ]

    for ss in scrape_schemes:
        links = [ l for l in ss(root) ]
        if len(links) > 0:
            return ss(root)

    return none(root)

def examine_landing_page_callback(body):
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
            return ("url", payload)

        results = [ collect_links(text, href) 
                    for text, href in scrape(url) ]
        if len(results) > 0:
            return results
    except:
        pass
    return [ ("broken", {"body": body}) ]
