
import sys
import lxml.html           
import requests
import socket

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
        return None
    except requests.exceptions.Timeout:
        return None
    try:
        root = lxml.html.fromstring(html)
    except ValueError:
        return None

    scrape_schemes = [ csv_links, csv_text_links ]

    for ss in scrape_schemes:
        links = [ l for l in ss(root) ]
        if len(links) > 0:
            return ss(root)

    return none(root)
