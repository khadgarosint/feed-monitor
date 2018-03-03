import feedparser
import settings
import rethinkdb as r
from time import sleep
import os

r.connect(os.environ.get('RETHINK_HOST', 'localhost'), int(os.environ.get('RETHINK_PORT', 28015))).repl()

while True:
    for f in settings.FEED_LIST:
        try:
            feed = feedparser.parse(f)

            for entry in feed['entries']:
                terms = []
                for t in settings.TERMS:
                    if t.lower() in entry['summary']:
                        terms.append(t)
                if terms:
                    d = {'external_id': entry['id'], 'agent': 'feed-monitor', 'source': feed['feed']['title'],
                         'text': entry['summary'], 'type': 'news', 'sub_type': 'term-match',
                         'date': entry['published'], 'url': entry['link'], 'summary': entry['summary'], 'terms': terms,
                         'metadata': {},
                         'tags': [], 'title': entry['title'], 'length': 0, 'author': None}

                    if not r.db('khadgar').table('url').filter({'external_id': entry['id']}).count().run():

                        r.db('khadgar').table('url').insert(d, conflict='update').run()
        except:
            continue

    sleep(10)
