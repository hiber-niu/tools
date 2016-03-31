# -*- coding: utf-8 -*-
'''
Read sqllite file and store them to mysql.

date: 2016/03/22 周二
author: hiber_niu@163.com
'''

import sqlite3
import traceback
from lxml import html
from lxml.html.clean import Cleaner
from pymongo import MongoClient


cleaner = Cleaner()
cleaner.javascript = True
cleaner.style = True
min_length = 10


def parse_texts(content):
    try:
        tree = html.fromstring(cleaner.clean_html(content))
        return ''.join(get_text_from_tree(tree))
    except:
        return ''


def get_text_from_tree(tree):
    text = tree.text_content()
    for t in text.split('\n'):
        t = t.strip(u'\r\n \t ')
        if t and len(t) >= min_length:
            if u'. ' in t or u'。' in t or t.endswith(u'.'):
                yield t


def read_table(conn):
    c = conn.cursor()
    for row in c.execute('SELECT * FROM Content ORDER BY id'):
        yield {'title': row[3], 'content': row[4], 'pdate': row[6],
               'source': row[7], 'url': row[8]}


def article_to_mongodb(database, collection, articles,
                       host='localhost', port=27017):
    '''
    # store stock information into database.collection.
    # jsonData is stock information
    '''
    client = MongoClient(host, port)
    db = client[database]
    coll = db[collection]
    coll.insert_many(articles)


def handle(sqlite_file):
    conn = sqlite3.connect(sqlite_file)
    count = 0
    articles = []
    for row in read_table(conn):
        if count == 0:
            print('== %s ==' % row['source'])
        row['text'] = parse_texts(row['content'])
        count = count + 1
        articles.append(row)
        if(count % 200) == 0:
            article_to_mongodb('cn_classify', 'dsti', articles,
                               '192.168.200.10')
            articles = []

    print('There are %d files stored' % count)
    article_to_mongodb('cn_classify', 'dsti', articles,
                       '192.168.200.10')

    conn.close()


'''
def handle1(sqlite_file):
    conn = sqlite3.connect(sqlite_file)
    count = 0
    articles = []
    for row in read_table(conn):
        if count == 0:
            print('== %s ==' % row['source'])
        row['text'] = parse_texts(row['content'])
        count = count + 1
        articles.append(row)
        if count == 200:
            article_to_mongodb('cn_classify', 'test', articles,
                               '192.168.200.10')
            articles = []
            break;

    print('There are %d files stored' % count)
    conn.close()
'''


if __name__ == '__main__':
    import glob
    db_list = glob.glob('d:\\*.db3')
    for db in db_list:
        print('read and store %s' % db)
        # handle1(db)
        handle(db)
