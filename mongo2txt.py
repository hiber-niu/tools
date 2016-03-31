# -*-: coding:utf-8 -*-
'''
This program is used to read mongodb and store them to txt.

date: 2016/03/30 周三
author: hiber.niu@gmail.com
'''

from pymongo import MongoClient
import codecs
import os


class MongoUtil():
    def __init__(self, database, collection, host='localhost', port=27017):
        client = MongoClient(host, port)
        db = client[database]
        self.coll = db[collection]

    def find(self, query=None):
        '''
        query is certain dict like this:
            {"birthday":{"$lt":new Date("1990/01/01")}}
        '''
        return self.coll.find(query)


def get_detail_content_category():
    contents = []
    counts = {'0':0, '1':0, '2':0, '3':0, '4':0, '5':0}

    results = MongoUtil('cn_classify', 'space', '192.168.200.10').find()
    for row in results:
        contents.append(row['content'])
        category = row['category']
        category = category[1:]
        if category == u'战略综合':
            counts['0'] = counts['0']+1
            fn = u'./战略综合/'
            if not os.path.exists(fn):
                os.makedirs(fn)
            fn = u'./战略综合/'+str(counts['0'])+'.txt'
            with codecs.open(fn, 'w', 'utf-8') as fid:
                fid.write(row['content'])

        elif category == u'进入空间':
            counts['1'] = counts['1']+1
            fn = u'./进入空间/'
            if not os.path.exists(fn):
                os.makedirs(fn)
            fn = u'./进入空间/'+str(counts['1'])+'.txt'
            with codecs.open(fn, 'w', 'utf-8') as fid:
                fid.write(row['content'])

        elif category == u'利用空间':
            counts['2'] = counts['2']+1
            fn = u'./利用空间/'
            if not os.path.exists(fn):
                os.makedirs(fn)
            fn = u'./利用空间/'+str(counts['2'])+'.txt'
            with codecs.open(fn, 'w', 'utf-8') as fid:
                fid.write(row['content'])

        elif category == u'控制空间':
            counts['3'] = counts['3']+1
            fn = u'./控制空间/'
            if not os.path.exists(fn):
                os.makedirs(fn)
            fn = u'./控制空间/'+str(counts['3'])+'.txt'
            with codecs.open(fn, 'w', 'utf-8') as fid:
                fid.write(row['content'])

        elif category == u'载人航天':
            counts['4'] = counts['4']+1
            fn = u'./载人航天/'
            if not os.path.exists(fn):
                os.makedirs(fn)
            fn = u'./载人航天/'+str(counts['4'])+'.txt'
            with codecs.open(fn, 'w', 'utf-8') as fid:
                fid.write(row['content'])

        else:  #'前沿技术':
            counts['5'] = counts['5']+1
            fn = u'./前沿技术/'
            if not os.path.exists(fn):
                os.makedirs(fn)
            fn = u'./前沿技术/'+str(counts['5'])+'.txt'

            with codecs.open(fn, 'w', 'utf-8') as fid:
                fid.write(row['content'])

    return contents


if __name__ == '__main__':
    get_detail_content_category()
