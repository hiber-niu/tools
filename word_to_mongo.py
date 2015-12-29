# -*-: coding:utf-8 -*-
'''
将四室的有关太空的word文章，按照xml格式提取其类别、标题、正文，并将结果存储到
mongodb中。
This program is used to read word file and store them to mongodb.

date: 2015/12/23 周三
author: hiber.niu@gmail.com
'''

from pymongo import MongoClient
import zipfile
from lxml import etree
import os


class MsWordReader():
    def get_word_xml(self, docx_filename):
        zip = zipfile.ZipFile(docx_filename)
        xml_content = zip.read('word/document.xml')
        return xml_content

    def get_xml_tree(self, xml_string):
        return etree.fromstring(xml_string)

    def itertext(self, my_etree):
        """Iterator to go through xml tree's text nodes"""
        for node in my_etree.iter(tag=etree.Element):
            if self.check_element_is(node, 'p'):
                yield (node, node.text)

    def check_element_is(self, element, type_char):
        word_schema = 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'
        return element.tag == '{%s}%s' % (word_schema, type_char)


def format_word(file_dir):
    mswr = MsWordReader()
    for root, dirs, files in os.walk(file_dir):
        for f in files:
            word_file = os.path.join(root, f)
            xml_from_file = mswr.get_word_xml(word_file)
            xml_tree = mswr.get_xml_tree(xml_from_file)

            doc = []
            article = {'category':'', 'title':'', 'content':''}
            begin_tag = False
            for node, txt in mswr.itertext(xml_tree):
                p_tag = 'content'
                for all_tags in node.findall('.//'):
                    if mswr.check_element_is(all_tags, 'pStyle'):
                        if all_tags.values()[0] == '1':  # get category
                            # store previous article
                            if article['content'] != '':
                                article['filename'] = f.decode('gb2312')
                                doc.append(article.copy())

                            p_tag = 'category'   # article belongs to which category
                            begin_tag = True   # indicate that begin read text
                            article = {'category':'', 'title':'', 'content':''}

                        elif all_tags.values()[0] == '2':  # get title
                            # store previous article
                            if article['content'] != '':
                                article['filename'] = f.decode('gb2312')
                                doc.append(article.copy())

                            p_tag = 'title'  # article title
                            article['title'] = ''
                            article['content'] = ''

                    if begin_tag and all_tags.text:
                        article[p_tag] = article[p_tag]+all_tags.text

            article['filename'] = f.decode('gb2312')
            doc.append(article.copy())
            for art in doc:
                for key in art.keys():
                    art[key] = art[key].encode('utf8')
            yield doc


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



for document in format_word(r'd:\workspace\cnnlp\2015'):
    article_to_mongodb('cn_classify', 'space', document,
                       '192.168.200.10')
