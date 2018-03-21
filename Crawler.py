# -*- coding: utf-8 -*-
import urllib
from bs4 import BeautifulSoup
import urllib.request as request
import re
import sqlite3
import os
from datetime import datetime

class Crawler(object):
    def __init__(self, db_path, use_proxies=False):
        self.con = sqlite3.connect(db_path)
        self.cursor = self.con.cursor()
        self.ignore_words = {'the', 'of', 'to', 'and', 'a', 'in', 'is', 'it'}
        self.headers = {
            'user-agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
        }
        proxies = {
            'https': 'https://127.0.0.1:1087',
            'http': 'http://127.0.0.1:1087'
        }
        self.opener = request.build_opener(request.ProxyHandler(
            proxies)) if use_proxies else request.build_opener()
        request.install_opener(self.opener)

    def __del__(self):
        self.cursor.close()
        self.con.close()

    def db_commit(self):
        self.con.commit()

    def creat_db_tables(self):
        self.cursor.execute(
            'create table url_list(id integer primary key autoincrement, \
                                   url varchar(200) not null)')
        self.cursor.execute(
            'create table word_list(id integer primary key autoincrement, \
                                    word varchar(50) not null)')
        self.cursor.execute(
            'create table word_location(id integer primary key autoincrement, \
                                        url_id integer,\
                                        word_id integer,\
                                        location integer)')
        self.cursor.execute(
            'create table link(id integer primary key autoincrement,\
                               from_id integer,\
                               to_id integer)')
        self.cursor.execute(
            'create table link_words(id integer primary key autoincrement,\
                                     word_id integer,\
                                     link_id integer)')
        self.cursor.execute('create index word_idx on word_list(word)')
        self.cursor.execute('create index url_idx on url_list(url)')
        self.cursor.execute(
            'create index word_url_idx on word_location(word_id)')
        self.cursor.execute('create index url_to_idx on link(to_id)')
        self.cursor.execute('create index url_from_idx on link(from_id)')
        self.db_commit()

    def add_to_index(self, url, soup):
        if self.has_indexed(url):
            return
        # print('Indexing %s' % url)
        # 获取每个单词
        text = self.get_text_only(soup)
        words = self.separate_words(text)
        # 得到URL的id
        url_id = self.get_entry_id('url_list', 'url', url)
        # 将每个单词与该url关联
        for i in range(len(words)):
            word = words[i]
            if word in self.ignore_words:
                continue
            word_id = self.get_entry_id('word_list', 'word', word)
            self.cursor.execute(
                "insert into word_location(url_id,word_id,location) values (?,?,?)",
                (url_id, word_id, i))

    def get_text_only(self, soup):
        v = soup.string
        if v == None:
            c = soup.contents
            if c != []:
                resulttext = ''
                for t in c:
                    subtext = self.get_text_only(t)
                    resulttext += subtext + '\n'
                return resulttext
            else:
                return ''
        else:
            return v.strip()

    def separate_words(self, text):
        return [s.lower() for s in re.split(r'\W*', text) if s != '']

    def has_indexed(self, url):
        u = self.cursor.execute("select id from url_list where url=?",
                                (url, )).fetchone()
        if u != None:
            v = self.cursor.execute(
                "select * from word_location where url_id=?",
                (u[0], )).fetchone()
            if v != None:
                return True
        return False

    def get_entry_id(self, table, field, value, create_new=True):
        self.cursor.execute("select rowid from {} where {}=?".format(
            table, field), (value, ))
        res = self.cursor.fetchone()
        if res == None:
            self.cursor.execute("insert into {}({}) values(?)".format(
                table, field), (value, ))
            return self.cursor.lastrowid
        else:
            return res[0]

    def add_link_ref(self, urlFrom, urlTo, linkText):
        words = self.separate_words(linkText)
        from_id = self.get_entry_id('url_list', 'url', urlFrom)
        to_id = self.get_entry_id('url_list', 'url', urlTo)
        if from_id == to_id:
            return
        cur = self.cursor.execute(
            "insert into link(from_id,to_id) values (?,?)", (from_id, to_id))
        link_id = cur.lastrowid
        for word in words:
            if word in self.ignore_words:
                continue
            word_id = self.get_entry_id('word_list', 'word', word)
            self.cursor.execute(
                "insert into link_words(link_id,word_id) values (?,?)",
                (link_id, word_id))

    def crawl(self, init_url, depth=1):
        pages = [init_url]
        for i in range(depth):
            new_layer_pages = set()
            print('-----------layer %s has %s pages-----------' % (i,
                                                                   len(pages)))
            for j, page in enumerate(pages):
                try:
                    req = request.Request(page, headers=self.headers)
                    c = request.urlopen(req)
                except Exception as e:
                    print('could not open %s\nBecause:' % page, e)
                    continue
                soup = BeautifulSoup(c.read(), 'lxml')
                self.add_to_index(page, soup)

                print('has indexed %3.2f %%:' % ((j + 1) / len(pages) * 100),
                      page)

                links = soup('a')
                for k, link in enumerate(links):
                    if 'href' in link.attrs:
                        url = urllib.parse.urljoin(page,
                                                   link['href']).split('#')[0]
                        country = re.search(r'(\w{2}).wikipedia.org', url)
                        if country == None or (country != None and country.group(1) != 'en'
                            ) or url.find("'") != -1:
                            continue
                        if i<2 and url[:4] == 'http' and not self.has_indexed(url):
                            new_layer_pages.add(url)
                        linkText = self.get_text_only(link)
                        self.add_link_ref(page, url, linkText)
                self.db_commit()
            pages = new_layer_pages
        self.db_commit()
        return