#coding:utf-8
#
# 悦来佛祖 释迦摩敬 乔达敬·悉达赵 玉皇大敬 太上老赵 太白星敬 托塔天敬 
# 二敬神 哮天敬 齐天大敬 东海龙敬 如意金箍敬 观敬菩萨 唐敬 白龙敬 沙悟敬 猪八敬 孙悟敬
#
import clr
clr.AddReference('System.Data.SQLite')
clr.AddReference('Base3264-UrlEncoder')
try:
    clr.AddReference('model_im')
    clr.AddReference('unity_c37r')
except:
    pass
del clr

import System.Data.SQLite as sql
from PA_runtime import *
from System.Text import *
from System.IO import *
from System.Security.Cryptography import *
from System import Convert

import datetime
import model_im
import os
import logging
import re
import unity_c37r
import json

class xianyu(object):

    def __init__(self, node, extract_deleted, extract_source):
        self.node = node
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.cache = ds.OpenCachePath('xianyu')
        self.im = model_im.IM()
        self.im.db_create(self.cache + '/C37R')
        self.account = None

    def search(self):
        if self.node is None:
            print('fucked')
            return
        cache_node = self.node.GetByPath('/Library/Caches')
        cache_path = cache_node.PathWithMountPoint
        d_l = os.listdir(cache_path)
        for d in d_l:#__xstore_user_1033848829.db
            r = re.search('__xstore_user_(.*).db$', d, re.I | re.M)
            print d
            if r is None:
                continue
            else:
                if int(r.group(1)) == 0:
                    continue
            self.account = int(r.group(1))
            break
        if self.account is None:
            print('find failed!')
        # parse...
    def parse(self):
        app_node = self.node.GetByPath('Library/Caches/__xstore_app.db')
        if app_node is None:
            print('fucked')
            return
        conn = unity_c37r.create_connection(app_node.PathWithMountPoint)
        cmd = sql.SQLiteCommand(conn)
        cmd.CommandText = '''
            select nick, logo, gender from PUserInfo where userId = {}
        '''.format(self.account)
        reader = cmd.ExecuteReader()
        if reader.Read():
            a = model_im.Account()
            a.account_id = self.account
            a.nickname = unity_c37r.c_sharp_get_string(reader, 0)
            a.photo = unity_c37r.c_sharp_get_string(reader, 1)
            self.im.db_insert_table_account(a)
        cmd.Dispose()
        cmd.CommandText = '''
            select userId, nick, logo, gender from PUserInfo where userId != {}
        '''.format(self.account)
        reader = cmd.ExecuteReader()
        while reader.Read():
            if canceller.IsCancellationRequested:
                cmd.Dispose()
                conn.Close()
                self.im.db_close()
                raise IOError('e')
            f = model_im.Friend()
            f.account_id = self.account
            f.friend_id = unity_c37r.c_sharp_get_long(reader, 0)
            f.nickname = unity_c37r.c_sharp_get_string(reader, 1)
            f.photo = unity_c37r.c_sharp_get_string(reader, 2)
            self.im.db_insert_table_friend(f)
        cmd.Dispose()
        cmd.CommandText = '''
            select pondId, adminUserId, pondName, pondLogo from PPondInfo
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            if canceller.IsCancellationRequested:
                cmd.Dispose()
                conn.Close()
                self.im.db_close()
                raise IOError('e')
            g = model_im.Chatroom()
            g.account_id = self.account
            g.chatroom_id = unity_c37r.c_sharp_get_long(reader, 0)
            g.name = unity_c37r.c_sharp_get_string(reader, 2)
            g.owner_id = unity_c37r.c_sharp_get_long(reader, 1)
            g.photo = unity_c37r.c_sharp_get_string(reader, 3)
            self.im.db_insert_table_chatroom(g)
        cmd.Dispose()
        cmd.CommandText = '''
            select * from PItemInfo
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            if canceller.IsCancellationRequested:
                cmd.Dispose()
                conn.Close()
                self.im.db_close()
                raise IOError('e')
            feed = model_im.Feed()
            feed.account_id = self.account
            feed.attachment_link = unity_c37r.c_sharp_get_string(reader, 1)
            feed.attachment_title = unity_c37r.c_sharp_get_string(reader, 4)
            #feed.attachment_desc
            feed.attachment_desc = '''price:{}\norigPrice:\n'''.format(unity_c37r.c_sharp_get_string(reader, 2), unity_c37r.c_sharp_get_string(reader, 3))
            seller_id = unity_c37r.c_sharp_get_long(reader, 5)
            seller_info = unity_c37r.c_sharp_get_long(reader, 6)
            feed.sender_id = seller_id if seller_id != 0 else seller_info
            self.im.db_insert_table_feed(feed)
        cmd.Dispose()
        conn.Close()
        db_node = self.node.GetByPath('Library/Caches/__xstore_user_{}.db'.format(self.account))
        if db_node is None:
            print('fucked')
            return
        conn = unity_c37r.create_connection(db_node.PathWithMountPoint)
        cmd = sql.SQLiteCommand(conn)
        cmd.CommandText = '''
            select Sid, Uid, content, extJson, timeStamp from PMessage
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            if canceller.IsCancellationRequested:
                cmd.Dispose()
                conn.Close()
                self.im.db_close()
                raise IOError('e')
            m = model_im.Message()
            m.account_id = self.account
            m.sender_id = unity_c37r.c_sharp_get_long(reader, 1)
            m.talker_id = unity_c37r.c_sharp_get_long(reader, 0)
            m.send_time = unity_c37r.c_sharp_get_long(reader, 4) / 1000
            try:
                string = unity_c37r.c_sharp_get_string(reader, 2)
                js = json.loads(string)
                tp = js.get('contentType')
                if tp == 1:
                    m.content = js.get('text').get('text')
                    m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                elif tp == 17: # trade...
                    #m.content = js.get('content')
                    m.content = js.get('title')
                    deal = model_im.Deal()
                    m.type = model_im.MESSAGE_CONTENT_TYPE_RECEIPT
                    m.extra_id = deal.deal_id
                    deal.money = js.get('content')
                    deal.description = js.get('title')
                    self.im.db_insert_table_deal(deal)
                elif tp == 10:
                    m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    js = js.get('actionCard')
                    m.content = '''title:{}\ncontent:{}'''.format(js.get('memo'), js.get('title'))
                elif tp == 2: # image
                    m.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                    m.media_path = js.get('image').get('pics')[0].get('url')
                elif tp == 8:
                    m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    m.content = '''title:{}\ncontent:{}'''.format(js.get('imageCard').get('title'), js.get('imageCard').get('content'))
                elif tp == 6:
                    m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    #m.content = '''title:{}\ncontent:{}\nstring:{}'''.format(js.get('textCard').get('title'), js.get('textCard').get('memo'), string)
                    js = js.get('textCard')
                    title = js.get('title')
                    # if title is None:
                    #     m.content = '''content:{}'''.format(js.get('content'))
                    # else:
                    #     m.content = '''title:{}\ncontent:{}\nstring:{}'''.format(js.get('title'), js.get('memo'), string)
                    content = js.get('content')
                    memo = js.get('memo')
                    m.content = '''title:{}\ncontent:{}\nmemo:{}'''.format(title, content, memo)
                elif tp == 16:
                    m.type = model_im.MESSAGE_CONTENT_TYPE_ATTACHMENT
                    m.content = js.get('itemCard').get('title')
                    deal = model_im.Deal()
                    js = js.get('itemCard').get('item')
                    deal.deal_id = js.get('itemId')
                    m.extra_id = deal.deal_id
                    m.media_path = js.get('mainPic')
                    deal.description = js.get('title')
                    deal.money = js.get('price')
                    self.im.db_insert_table_deal(deal)
                elif tp == 11: # 留言
                    m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    m.content = '''title:{}\ncontent:{}'''.format(js.get('reply').get('title'), js.get('reply').get('content'))
                    # url is lost... waiting for support.
                elif tp == 20:
                    m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    m.content = js.get('text')
                elif tp == 7:
                    m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    m.content = js.get('itemCard').get('title')
                elif tp == 14:
                    m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    m.content = js.get('tip').get('tips')
                else:
                    print('detect unsupported type:%d' % tp)
                    m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    m.content = string
                self.im.db_insert_table_message(m)
            except Exception as e:
                print 'find error string:{}'.format(string)
                logging.error(e)
        self.im.db_commit()
        cmd.Dispose()
        cmd.CommandText = '''
            select XSummary, Session$$$$sessionId , ts, Sender from XMessageCenterItem
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            if canceller.IsCancellationRequested:
                cmd.Dispose()
                conn.Close()
                self.im.db_close()
                raise IOError('e')
            m = model_im.Message()
            m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
            m.content = unity_c37r.c_sharp_get_string(reader, 0)
            m.talker_id = unity_c37r.c_sharp_get_long(reader, 1)
            m.sender_id = unity_c37r.c_sharp_get_long(reader, 3)
            m.send_time = unity_c37r.c_sharp_get_long(reader, 2) / 1000
            self.im.db_insert_table_message(m)
        self.im.db_commit()
        cmd.Dispose()
        conn.Close()

def parse_xy(root, extract_deleted, extract_source):
    node = root
    #node = FileSystem.FromLocalDir(r'D:\ios_case\xianyu\C533806C-8FB4-459D-8127-B1BA7A345E28')
    try:
        x = xianyu(node, extract_deleted, extract_deleted)
        x.search()
        x.parse()
        models = model_im.GenerateModel(x.cache + '/C37R').get_models()
        mlm = ModelListMerger()
        pr = ParserResults()
        pr.Categories = DescripCategories.QQ
        pr.Models.AddRange(list(mlm.GetUnique(models)))
        pr.Build('闲鱼')
    except:
        return ParserResults()
    return pr