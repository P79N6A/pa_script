#coding:utf-8
#
# why i'm cofusing, what i'm worrying about? why i'm not happy?
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
from PA.InfraLib.Utils import PList
from PA.InfraLib.Extensions import PlistHelper

import datetime
import model_im
import os
import logging
import re
import unity_c37r
import json
import random
import traceback

TW_SYS_DB_VERSION = [49, 55, 58]

def parse_statuses(obj):
    if obj is None:
        return ""
    header = int(obj[1]) << 8 | int(obj[0])
    if header != 0x34A:
        return ""
    length = len(obj)
    tp = obj[2]
    slen = 0
    tlen = 0
    if tp == 0x42 or tp == 0x6a:
        slen = int(obj[3])
    elif tp == 0x43 or tp == 0x6b:
        slen = int(obj[3]) << 8 | int(obj[4])
        tlen = 2
    else:
        print('illegal blob detected!')
    if slen > length:
        return ""
    bt = obj[3 + tlen: length]
    s = bt.decode('utf-8', 'ignore')
    s = s[:slen]
    if s is None:
        print('decode failed!')
        return ""
    return s

def parse_messages(obj):
    if obj is None:
        return ""
    header = int(obj[1] << 8) | int(obj[0])
    if header != 0x1d49:
        print('header is wrong!')
        return ""
    tlen = obj[12]
    idx = 12 + tlen + 3 + 2
    return parse_statuses(obj[idx : len(obj)])

def parse_description(obj):
    if(type(obj) is int):
        return ""
    return parse_statuses(obj)

AND_TWR_VERSION = 1

class Twitter(object):
    def __init__(self, node, extract_source, extract_deleted, is_scripts = True):
        self.node = node
        self.extract_source = extract_source
        self.extract_deleted = extract_deleted
        self.account_list = list()
        self.account_dbs = dict()
        self.im = model_im.IM()
        self.cache = ''
        if is_scripts:
            self.cache = ds.OpenCachePath('Twitter')
        else:
            self.cache = "D:/Cache"
        self.hash = unity_c37r.md5(self.node.AbsolutePath)
        self.cache_db = os.path.join(self.cache, self.hash)
        self.need_parse = model_im.IM.need_parse(self.cache_db, AND_TWR_VERSION)
        if self.need_parse:
            self.im.db_create(self.cache_db)

    def search(self):
        db_path = self.node.GetByPath('databases')
        fl = os.listdir(db_path.PathWithMountPoint)
        re_string = '(.*)-{}\\.db'
        for f in fl:
            grp = None
            for v in TW_SYS_DB_VERSION:
                grp = re.search(re_string.format(v), f, re.I | re.M)
                if grp is not None:
                    break
            if grp is None:
                continue
            try:
                print grp.group(0)
                n = int(grp.group(1))
                if n == 0:
                    continue
                if self.account_list.__contains__(n):
                    continue
                self.account_list.append(n)
                self.account_dbs[n] = grp.group(0)
            except:
                pass
        for aid in self.account_list:
            print aid

    def parse(self, aid):
        db_node = self.node.GetByPath('databases/{}'.format(self.account_dbs[aid]))
        if db_node is None:
            return
        conn = unity_c37r.create_connection_tentatively(db_node)
        cmd = sql.SQLiteCommand(conn)
        f_dict = dict()
        cmd.CommandText = '''
            select user_id, username, name, description, web_url, image_url from users where user_id = {}
        '''.format(aid)
        reader = cmd.ExecuteReader()
        if reader.Read():
            a = model_im.Account()
            a.account_id = unity_c37r.c_sharp_get_long(reader, 0)
            a.nickname = unity_c37r.c_sharp_get_string(reader, 2)
            a.username = unity_c37r.c_sharp_get_string(reader, 1)
            a.signature = parse_description(unity_c37r.c_sharp_get_blob(reader, 3))
            a.photo = unity_c37r.c_sharp_get_string(reader, 5)
            f_dict[a.account_id] = a
            self.im.db_insert_table_account(a)
        reader.Close()
        cmd.CommandText = '''
            select user_id, username, name, description, web_url, image_url from users where user_id != {}
        '''.format(aid)
        reader = cmd.ExecuteReader()
        while reader.Read():
            f = model_im.Friend()
            f.account_id = aid
            f.friend_id = unity_c37r.c_sharp_get_long(reader, 0)
            f.nickname = unity_c37r.c_sharp_get_string(reader, 2)
            f.remark = unity_c37r.c_sharp_get_string(reader, 1)
            f.signature = parse_description(unity_c37r.c_sharp_get_blob(reader, 3))
            f.photo = unity_c37r.c_sharp_get_string(reader, 5)
            f_dict[f.account_id] = f
            self.im.db_insert_table_friend(f)
        reader.Close()
        cmd.CommandText = '''
            select status_id, author_id, content, created, favorite_count, retweet_count from statuses
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            feed = model_im.Feed()
            feed.account_id = aid
            feed.likecount = unity_c37r.c_sharp_get_long(reader, 4)
            feed.rtcount = unity_c37r.c_sharp_get_long(reader, 5)
            feed.content = parse_statuses(unity_c37r.c_sharp_get_blob(reader, 2))
            feed.sender_id = unity_c37r.c_sharp_get_long(reader, 1)
            feed.send_time = unity_c37r.c_sharp_get_long(reader, 3) / 1000
            self.im.db_insert_table_feed(feed)
        reader.Close()
        cmd.CommandText = '''
            select  user_id, created, data, conversation_id from conversation_entries
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            cid = unity_c37r.c_sharp_get_string(reader, 3)
            uids = cid.split('-')
            obj_id = None
            if uids[0] == str(aid):
                obj_id = uids[1]
            else:
                obj_id = uids[0]
            m = model_im.Message()
            m.account_id = aid
            m.send_time = unity_c37r.c_sharp_get_long(reader, 1) / 1000
            m.content = parse_messages(unity_c37r.c_sharp_get_blob(reader, 2))
            m.talker_id = obj_id
            m.send_id = unity_c37r.c_sharp_get_long(reader, 0)
            if f_dict.__contains__(int(obj_id)):
                m.talker_name = f_dict[int(obj_id)].nickname
            self.im.db_insert_table_message(m)
        self.im.db_commit()


def parse_android_twr(node, es, ed):
    t = Twitter(node, es, ed)
    try:
        if t.need_parse:
            t.search()
            for aid in t.account_list:
                t.parse(aid)
            t.im.db_insert_table_version(model_im.VERSION_KEY_DB, model_im.VERSION_VALUE_DB)
            t.im.db_insert_table_version(model_im.VERSION_KEY_APP, AND_TWR_VERSION)
            t.im.db_commit()
            t.im.db_close()
        models = model_im.GenerateModel(t.cache_db).get_models()
        mlm = ModelListMerger()
        pr = ParserResults()
        pr.Categories = DescripCategories.Twitter
        pr.Models.AddRange(list(mlm.GetUnique(models)))
        pr.Build("Twitter")
    except:
        pr = ParserResults()
        return pr
#
# IPY 脚本入口
#
if __name__ == '__main__':
    node = FileSystem.FromLocalDir(r'E:\electron\twitter\com.twitter.android')
    try:
        t = Twitter(node, 1, 1, False)
        if t.need_parse:
            t.search()
            for aid in t.account_list:
                t.parse(aid)
            t.im.db_insert_table_version(model_im.VERSION_KEY_DB, model_im.VERSION_VALUE_DB)
            t.im.db_insert_table_version(model_im.VERSION_KEY_APP, AND_TWR_VERSION)
            t.im.db_commit()
            t.im.db_close()
    except:
        traceback.print_exc()

    