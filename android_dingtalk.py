#coding:utf-8
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
clr.AddReference('Base3264-UrlEncoder')
del clr
import System.Data.SQLite as sql
import PA
from PA_runtime import *
from System.Text import *
from System.IO import *
from System.Security.Cryptography import *
from System import Convert
from MhanoHarkness import *

import model_im
import hashlib
import base64
import os
import sys
import re
import json

#####################Get Functions######################################
def GetString(reader, idx):
    return reader.GetString(idx) if not reader.IsDBNull(idx) else ""

def GetInt64(reader, idx):
    return reader.GetInt64(idx) if not reader.IsDBNull(idx) else 0

def GetBlob(reader, idx):
    return reader.GetValue(idx) if not reader.IsDBNull(idx) else None

def md5(string):
    return hashlib.md5(string).hexdigest()

class DingA(object):
    def __init__(self, fs, extract_deleted, extract_source):
        self.fs = fs
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.im = model_im.IM()
        cache = ds.OpenCachePath('Dingtalk')
        self.im.db_create(cache + '/C37R')
        self.cache = cache
        self.account = list()
    
    @staticmethod
    def m_print(msg):
        print('[钉钉]:%s' %msg)

    @staticmethod
    def aes_decrypt(src, dst, key):
        rm = RijndaelManaged()
        rm.Key = Convert.FromBase64String(base64.b64encode(key))
        rm.Mode = CipherMode.ECB
        rm.Padding = PaddingMode.None
        tr = rm.CreateDecryptor()
        f = open(dst, 'wb')
        data = src.Data
        sz = src.Size
        idx = 0
        while idx < sz:
            bts = data.read(16)
            bts = Convert.FromBase64String(base64.b64encode(bts))
            t_r = tr.TransformFinalBlock(bts, 0, 16)
            f.write(t_r)
            idx += 16
        f.close()
    
    @staticmethod
    def check_key(key, src):
        rm = RijndaelManaged()
        rm.Key = Convert.FromBase64String(base64.b64encode(key))
        rm.Mode = CipherMode.ECB
        rm.Padding = PaddingMode.None
        tr = rm.CreateDecryptor()
        data = src.Data
        data.seek(0)
        bts = data.read(16)
        bts = Convert.FromBase64String(base64.b64encode(bts))
        t_r = tr.TransformFinalBlock(bts, 0, 16)
        name = str(bytes(t_r))
        print name
        if name != "SQLite format 3\0":
            return False
        return True

    def search_account(self):
        db_dir_node = self.fs.GetByPath('databases')
        abs_path = db_dir_node.PathWithMountPoint
        l = os.listdir(abs_path)
        for i in l:
            try:
                r = re.search('(.*)\\.db$', i, re.I | re.M)
                if r is None:
                    continue
                res = int(r.group(1))
                if res <= 0 or self.account.__contains__(i):
                    continue
                self.account.append(res)
            except:
                continue
        self.m_print('total find {} accounts'.format(len(self.account)))
        model = "HUAWEI NXT-AL10"
        board = 'NXT-AL10'
        hw = 'hi3650'
        device = 'HWNXT'
        cpu_abi = [ "arm64-v8a", "armeabi-v7a", "armeabi"]
        right_key = None
        for abi in cpu_abi:
            key_base = '{}/{}/{}/{}/{}'.format(model, abi, board, hw, device)
            key = md5(key_base)[0:16]
            r = self.check_key(key, self.fs.GetByPath('databases/0.db'))
            if r:
                right_key = key
                break
        if right_key is None:
            raise IOError('''can't find correct key! parse exits!''')
        for aid in self.account:
            db_node = self.fs.GetByPath('databases/{}.db'.format(aid))
            #db_wal_node = self.fs.GetByPath('databases/{}.db-wal'.format(aid))
            #db_shm_node = self.fs.GetByPath('databases/{}.db-shm'.format(aid))
            if db_node is None:
                continue
            self.aes_decrypt(db_node, self.cache + '/{}.db'.format(aid), right_key)
            #if db_wal_node is not None:
                #self.aes_decrypt(db_wal_node, self.cache + '/{}.db-wal'.format(aid), right_key)
            #if db_shm_node is not None:
                #self.aes_decrypt(db_shm_node, self.cache + '/{}.db-shm'.format(aid), right_key)
            
            chat_node = self.fs.GetByPath('databases/{}.db'.format(md5('{}@dingding'.format(aid))))
            #chat_wal_node = self.fs.GetByPath('databases/{}.db-wal'.format(md5('{}@dingding'.format(aid))))
            #chat_shm_node = self.fs.GetByPath('databases/{}.db-shm'.format(md5('{}@dingding'.format(aid))))
            if chat_node is None:
                continue
            self.aes_decrypt(chat_node, self.cache + '/{}_chat.db'.format(aid), right_key)
            #if chat_wal_node is not None:
                #self.aes_decrypt(chat_wal_node, self.cache + '/{}_chat.db-wal'.format(aid), right_key)
            #if chat_shm_node is not None:
                #self.aes_decrypt(chat_shm_node, self.cache + '/{}_chat.db-shm'.format(aid), right_key)
        self.m_print('search account and decrypt done!')
    
    def parse(self, aid):
        files = self.cache + '/{}.db'.format(aid)
        if not os.path.exists(self.cache + '/{}.db'.format(aid)):
            self.m_print('no sql file,parse exit!')
            return
        conn = sql.SQLiteConnection('Data Source = {}; Readonly = True'.format(self.cache + '/{}.db'.format(aid)))
        conn.Open()
        cmd = sql.SQLiteCommand(conn)
        cmd.CommandText = '''
            select iconMedia, nick, gender, city, mobile, real_name, dingtalkId, email, activeTime
            from tbuser where uid = {}
        '''.format(aid)
        reader = cmd.ExecuteReader()
        reader.Read()
        a = model_im.Account()
        a.account_id = aid
        a.nickname = GetString(reader, 1)
        if GetString(reader, 2) == 'F':
            a.gender = model_im.GENDER_FEMALE
        elif GetString(reader, 2) == 'M':
            a.gender = model_im.GENDER_MALE
        else:
            a.gender = model_im.GENDER_OTHER
        a.address = GetString(reader, 3)
        a.telephone = GetString(reader, 4)
        a.email = GetString(reader, 7)
        a.photo = GetString(reader, 0)
        self.im.db_insert_table_account(a)
        cmd.Dispose()
        cmd.CommandText = '''
            select uid, iconMedia, nick, gender, city, mobile, email, extensation from tbuser 
            where uid != {}
        '''.format(aid)
        reader = cmd.ExecuteReader()
        f_dict = dict()
        while reader.Read():
            f = model_im.Friend()
            f.account_id = aid
            f.friend_id = GetInt64(reader, 0)
            f.photo = GetString(reader, 1)
            gender = GetString(reader, 3)
            if gender == 'F':
                f.gender = model_im.GENDER_FEMALE
            elif gender == 'M':
                f.gender = model_im.GENDER_MALE
            else:
                f.gender = model_im.GENDER_OTHER
            f.nickname = Getstring(reader, 2)
            f.address = GetString(reader, 4)
            f.telephone = GetString(reader, 5)
            f.email = GetString(reader, 6)
            f.source = files
            ext = Getstring(reaser, 7)
            js = json.loads(ext)
            if js.get('ownness') is not None:
                f.signature = js.get('ownness')[0].get('status')
            #self.im.db_insert_table_friend(f)
            f_dict[f.friend_id] = f
        cmd.Dispose()
        # 暂时未加入分组信息
        cmd.CommandText = '''
            select uid, empName, orgName, depName from tb_user_intimacy
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            try:
                #f = model_im.Friend()
                f_dict[GetInt64(reader, 0)].remark = GetString(reader, 2)
            except:
                continue
        for k in f_dict:
            self.im.db_insert_table_friend(f_dict[k])
        cmd.Dispose()
        cmd.CommandText = '''
            select tbdingcontent.content, tbdingcontent.dingId, tbdinglist.senderId,
             tbdinglist.dingCreatedAt, tbdinglist.latestComments 
            from tbdingcontent,tbdinglist where  tbdingcontent.dingId = tbdinglist.dingId
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            feed = model_im.Feed()
            feed.account_id = aid
            feed.content = GetString(reader, 0)
            feed.sender_id = GetInt64(reader, 2)
            feed.send_time = GetInt64(reader, 3) / 1000
            feed.source = files
            string = GetString(reader, 4)
            if string is not '':
                js = json.loads(string)
                for a in js:
                    feed.comments += '{},'.format(a.get('commentId'))
                    fcm = model_im.FeedComment()
                    fcm.comment_id = a.get('commentId')
                    fcm.content = a.get('commentContent').get('text')
                    fcm.sender_id = a.get('senderId')
                    fcm.sender_name = a.get('commenter')
                    fcm.create_time = a.get('createdAt') / 1000
                    fcm.source = files
                    self.im.db_insert_table_feed_comment(fcm)
            self.im.db_insert_table_feed(feed)
        cmd.Dispose()
        conn.Close()
        self.im.db_commit()
        files = self.cache + '/{}_chat.db'.format(aid)
        if not os.path.exists(files):
            self.m_print('no chat sqlite file, parse exists!')
            return
        conn = sql.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(files))
        conn.Open()
        cmd = sql.SQLiteCommand(conn)
        cmd.CommandText('''
            select cid, title , createAt, ownerId, groupIcon from tbconversation where type = 2
        ''')
        reader = cmd.ExecuteReader()
        while reader.Read():
            g = model_im.Chatroom()
            g.account_id = aid
            g.chatroom_id = GetString(reader, 0)
            g.name = GetString(reader, 1)
            g.owner_id = GetInt64(reader, 3)
            g.create_time = GetInt64(reader, 2) /1000
            g.source = files
            self.im.db_insert_table_chatroom(g)
        cmd.Dispose()
        cmd.CommandText = '''
            select tbl_name from sqlite_master where type = 'table' and tbl_name like 'tbmsg_%'
        '''
        reader = cmd.ExecuteReader()
        tbl_list = list()
        while reader.Read():
            tbl_list.append(GetString(reade, 0))
        cmd.Dispose()
        for tbl in tbl_list:
            cmd.CommandText = '''
                select cid, mid, senderId, createdAt, contentType, content from {}
            '''.format(tbl)
            reader = cmd.ExecuteReader()
            while reader.Read():
                msg = model_im.Message()
                msg.account_id = aid
                msg.msg_id = GetInt64(reader, 1)
                msg.talker_id = GetString(reader, 0)
                # talker_name?
                msg.sender_id = GetInt64(reader, 2)
                msg.is_sender = 1 if msg.sender_id == aid else 0
                msg.send_time = GetInt64(reader, 3) / 1000
                tp = GetInt64(reader, 4)
                string = GetString(reader, 5)
                if tp == 1:
                    msg.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    js = json.loads(string)
                    msg.content = js.get('txt')
                elif tp == 2:
                    msg.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                    js = json.loads(string)
                    msg.content = js.get('url')
                elif tp == 501 or tp == 503 or tp == 502:
                    msg.type = model_im.MESSAGE_CONTENT_TYPE_ATTACHMENT
                    msg.content =  js.get('ext').get('f_name')
                elif tp == 901: # redpakcet
                    #msg.type = model_im.message_content_type_   
                    # pass
                    js = json.loads(string)
                    amount = js.get('ext').get('amount')
                    title = js.get('ext').get('congrats')
                    size = js.get('ext').get('size')
                elif tp == 203: # picture...
                    msg.type = model_im.MESSAGE_CONTENT_TYPE_ATTACHMENT
                    js = json.loads(string)
                    msg.content = js.get('f_name')
                elif tp == 1200 or tp == 1203:
                    msg.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    js = json.loads(string)
                    msg.content = js.get('markdown')
                elif tp == 300:
                    msg.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    js = json.loads(string)
                    ext = js.get('multi')[0].get('ext')
                    js = json.loads(ext)
                    msg.content = js.get('b_tl')
                elif tp == 103:
                    msg.type = model_im.MESSAGE_CONTENT_TYPE_VIDEO
                    js = json.loads(string)
                    msg.content = js.get('url')
                    # msg.media_path = msg.content # ???
                elif tp == 1101:
                    msg.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    msg.content = string
                elif tp == 3:
                    msg.type = model_im.MESSAGE_CONTENT_TYPE_VOICE
                    msg.content = json.loads(string).get('url')
                elif tp == 1600:
                    msg.type = model_im.MESSAGE_CONTENT_TYPE_SYSTEM
                    msg.content = json.loads(string).get('dingContent')
                else:
                    msg.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    msg.content = string
                self.im.db_insert_table_message(msg)
            self.im.db_commit()
            cmd.Dispose()
            conn.Close()
            
def parse_ding(fs, extract_deleted, extract_source):
    nfs = FileSystem.FromLocalDir(r'G:\X\HUAWEI NXT-AL10_7.0_861918038118833_logic\Apps\com.alibaba.android.rimet')
    d = DingA(nfs, extract_deleted, extract_source)
    d.search_account()
    print('done!')
