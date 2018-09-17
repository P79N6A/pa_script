#coding:utf-8
import clr
clr.AddReference('System.Data.SQLite')
clr.AddReference('Base3264-UrlEncoder')
try:
    clr.AddReference('model_im')
except:
    pass
del clr

import System.Data.SQLite as sql
from PA_runtime import *
from System.Text import *
from System.IO import *
from System.Security.Cryptography import *
from System import Convert
from MhanoHarkness import *

import model_im
import os
import sys
import logging
import re
import sqlite3 
import hashlib
import struct
import base64
import json

en_recover = True

def md5(string):
    return hashlib.md5(string).hexdigest()

class MediaBts(object):
    def __init__(self, bts):
        self.stream = bts
        self.idx = 0
        self.max_idx = len(bts)
    

#####################Get Functions######################################
def GetString(reader, idx):
    return reader.GetString(idx) if not reader.IsDBNull(idx) else ""

def GetInt64(reader, idx):
    return reader.GetInt64(idx) if not reader.IsDBNull(idx) else 0

def GetBlob(reader, idx):
    return reader.GetValue(idx) if not reader.IsDBNull(idx) else None
#####################Const Type########################################

JPG = 0
PNG = 1
GIF = 2
SYSTEM = 3
AVATAR = 4

class Ding(object):
    def __init__(self, root, extract_deleted, extract_source):
        self.root = root
        self.extract_source = extract_source
        self.extract_deleted = extract_deleted
        self.result_sql = list()
        self.im = model_im.IM()
        cache = ds.OpenCachePath('Dingtalk')
        self.im.db_create(cache + '/c37r')
        self.cache_res = cache + '/c37r'

    def log_print(self, msg):
        print(u'[钉钉]:%s' % msg)

    @staticmethod
    def check_is_md5(enc_str):
        if len(enc_str) < 16 or len(enc_str) > 32:
            return False
        
        for s in enc_str:
            try:
                int(s, base=16)
            except Exception:
                return False
        return True

    def search_account(self):
        pre_node = self.root.GetByPath('Library/Preferences/com.laiwang.DingTalk.plist')
        if pre_node is None:
            self.log_print('''Can't find preferences node, parse exits!''')
            return
        bp = BPReader(pre_node.Data).top
        # pass...
        #device_id = bp['UTDID']['UTDID'].Value
        b = bp['UTDID']
        if b is None:
            self.log_print("EXCEPTION OF NONE DATA!")
            return 
        device_id = bp['UTDID']['UTDID'].Value
        abs_path = self.root.PathWithMountPoint
        sql_dir = os.path.join(abs_path, 'Documents/db')
        k = os.listdir(sql_dir)
        scops = list()
        for i in k:
            res = self.check_is_md5(i)
            if res:
                res = os.path.exists(os.path.join(sql_dir, '{}/db.sqlite'.format(i)))
                if res:
                    scops.append(i)
        #self.log_print('''total find %d accounts''' % len(scops))
        cache = ds.OpenCachePath('Dingtalk')
        for i in scops:
            r = device_id + i
            hash_code = hashlib.md5(r).hexdigest()
            key = hash_code[8:24]
            dest_sql = os.path.join(cache, '{}.sqlite'.format(i))
            dest_sql_fts = os.path.join(cache, '{}.sqlite_fts'.format(i))
            f_dest = open(dest_sql, 'wb')
            f_dest_fts = open(dest_sql_fts, 'wb')
            source_node = self.root.GetByPath('Documents/db/{}/db.sqlite'.format(i))
            source_node_fts = self.root.GetByPath('Documents/db/{}/db.sqlite_fts'.format(i))
            if source_node is None:
                continue
            data = source_node.Data
            sz = source_node.Size
            idx = 0
            rm = RijndaelManaged()
            rm.Key = Convert.FromBase64String(base64.b64encode(key))
            rm.Mode = CipherMode.ECB
            rm.Padding = PaddingMode.None
            tr = rm.CreateDecryptor()
            while idx < sz:
                bts = data.read(16)
                bts = Convert.FromBase64String(base64.b64encode(bts))
                t_r = tr.TransformFinalBlock(bts, 0, 16)
                f_dest.write(t_r)
                idx += 16
            f_dest.close()
            self.result_sql.append(dest_sql)
            # for further using...
            if source_node_fts is None:
                continue
            sz = source_node_fts.Size
            data = source_node_fts.Data
            idx = 0
            while idx < sz:
                bts = data.read(16)
                bts = Convert.FromBase64String(base64.b64encode(bts))
                t_r = tr.TransformFinalBlock(bts, 0, 16)
                f_dest_fts.write(t_r)
                idx += 16
    
    @staticmethod
    def __media_id_parse(bts, opt):
        if bts.max_idx == 0:
            return 0
        if opt & 0x80:
            if opt & 0xe0 == 0xe0:
                return opt | 0xFFFFFFFFFFFFFF00
            if opt == 0xcc or opt == 0xd0:
                bts.idx += 1
                return bts.stream[bts.idx - 1]
            elif opt == 0xcd:
                bts.idx += 2
                #return struct.unpack('H', bts.stream[bts.idx - 2: bts.idx])[0]
                return (bts.stream[bts.idx - 2] << 8 | bts.stream[bts.idx - 1])
            elif opt == 0xce:
                bts.idx += 4
                #return struct.unpack('I', bts.stream[bts.idx - 4: bts.idx])[0]
                return (bts.stream[bts.idx - 4] << 24 | bts.stream[bts.idx - 3] << 16 | bts.stream[bts.idx - 2] << 8 | bts.stream[bts.idx - 1])
            elif opt == 0xcf or opt == 0xd3:
                bts.idx += 8
                #return struct.unpack('q', bts.stream[bts.idx - 8: bts.idx])[0]
                return (bts.stream[bts.idx - 8] << 56| bts.stream[bts.idx - 7] << 48 | bts.stream[bts.idx - 6] << 40 | bts.stream[bts.idx - 5] << 32 | bts.stream[bts.idx - 4] << 24|\
                        bts.stream[bts.idx - 3] << 16| bts.stream[bts.idx - 2] << 8| bts.stream[bts.idx - 1])                        
            elif opt == 0xd1:
                bts.idx += 2
                #return struct.unpack('h', bts.stream[bts.idx - 2: bts.idx])[0]
                return (bts.stream[bts.idx - 2] << 8 | bts.stream[bts.idx - 1])
            elif opt == 0xd2:
                bts.idx += 4
                #return struct.unpack('i', bts.stream[bts.idx - 4: bts.idx])[0]
                return (bts.stream[bts.idx - 4] << 24 | bts.stream[bts.idx - 3] << 16 | bts.stream[bts.idx - 2] << 8 | bts.stream[bts.idx - 1])
            else:
                raise ValueError('illegal input of opt!')
        else:
            return opt
        return 0

    def process_image(self, media_id):
        m = media_id.replace('@', '') # remove @
        bts = Base64Url.FromBase64ForUrlString(m)
        m_stream = MediaBts(bytearray(bts))
        idx = 0
        header = m_stream.stream[0]
        f_count = 0
        width = 0
        height = 0
        
        if (header & 0xf0) == 0x90:
            f_count = header & 0x0f
            if f_count - 1 < 5:
                if f_count == 4:
                    m_stream.idx = 3
                    opt = m_stream.stream[2]
                    self.__media_id_parse(m_stream, opt)
                    opt = self.__media_id_parse(m_stream, 0xcc)
                    height = self.__media_id_parse(m_stream, opt)
                    opt = self.__media_id_parse(m_stream, 0xcc)
                    width = self.__media_id_parse(m_stream, opt)
        if width == 0 or height == 0:
            return ""
        res = "http://static.dingtalk.com/media/{}_{}_{}".format(m, width, height)
        return res
    
    def check_file_exists(self, url, padding):
        string = url + padding
        hash_code = md5(string)
        abs_path = self.root.PathWithMountPoint
        # 这尼玛的拼接。。。
        f_name = os.path.join(abs_path, 'Library/Caches/default/com.hackemist.SDWebImageCache.default/%s' % hash_code)
        res_path = self.root.AbsolutePath
        if os.path.exists(f_name):
            return res_path + ('/Library/Caches/default/com.hackemist.SDWebImageCache.default/%s' % hash_code)
        else:
            return ""

    def get_picture(self, media_id, pic_type):
        if media_id is "":
            return ""
        r = self.process_image(media_id)
        if r == '':
            return ''
        if pic_type == JPG:
            res = self.check_file_exists(r, ".jpg")
            if res != "":
                return res
            res = self.check_file_exists(r, ".jpg_thumb")
            if res != "":
                return res
            res = self.check_file_exists(r, ".jpg_1200x1200g.jpg_.webp")
            if res != "":
                return res
        elif pic_type == PNG:
            res = self.check_file_exists(r, "png")
            if res != "":
                return res
            res = self.check_file_exists(r, ".png_1200x1200g.jpg_.webp")
            if res != "":
                return res
        elif pic_type == GIF or pic_type == SYSTEM:
            res = self.check_file_exists(r, ".gif")
            if res != "":
                return res
        elif pic_type == AVATAR:
            res = self.check_file_exists(r, ".jpga70")
            if res != "":
                return res
            res = self.check_file_exists(r, ".jpga40")
            if res != "":
                return res
            res = self.check_file_exists(r, ".jpg")
            if res != "":
                return res
        return ""
        
    # 兼容老版本分析
    def parse(self):
        for i in self.result_sql:
            current_id = None
            connection = sql.SQLiteConnection('Data Source = {}; ReadOnly=True'.format(i))
            connection.Open()
            cmd = sql.SQLiteCommand(connection)
            cmd.CommandText = '''
                select uid, nick, avatarMediaId, mobile, gender, birthdayValue, address, extension, email from contact 
            '''
            reader = cmd.ExecuteReader()
            idx = 0
            f_dict = dict()
            while reader.Read():
                if idx == 0:
                    a = model_im.Account()
                    a.account_id = GetInt64(reader, 0)
                    a.nickname = GetString(reader, 1)
                    # real_name????
                    # photo ... pass for a while.
                    self.get_picture(GetString(reader, 2), AVATAR)
                    a.telephone = GetString(reader, 3)
                    a.gender = GetInt64(reader, 4)
                    a.birthday = GetInt64(reader, 5)
                    a.address = GetString(reader, 6)
                    a.email = GetString(reader, 8)
                    r = GetString(reader, 7)
                    if r is '':
                        pass
                    else:
                        r = json.loads(r)
                        if r.get('ownness') is not None:
                            #r = r.get('ownness').get(status)
                            pass
                        else:
                            pass
                    self.im.db_insert_table_account(a)
                    idx += 1
                    current_id = a.account_id
                    continue
                f = model_im.Friend()
                f.friend_id = GetInt64(reader, 0)
                f.nickname = GetString(reader, 1)
                # photo....
                # f.photo = #TODO generate photo id....
                f.photo = self.get_picture(GetString(reader, 2), AVATAR)
                f.account_id = current_id
                f.telephone = GetString(reader, 3)
                f.gender = GetInt64(reader, 4)
                f.birthday = GetInt64(reader, 5)
                f.deleted = 0
                f.email = GetString(reader, 8)
                f.address = GetString(reader, 6)
                r = GetString(reader, 7)
                if r is  '':
                    pass
                else:
                    r = json.loads(r)
                    if r.get('ownness') is not None:
                        pass #TODO fix it later
                self.im.db_insert_table_friend(f)
                f_dict[f.friend_id] = f
            cmd.Dispose()
            cmd.CommandText = '''
                select conversationId, title, createdAt, extensionJson, ownerId,
                memberLimit, memberCount, automaticIcon, customIcon from WKConversation where conversationType != 2
            '''
            reader = cmd.ExecuteReader()
            groups = list()
            while reader.Read():
                g = model_im.Chatroom()
                g.account_id = current_id
                g.chatroom_id = GetString(reader, 0)
                g.name = GetString(reader, 1)
                g.photo = self.get_picture(GetString(reader, 8), AVATAR)
                g.create_time = GetInt64(reader, 2) / 1000
                g.owner_id = GetInt64(reader, 4)
                g.max_member_count = GetInt64(reader, 5)
                g.member_count = GetInt64(reader, 6)
                groups.append(g.chatroom_id)
                self.im.db_insert_table_chatroom(g)
            chat_tbl_list = list()
            cmd.Dispose()
            cmd.CommandText = '''
                select tbl_name from sqlite_master where tbl_name like 'WKChat_%' and type = 'table'
            '''
            reader = cmd.ExecuteReader()
            while reader.Read():
                r = GetString(reader, 0)
                if r is '' or r.__contains__('fts'):
                    continue
                chat_tbl_list.append(r)
            cmd.Dispose()
            for r in chat_tbl_list:
                cmd.CommandText = ''' 
                    select messageId, conversationId, localSentTime, content, attachmentsType, senderId,
                    attachmentsJson from {} 
                '''.format(r)
                reader = cmd.ExecuteReader()
                while reader.Read():
                    msg = model_im.Message()
                    msg.deleted = 0
                    msg.account_id = current_id
                    msg.talker_id = GetString(reader, 1)
                    msg.sender_id = GetInt64(reader, 5)
                    msg.is_sender = 1 if msg.sender_id == current_id else 0
                    msg.content = GetString(reader, 3)
                    msg.send_time = GetInt64(reader, 2)
                    msg.msg_id = GetInt64(reader, 0)
                    msg.source = i
                    msg.talker_type = model_im.CHAT_TYPE_GROUP if msg.talker_id.__contains__(':') else model_im.CHAT_TYPE_FRIEND
                    #TODO add other message decryptor.... and parse etc.
                    tp = GetInt64(reader, 4)
                    try:
                        if tp == 1 or tp == 500 or tp == 501:
                            msg.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                        elif tp == 1101:
                            msg.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            msg.content = '[call message]'
                        elif tp == 600:
                            msg.type = model_im.MESSAGE_CONTENT_TYPE_CONTACT_CARD
                            string = GetString(reader, 6)
                            js = json.loads(string)
                            name = js.get('attachments')[0].get('extension').get('name')
                            uid = js.get('attachments')[0].get('extension').get('uid')
                            msg.content = 'uid:{}\nname:{}'.format(uid, name)
                        elif tp == 102:
                            msg.type = model_im.MESSAGE_CONTENT_TYPE_CONTACT_CARD
                            string = GetString(reader, 6)
                            js = json.loads(string)
                            title = js.get('attachments')[0].get('extension').get('title')
                            text = js.get('attachments')[0].get('extension').get('text')
                            pic = js.get('attachments')[0].get('extension').get('picUrl')
                            msg.content = 'title:{}\ntext:{}\npicUrl:{}'.format(title, text, pic)
                        elif tp == 202 or tp == 103:
                            msg.type = model_im.MESSAGE_CONTENT_TYPE_VIDEO
                            string = GetString(reader, 6)
                            js = json.loads(string)
                            media_id = js.get('attachments')[0].get('extension').get('picUrl')
                            abs_path = self.root.PathWithMountPoint
                            f_name = os.path.join(abs_path, "Library/Caches/videoCaches/%s.mp4" %media_id)
                            if os.path.exists(f_name):
                                msg.media_path = f_name
                            else:
                                msg.content = "video message: not cached"
                                msg.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                        elif tp == 104: # location ?
                            string = GetString(reader, 6)
                            js = json.loads(string)
                            lati = js.get('attachments')[0].get('extension').get('latitude')
                            lng = js.get('attachments')[0].get('extension').get('longitude')
                            name = js.get('attachments')[0].get('extension').get('locationName')
                            msg.location = md5(str(lati) + str(lng))
                            msg.type = model_im.MESSAGE_CONTENT_TYPE_LOCATION
                            msg.content = name
                            l = model_im.Location()
                            l.location_id = msg.location
                            l.deleted = 0
                            l.latitude = lati
                            l.longitude = lng
                            l.address = name
                            l.timestamp = GetInt64(reader, 2)
                            self.im.db_insert_table_location(l)
                        elif tp == 2: # image
                            msg.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                            string = GetString(reader, 6)
                            js = json.loads(string)
                            media_id = js.get('photoContent').get('mediaId')
                            t_f_name = js.get('photoContent').get('filename')
                            if t_f_name is None or t_f_name == "":
                                print(string)
                                ext = ''
                            else:
                                fn, ext = os.path.splitext(t_f_name)
                            if ext == "JPG" or ext == "jpg" or ext == 'MOV' or ext == 'mov':
                                fn = self.get_picture(media_id, JPG)
                            elif ext == 'PNG' or ext == 'png':
                                fn = self.get_picture(media_id, PNG)
                            elif ext == 'gif' or ext == 'GIF':
                                fn = self.get_picture(media_id, GIF)
                            else:
                                fn = self.get_picture(media_id, SYSTEM)
                            if fn != "":
                                msg.media_path = fn
                            else:
                                msg.content = 'image message not cached...'
                                msg.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                        #TODO Fix it after rp is certain...
                        elif tp == 900 or tp == 901:
                            msg.content = GetString(reader, 6)
                            msg.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                        else:
                            msg.content = GetString(reader, 3) if GetString(reader, 3) is not '' else GetString(reader, 6)
                            msg.type = model_im.MESSAGE_CONTENT_TYPE_SYSTEM
                    except:
                            self.log_print('error occurs: {}'.format(reader, 3))
                    self.im.db_insert_table_message(msg)
                cmd.Dispose()
                # group members....
            for g in groups:
                members = list()
                for t in chat_tbl_list:
                    cmd.CommandText = '''
                    select senderId from {}  where conversationId = '{}' group by senderId
                    '''.format(t, g)
                    reader = cmd.ExecuteReader()
                    while reader.Read():
                        m_id = GetInt64(reader, 0)
                        if members.__contains__(m_id) or m_id == current_id:
                            continue
                        members.append(m_id)
                    cmd.Dispose()
                for m in members:
                    cm = model_im.ChatroomMember()
                    cm.account_id = current_id
                    cm.member_id = m
                    if f_dict.__contains__(m):
                        #f = model_im.Friend()
                        f = f_dict[m]
                        cm.address = f.address
                        cm.display_name = f.nickname
                        cm.birthday = f.birthday
                        cm.photo = f.photo
                        cm.deleted = 0
                        cm.gender = f.gender
                        cm.email = f.email
                        cm.telephone = f.telephone
                        cm.signature = f.signature
                    cm.chatroom_id = g
                    self.im.db_insert_table_chatroom_member(cm)     
            self.im.db_commit()
            #self.im.db_close()
            #以下部分不兼容老版本分析:
            cmd.CommandText = '''
                select dingId, senderUid, sendAt, content from Ding
            '''
            reader = cmd.ExecuteReader()
            feed_dict = dict()
            while reader.Read():
                s_id = GetInt64(reader, 1)
                d_id = GetInt64(reader, 0)
                s_time = GetInt64(reader, 2) / 1000
                s_content = GetString(reader, 3)
                feed = model_im.Feed()
                feed.sender_id = s_id
                feed.account_id = current_id
                feed.repeated = 0
                feed.send_time = s_time
                feed.content = s_content
                feed_dict[d_id] = feed
                #feed.comments = s_id
                #self.im.db_insert_table_feed(feed)
            cmd.Dispose()
            cmd.CommandText = '''
                select dingId, commentId, commenterUid, attachmentJSON, createAt from dingcomment
            '''
            try:
                reader = cmd.ExecuteReader()
            except:
                reader = None
            while reader is not None and reader.Read():
                fcm = model_im.FeedComment()
                d_id = GetInt64(reader, 0)
                if not feed_dict.__contains__(d_id):
                    continue
                feed_dict[d_id].comments = str()
                feed_dict[d_id].comments += '{},'.format(GetInt64(reader, 1))
                fcm.comment_id = GetInt64(reader, 1)
                string = GetString(reader, 3)
                try:
                    fcm.content = json.loads(string).get('text')
                    fcm.sender_id = GetInt64(reader, 1)
                    fcm.sender_name = "" if not f_dict.__contains__(fcm.sender_id) else f_dict[fcm.sender_id].nickname
                    fcm.create_time = GetInt64(reader, 4) / 1000
                except:
                    pass
                self.im.db_insert_table_feed_comment(fcm)
            for k in feed_dict:
                self.im.db_insert_table_feed(feed_dict[k])
            feed_dict.clear() # no longer use...
            cmd.Dispose()
            # 出勤活动
            cmd.CommandText = '''
                select value from WebPersistenceModel where key like 'fastCheck_%' or 'asyncCheck_%'
            '''
            try:
                reader = cmd.ExecuteReader()
            except:
                reader = None
            while reader is not None and reader.Read():
                try:
                    string = GetString(reader, 0)
                    js = json.loads(string)
                    feed = model_im.Feed()
                    feed.sender_id = current_id
                    addr = js.get('checkResult').get('address')
                    time = js.get('checkResult').get('checkTime')
                    f_id = js.get('checkResult').get('id')
                    method = js.get('checkResult').get('locationMethod')
                    s_type = js.get('checkResult').get('scheduleType')
                    feed.content = '''
                    event:{}
                    address:{}
                    method:{}
                    '''.format(s_type, addr, method)
                    feed.send_time = time / 1000
                    self.im.db_insert_table_feed(feed)
                except:
                    continue
            self.im.db_commit()
            cmd.Dispose()
            connection.Close()
            
def parse_ding(root, extract_deleted, extract_source):
    #n_node = FileSystem.FromLocalDir(r'D:\ios_case\ding\5D59B4C6-C37F-43A3-86AF-312C5C3D427C')
    d = Ding(root, extract_deleted, extract_source)
    d.search_account()
    d.parse()
    models = model_im.GenerateModel(d.cache_res).get_models()
    mlm = ModelListMerger()
    pr = ParserResults()
    pr.Categories = DescripCategories.QQ

    pr.Models.AddRange(list(mlm.GetUnique(models)))
    pr.Build('钉钉')
    return pr
    
