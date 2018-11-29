#coding=utf-8
__author__ = "sumeng"

from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference('model_im')
    clr.AddReference('bcp_im')
    clr.AddReference('base_wechat')
    clr.AddReference('tencent_struct')
except:
    pass
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Security.Cryptography import *
from System.Text import *
from System.IO import *
from System import Convert
import System.Data.SQLite as SQLite

import os
import hashlib
import json
import base64
import sqlite3
import shutil
import model_im
import bcp_im
from base_wechat import *
import tencent_struct

# EnterPoint: analyze_wechat(root, extract_deleted, extract_source):
# Patterns: '/MicroMsg/.+/EnMicroMsg.db$'
# Models: Common.User, Common.Friend, Common.Group, Generic.Chat, Common.MomentContent

# app数据库版本
VERSION_APP_VALUE = 1


def analyze_wechat(root, extract_deleted, extract_source):
    pr = ParserResults()
    pr.Categories = DescripCategories.Wechat #声明这是微信应用解析的数据集
    models = WeChatParser(root, extract_deleted, extract_source).parse()
    mlm = ModelListMerger()
    
    pr.Models.AddRange(list(mlm.GetUnique(models)))
    pr.Build('微信')
    return pr


class WeChatParser(Wechat):
    
    def __init__(self, node, extract_deleted, extract_source):
        super(WeChatParser, self).__init__()
        self.root = node.Parent.Parent.Parent
        self.user_node = node.Parent
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.is_valid_user_dir = self._is_valid_user_dir()
        self.uin = self._get_uin()
        self.imei = self._get_imei(self.root.GetByPath('/MicroMsg/CompatibleInfo.cfg'))
        self.user_hash = self._get_user_hash()
        self.cache_path = ds.OpenCachePath('wechat')
        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)
        self.cache_db_name = self._md5(node.AbsolutePath)
        self.cache_db = os.path.join(self.cache_path, self.cache_db_name + '.db')
        nameValues.SafeAddValue(bcp_im.CONTACT_ACCOUNT_TYPE_IM_WECHAT, self.cache_db)

    def parse(self):
        if not self.is_valid_user_dir:
            return []
        if not self._can_decrypt(self.uin, self.user_hash):
            return []

        if self.im.need_parse(self.cache_db, VERSION_APP_VALUE):
            self.im.db_create(self.cache_db)

            self.contacts = {}
            self.user_account = model_im.Account()
            self.extend_nodes = []
            extend_nodes = self.root.FileSystem.Search('/Tencent/MicroMsg/{}$'.format(self.user_hash))
            for extend_node in extend_nodes:
                self.extend_nodes.append(extend_node)

            node = self.user_node.GetByPath('/EnMicroMsg.db')
            mm_db_path = os.path.join(self.cache_path, self.cache_db_name + '_mm.db')
            if Decryptor.decrypt(node, self._get_db_key(self.imei, self.uin), mm_db_path):
                self._parse_mm_db(mm_db_path, node.AbsolutePath)
            self._parse_wc_db(self.user_node.GetByPath('/SnsMicroMsg.db'))
            self._parse_fts_db(self.user_node.GetByPath('/FTS5IndexMicroMsg.db'))

            self.im.db_create_index()
            # 数据库填充完毕，请将中间数据库版本和app数据库版本插入数据库，用来检测app是否需要重新解析
            if not canceller.IsCancellationRequested:
                self.im.db_insert_table_version(model_im.VERSION_KEY_DB, model_im.VERSION_VALUE_DB)
                self.im.db_insert_table_version(model_im.VERSION_KEY_APP, VERSION_APP_VALUE)
            self.im.db_commit()
            self.im.db_close()

        models = self.get_models_from_cache_db()
        return models

    def get_models_from_cache_db(self):
        models = model_im.GenerateModel(self.cache_db).get_models()
        return models

    def _is_valid_user_dir(self):
        if self.root is None or self.user_node is None:
            return False
        if self.root.GetByPath('/shared_prefs/auth_info_key_prefs.xml') is None and self.root.GetByPath('/shared_prefs/com.tencent.mm_preferences.xml'):
            return False
        return True

    @staticmethod
    def _can_decrypt(uin, user_hash):
        return WeChatParser._md5('mm' + uin) == user_hash

    def _get_uin(self):
        uin = self._get_uin_from_auth_info(self.root.GetByPath('/shared_prefs/auth_info_key_prefs.xml'))
        if uin is None:
            uin = self._get_uin_from_prefs(self.root.GetByPath('/shared_prefs/com.tencent.mm_preferences.xml'))
        if uin is None:
            uin = ''
        return uin

    @staticmethod
    def _get_uin_from_auth_info(node):
        if node is None:
            return None
        es = []
        try:
            node.Data.seek(0)
            xml = XElement.Parse(node.read())
            es = xml.Elements('int')
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
        for e in es:
            if e.Attribute('name') and e.Attribute('name').Value == '_auth_uin' and e.Attribute('value'):
                return e.Attribute('value').Value
        return None

    @staticmethod
    def _get_uin_from_prefs(node):
        if node is None:
            return None
        es = []
        try:
            node.Data.seek(0)
            xml = XElement.Parse(node.read())
            es = xml.Elements('string')
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
        for e in es:
            if e.Attribute('name') and e.Attribute('name').Value == 'last_login_uin':
                return e.Value
        return None

    def _get_user_hash(self):
        path = self.user_node.AbsolutePath
        return os.path.basename(os.path.normpath(path))

    @staticmethod
    def _get_imei(node):
        imei = None
        if node is not None:
            try:
                node.Data.seek(0)
                content = node.read()
                pos = content.find(b'\x01\x02\x74\x00')
                if pos != -1:
                    size = ord(content[pos+4:pos+5])
                    imei = content[pos+5:pos+5+size]
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
        return imei

    @staticmethod
    def _get_db_key(imei, uin):
        if imei is None or uin is None:
            return None
        return WeChatParser._md5(imei + uin)[:7]

    def _parse_mm_db(self, mm_db_path, source):
        node_db = None
        if self.extract_deleted:
            node = self.create_memory_node(self.user_node, mm_db_path, os.path.basename(mm_db_path))
            try:
                node_db = SQLiteParser.Database.FromNode(node, canceller)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
                return
        db = sqlite3.connect(mm_db_path)
        self._parse_mm_db_user_info(db, source, node_db)
        self._parse_mm_db_contact(db, source, node_db)
        self._parse_mm_db_chatroom_member(db, source, node_db)
        self._parse_mm_db_message(db, source, node_db)
        db.close()

    def _parse_wc_db(self, node):
        if node is None:
            return False
        if canceller.IsCancellationRequested:
            return False

        db_path = os.path.join(self.cache_path, 'cache.db')
        self.db_mapping(node.PathWithMountPoint, db_path)
        if not os.path.exists(db_path):
            return False
        db = sqlite3.connect(db_path)
        if db is None:
            return False

        cursor = db.cursor()
        sql = 'select userName,content,attrBuf from SnsInfo'
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))

        while row is not None:
            if canceller.IsCancellationRequested:
                break
            try:
                username = self._db_column_get_string_value(row[0])
                content = self._db_column_get_blob_value(row[1])
                attr = self._db_column_get_blob_value(row[2])
                self._parse_wc_db_with_value(0, node.AbsolutePath, username, content, attr)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
            row = cursor.fetchone()
        self.im.db_commit()
        cursor.close()
        db.close()
        self.db_remove_mapping(db_path)

        if self.extract_deleted:
            if canceller.IsCancellationRequested:
                return False
            try:
                db = SQLiteParser.Database.FromNode(node, canceller)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
                return False
            if not db:
                return False

            if 'SnsInfo' in db.Tables:
                ts = SQLiteParser.TableSignature('SnsInfo')
                SQLiteParser.Tools.AddSignatureToTable(ts, "userName", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                SQLiteParser.Tools.AddSignatureToTable(ts, "content", SQLiteParser.FieldType.Blob, SQLiteParser.FieldConstraints.NotNull)
                for rec in db.ReadTableDeletedRecords(ts, False):
                    if canceller.IsCancellationRequested:
                        break
                    try:
                        username = self._db_record_get_string_value(rec, 'userName')
                        content = self._db_record_get_blob_value(rec, 'content')
                        attr = self._db_record_get_blob_value(rec, 'attrBuf')
                        deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                        self._parse_wc_db_with_value(deleted, node.AbsolutePath, username, content, attr)
                    except Exception as e:
                        TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
                self.im.db_commit()

    def _parse_wc_db_with_value(self, deleted, source, username, content_blob, attr_blob):
        content = None
        attr = None
        try:
            content = tencent_struct.tencent_struct().getSnsContent(content_blob)
            attr = tencent_struct.tencent_struct().getSnsAttrBuf(attr_blob)
        except Exception as e:
            return
        if content is None:
            return
        sns = SnsParser(content, attr)
        if username != sns.get_username():
            return

        moment_type = sns.get_type()

        feed = model_im.Feed()
        feed.deleted = deleted
        feed.source = source
        feed.account_id = self.user_account.account_id
        feed.sender_id = username
        feed.content = sns.get_content_text()
        feed.send_time = sns.get_timestamp()

        if moment_type == MOMENT_TYPE_IMAGE:
            medias = sns.get_content_medias()
            feed.image_path = ','.join(str(m) for m in medias)
        elif moment_type == MOMENT_TYPE_VIDEO:
            medias = sns.get_content_medias()
            feed.video_path = ','.join(str(m) for m in medias)
        elif moment_type in [MOMENT_TYPE_SHARED, MOMENT_TYPE_MUSIC]:
            feed.url, feed.url_title, feed.url_desc = sns.get_url_info()

        sns.get_location(feed)
        sns.get_likes(feed)
        sns.get_comments(feed)
        feed.insert_db(self.im)

    def _parse_fts_db(self, node):
        if node is None:
            return False
        if canceller.IsCancellationRequested:
            return False

        db_path = os.path.join(self.cache_path, 'cache.db')
        self.db_mapping(node.PathWithMountPoint, db_path)
        if not os.path.exists(db_path):
            return False
        db = SQLite.SQLiteConnection('Data Source = {}'.format(db_path))
        if db is None:
            return False
        db.Open()

        sql = 'select chatroom,member from FTS5ChatRoomMembers'
        db_cmd = SQLite.SQLiteCommand(sql, db)
        reader = None
        try:
            reader = db_cmd.ExecuteReader()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))

        if reader is not None:
            while reader.Read():
                if canceller.IsCancellationRequested:
                    break
                chatroom = self._db_reader_get_string_value(reader, 0)
                member = self._db_reader_get_string_value(reader, 1)
                if chatroom != '' and member != '':
                    self._parse_mm_db_chatroom_member_with_value(1, node.AbsolutePath, chatroom, member, '', None)
            reader.Close()
        db_cmd.Dispose()
        self.im.db_commit()

        sql = '''select c0,aux_index,timestamp 
                 from FTS5IndexContact_content
                 left join FTS5MetaContact
                 on id = docid'''
        db_cmd = SQLite.SQLiteCommand(sql, db)
        reader = None
        try:
            reader = db_cmd.ExecuteReader()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))

        contacts = {}
        if reader is not None:
            while reader.Read():
                if canceller.IsCancellationRequested:
                    break
                aux_index = self._db_reader_get_string_value(reader, 0)
                username = self._db_reader_get_string_value(reader, 1)
                timestamp = self._db_reader_get_int_value(reader, 2) / 1000
                if username in contacts:
                    contacts[username].append(aux_index)
                else:
                    contacts[username] = [aux_index]
            reader.Close()
        db_cmd.Dispose()

        for username in contacts:
            aux_index = contacts.get(username, [])
            alias = None
            nickname = None
            if username.endswith("@chatroom"):
                if len(aux_index) > 0:
                    nickname = aux_index[0]
            else:
                if len(aux_index) > 0:
                    alias = aux_index[0]
                if len(aux_index) > 1:
                    nickname = aux_index[1]
            self._parse_mm_db_contact_with_value(1, node.AbsolutePath, username, alias, nickname, '', 0, 0, None, None)
        self.im.db_commit()
        del contacts

        sql = '''select c0,aux_index,talker,timestamp 
                 from FTS5IndexMessage_content
                 left join FTS5MetaMessage
                 on id = docid'''
        db_cmd = SQLite.SQLiteCommand(sql, db)
        reader = None
        try:
            reader = db_cmd.ExecuteReader()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))

        if reader is not None:
            while reader.Read():
                if canceller.IsCancellationRequested:
                    break
                msg = self._db_reader_get_string_value(reader, 0)
                talker = self._db_reader_get_string_value(reader, 1)
                sender = self._db_reader_get_string_value(reader, 2)
                timestamp = self._db_reader_get_int_value(reader, 3) / 1000
                if talker != '':
                    message = model_im.Message()
                    message.deleted = 1
                    message.source = node.AbsolutePath
                    message.account_id = self.user_account.account_id
                    message.talker_id = talker
                    message.talker_name = self.contacts.get(talker, {}).get('nickname')
                    message.sender_id = sender
                    message.sender_name = self.contacts.get(sender, {}).get('nickname')
                    message.is_sender = 1 if talker == sender else 0
                    message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    message.send_time = timestamp
                    message.content = msg
                    message.talker_type = model_im.CHAT_TYPE_GROUP if talker.endswith("@chatroom") else model_im.CHAT_TYPE_FRIEND
                    message.insert_db(self.im)
            reader.Close()
        db_cmd.Dispose()
        self.im.db_commit()

        db.Close()
        self.db_remove_mapping(db_path)

    def _parse_mm_db_user_info(self, db, source, node_db):
        cursor = db.cursor()
        self.user_account.source = source
        self.user_account.account_id = self._parse_mm_db_get_user_info_from_userinfo(cursor, 2)
        self.user_account.nickname = self._parse_mm_db_get_user_info_from_userinfo(cursor, 4)
        self.user_account.email = self._parse_mm_db_get_user_info_from_userinfo(cursor, 5)
        self.user_account.telephone = self._parse_mm_db_get_user_info_from_userinfo(cursor, 6)
        self.user_account.signature = self._parse_mm_db_get_user_info_from_userinfo(cursor, 12291)
        self.user_account.username = self._parse_mm_db_get_user_info_from_userinfo2(cursor, 'USERINFO_LAST_LOGIN_USERNAME_STRING')
        self.user_account.photo = self._parse_mm_db_get_user_info_from_userinfo2(cursor, 'USERINFO_SELFINFO_SMALLIMGURL_STRING')
        self.user_account.insert_db(self.im)
        self.im.db_commit()
        cursor.close()

    def _parse_mm_db_get_user_info_from_userinfo(self, cursor, id):
        sql = 'select value from userinfo where id = {}'.format(id)
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
        if row is not None:
            return self._db_column_get_string_value(row[0])
        return None

    def _parse_mm_db_get_user_info_from_userinfo2(self, cursor, sid):
        sql = "select value from userinfo2 where sid = '{}'".format(sid)
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
        if row is not None:
            return self._db_column_get_string_value(row[0])
        return None

    def _parse_mm_db_contact(self, db, source, node_db):
        sql = '''select rcontact.username,alias,nickname,conRemark,type,verifyFlag,reserved1,reserved2 
                 from rcontact 
                 left outer join img_flag 
                 on rcontact.username = img_flag.username'''
        row = None
        cursor = db.cursor()
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
        while row is not None:
            if canceller.IsCancellationRequested:
                break
            username = self._db_column_get_string_value(row[0])
            alias = self._db_column_get_string_value(row[1])
            nickname = self._db_column_get_string_value(row[2])
            remark = self._db_column_get_string_value(row[3])
            contact_type = self._db_column_get_int_value(row[4])
            verify_flag = self._db_column_get_int_value(row[5])
            portrait_hd = self._db_column_get_string_value(row[6])
            portrait = self._db_column_get_string_value(row[7])

            self._parse_mm_db_contact_with_value(0, source, username, alias, nickname, remark, contact_type, verify_flag, portrait_hd, portrait)
            row = cursor.fetchone()
        self.im.db_commit()
        cursor.close()

        if self.extract_deleted and node_db is not None and 'rcontact' in node_db.Tables:
            if canceller.IsCancellationRequested:
                return
            ts = SQLiteParser.TableSignature('rcontact')
            SQLiteParser.Tools.AddSignatureToTable(ts, "username", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in node_db.ReadTableDeletedRecords(ts, False):
                if canceller.IsCancellationRequested:
                    break
                username = self._db_record_get_string_value(rec, 'username')
                if username in [None, '']:
                    continue
                alias = self._db_record_get_string_value(rec, 'alias')
                nickname = self._db_record_get_string_value(rec, 'nickname')
                remark = self._db_record_get_string_value(rec, 'conRemark')
                contact_type = self._db_record_get_int_value(rec, 'type')
                verify_flag = self._db_record_get_string_value(rec, 'verifyFlag')
                deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                self._parse_mm_db_contact_with_value(deleted, source, username, alias, nickname, remark, contact_type, verify_flag, None, None)
            self.im.db_commit()

    def _parse_mm_db_contact_with_value(self, deleted, source, username, alias, nickname, remark, contact_type, verify_flag, portrait_hd, portrait):
        if username not in [None, '']:
            head = portrait
            if portrait_hd and len(portrait_hd) > 0:
                head = portrait_hd

            contact = {}
            if nickname:
                contact['nickname'] = nickname
            if remark:
                contact['remark'] = remark
            contact['verify_flag'] = verify_flag
            if head:
                contact['photo'] = head

            if deleted == 0:
                self.contacts[username] = contact
            else:
                if username not in self.contacts:
                    self.contacts[username] = contact

            if username.endswith('@chatroom'):
                chatroom = model_im.Chatroom()
                chatroom.deleted = deleted
                chatroom.source = source
                chatroom.account_id = self.user_account.account_id
                chatroom.chatroom_id = username
                chatroom.name = nickname
                chatroom.photo = head
                chatroom.insert_db(self.im)
            else:
                friend = model_im.Friend()
                friend.deleted = deleted
                friend.source = source
                friend.account_id = self.user_account.account_id
                friend.friend_id = username
                friend.type = model_im.FRIEND_TYPE_FRIEND if verify_flag == 0 else model_im.FRIEND_TYPE_SUBSCRIBE
                friend.nickname = nickname
                friend.remark = remark
                friend.photo = head
                friend.insert_db(self.im)

    def _parse_mm_db_chatroom_member(self, db, source, node_db):
        sql = '''select chatroomname,memberlist,displayname,selfDisplayName,roomowner
                 from chatroom'''
        row = None
        cursor = db.cursor()
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
        
        while row is not None:
            if canceller.IsCancellationRequested:
                break
            chatroom_id = self._db_column_get_string_value(row[0])
            member_list = self._db_column_get_string_value(row[1])
            display_name_list = self._db_column_get_string_value(row[2])
            room_owner = self._db_column_get_string_value(row[4])

            self._parse_mm_db_chatroom_member_with_value(0, source, chatroom_id, member_list, display_name_list, room_owner)
            row = cursor.fetchone()
        self.im.db_commit()
        cursor.close()

        if self.extract_deleted and node_db is not None and 'chatroom' in node_db.Tables:
            ts = SQLiteParser.TableSignature('chatroom')
            SQLiteParser.Tools.AddSignatureToTable(ts, "chatroomname", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in node_db.ReadTableDeletedRecords(ts, False):
                if canceller.IsCancellationRequested:
                    break
                chatroom_id = self._db_record_get_string_value(rec, 'chatroomname')
                if chatroom_id in [None, '']:
                    continue
                member_list = self._db_record_get_string_value(rec, 'memberlist')
                display_name_list = self._db_record_get_string_value(rec, 'displayname')
                room_owner = self._db_record_get_string_value(rec, 'roomowner')
                deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                self._parse_mm_db_chatroom_member_with_value(deleted, source, chatroom_id, member_list, display_name_list, room_owner)
            self.im.db_commit()

    def _parse_mm_db_chatroom_member_with_value(self, deleted, source, chatroom_id, member_list, display_name_list, room_owner):
        room_members = member_list.split(';')
        display_names = display_name_list.split('、')
            
        cm = model_im.ChatroomMember()
        cm.deleted = deleted
        cm.source = source
        cm.account_id = self.user_account.account_id
        cm.chatroom_id = chatroom_id
        for i, room_member in enumerate(room_members):
            if canceller.IsCancellationRequested:
                break
            cm.member_id = room_member
            if i < len(display_names) and display_names[i] != room_member:
                cm.display_name = display_names[i]
            cm.insert_db(self.im)

    def _parse_mm_db_message(self, db, source, node_db):
        sql = 'select talker,content,imgPath,isSend,status,type,createTime,msgId,lvbuffer,msgSvrId from message'
        row = None
        cursor = db.cursor()
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
        
        while row is not None:
            if canceller.IsCancellationRequested:
                break
            talker = self._db_column_get_string_value(row[0])
            msg = self._db_column_get_string_value(row[1])
            img_path = self._db_column_get_string_value(row[2])
            is_send = self._db_column_get_int_value(row[3])
            status = self._db_column_get_int_value(row[4])
            msg_type = self._db_column_get_int_value(row[5])
            create_time = self._db_column_get_int_value(row[6]) / 1000
            msg_id = self._db_column_get_string_value(row[7])
            lv_buffer = self._db_column_get_blob_value(row[8])
            msg_svr_id = self._db_column_get_string_value(row[9])
            
            self._parse_mm_db_message_with_value(0, source, talker, msg, img_path, is_send, status, msg_type, create_time, msg_id, lv_buffer, msg_svr_id)
            row = cursor.fetchone()
        self.im.db_commit()
        cursor.close()

        if self.extract_deleted and node_db is not None and 'message' in node_db.Tables:
            if canceller.IsCancellationRequested:
                return
            ts = SQLiteParser.TableSignature('message')
            SQLiteParser.Tools.AddSignatureToTable(ts, "talker", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            SQLiteParser.Tools.AddSignatureToTable(ts, "content", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in node_db.ReadTableDeletedRecords(ts, False):
                if canceller.IsCancellationRequested:
                    break
                talker = self._db_record_get_string_value(rec, 'talker')
                if talker in [None, '']:
                    continue
                msg = self._db_record_get_string_value(rec, 'content')
                img_path = self._db_record_get_string_value(rec, 'imgPath')
                is_send = self._db_record_get_int_value(rec, 'isSend')
                status = self._db_record_get_int_value(rec, 'status')
                msg_type = self._db_record_get_int_value(rec, 'type')
                create_time = self._db_record_get_int_value(rec, 'createTime') / 1000
                msg_id = self._db_record_get_string_value(rec, 'msgId')
                lv_buffer = self._db_record_get_blob_value(rec, 'lvbuffer')
                msg_svr_id = self._db_record_get_string_value(rec, 'msgSvrId')
                deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                self._parse_mm_db_message_with_value(deleted, source, talker, msg, img_path, is_send, status, msg_type, create_time, msg_id, lv_buffer, msg_svr_id)
            self.im.db_commit()

    def _parse_mm_db_message_with_value(self, deleted, source, talker, msg, img_path, is_send, status, msg_type, create_time, msg_id, lv_buffer, msg_svr_id):
        contact = self.contacts.get(talker, {})

        message = model_im.Message()
        message.deleted = deleted
        message.source = source
        message.account_id = self.user_account.account_id
        message.talker_id = talker
        message.talker_name = contact.get('nickname')
        message.is_sender = is_send
        message.msg_id = msg_svr_id
        message.type = self._convert_msg_type(msg_type)
        message.send_time = create_time
        message.media_path = self._process_parse_message_img_path(msg_type, img_path)
        if talker.endswith("@chatroom"):
            self._process_parse_group_message(msg, msg_type, is_send != 0, 'hash', message)
            message.sender_name = self.contacts.get(message.sender_id, {}).get('nickname')
            message.talker_type = model_im.CHAT_TYPE_GROUP
        else:
            message.sender_id = self.user_account.account_id if is_send != 0 else talker
            self._process_parse_friend_message(msg, msg_type, 'hash', message)
            message.sender_name = self.contacts.get(message.sender_id, {}).get('nickname')
            message.talker_type = model_im.CHAT_TYPE_FRIEND if contact.get('verify_flag') == 0 else model_im.CHAT_TYPE_SUBSCRIBE
        message.insert_db(self.im)

    def _process_parse_friend_message(self, msg, msg_type, friend_hash, model):
        content = msg

        if msg_type in [MSG_TYPE_TEXT, MSG_TYPE_IMAGE, MSG_TYPE_VOICE, MSG_TYPE_CONTACT_CARD, MSG_TYPE_VIDEO, MSG_TYPE_VIDEO_2, MSG_TYPE_EMOJI]:
            pass
        elif msg_type == MSG_TYPE_LOCATION:
            if model is not None:
                self._process_parse_message_location(content, model)
        elif msg_type == MSG_TYPE_VOIP:
            content = self._process_parse_message_voip(content)
        elif msg_type == MSG_TYPE_VOIP_GROUP:
            content = self._process_parse_message_voip_group(content)
        elif msg_type == MSG_TYPE_SYSTEM:
            pass
        elif msg_type in [MSG_TYPE_SYSTEM_2, MSG_TYPE_SYSTEM_3]:
            content = self._process_parse_message_system_xml(content)
        elif msg_type == MSG_TYPE_LINK_SEMI:
            pass
        else:  # MSG_TYPE_LINK
            self._process_parse_message_link(content, model)

        model.content = content

    def _process_parse_group_message(self, msg, msg_type, is_sender, group_hash, model):
        content = msg
        if not is_sender:
            sender_id = None
            index = msg.find(':')
            if index != -1:
                sender_id = msg[:index]
            if msg_type == MSG_TYPE_EMOJI:
                index = content.find('*#*\n')
                if index != -1:
                    content = content[index+4:]
            else:
                index = content.find(':\n')
                if index != -1:
                    content = content[index+2:]
            model.sender_id = sender_id
        else:
            model.sender_id = self.user_account.account_id
        
        self._process_parse_friend_message(content, msg_type, group_hash, model)

    def _process_parse_message_img_path(self, msg_type, img_path):
        media_path = None
        if msg_type == MSG_TYPE_IMAGE:
            media_path = self._process_parse_message_tranlate_img_path(img_path)
        elif msg_type == MSG_TYPE_VOICE:
            media_path = self._process_parse_message_tranlate_voice_path(img_path)
        elif msg_type == MSG_TYPE_VIDEO:
            media_path = self._process_parse_message_tranlate_video_path(img_path)
        return media_path

    def _process_parse_message_tranlate_img_path(self, img_path):
        media_path = None
        if img_path in (None, ''):
            return media_path
        THUMBNAIL_DIRPATH = 'THUMBNAIL_DIRPATH://'
        TH_PREFIX = 'th_'
        if img_path.startswith(THUMBNAIL_DIRPATH):
            img_name = img_path[len(THUMBNAIL_DIRPATH):]
            m1 = ''
            m2 = ''
            if img_name.startswith(TH_PREFIX):
                m1 = img_name[len(TH_PREFIX):len(TH_PREFIX)+2]
                m2 = img_name[len(TH_PREFIX)+2:len(TH_PREFIX)+4]
            else:
                m1 = img_name[0:2]
                m2 = img_name[2:4]
            for extend_node in self.extend_nodes:
                if canceller.IsCancellationRequested:
                    break
                node = extend_node.GetByPath('/image2/{0}/{1}/{2}'.format(m1, m2, img_name))
                if node is not None:
                    media_path = node.AbsolutePath
                    break
            if media_path is None:
                media_path = '/no_image'
        return media_path

    def _process_parse_message_tranlate_voice_path(self, voice_id):
        media_path = None
        hash = self._md5(voice_id)
        m1 = hash[0:2]
        m2 = hash[2:4]
        for extend_node in self.extend_nodes:
            if canceller.IsCancellationRequested:
                break
            node = extend_node.GetByPath('/voice2/{0}/{1}/msg_{2}.amr'.format(m1, m2, voice_id))
            if node is not None:
                media_path = node.AbsolutePath
                break
        if media_path is None:
            media_path = '/no_voice'
        return media_path

    def _process_parse_message_tranlate_video_path(self, video_id):
        media_path = None
        for extend_node in self.extend_nodes:
            if canceller.IsCancellationRequested:
                break
            node = extend_node.GetByPath('/video/{}.mp4'.format(video_id))
            if node is not None:
                media_path = node.AbsolutePath
                break

            node = extend_node.GetByPath('/video/{}.jpg'.format(video_id))
            if node is not None:
                media_path = node.AbsolutePath
                break
        if media_path is None:
            media_path = '/no_video'
        return media_path


class SnsParser:
    def __init__(self, content, attr):
        self.content = content
        self.attr = attr
        if self._get_ts_value(self.content, 2) != self._get_ts_value(self.attr, 2):
            self.attr = None

    def get_username(self):
        return self._get_ts_value(self.content, 2)

    def get_type(self):
        ret = self._get_ts_value(self.content, 8)
        if type(ret) == list and len(ret) > 0:
            ret = ret[0]
            if type(ret) == tuple and len(ret) > 1:
                ret = ret[1]
                if type(ret) == dict:
                    return self._get_ts_value(ret, 2)
        return MOMENT_TYPE_TEXT_ONLY

    def get_content_text(self):
        return self._get_ts_value(self.content, 5)

    def get_content_medias(self):
        ret = self._get_ts_value(self.content, 8)
        if type(ret) == list and len(ret) > 0:
            ret = ret[0]
            if type(ret) == tuple and len(ret) > 1:
                ret = ret[1]
                if type(ret) == dict:
                    ret = self._get_ts_value(ret, 5)
                    if type(ret) == list and len(ret) > 0:
                        medias = []
                        for media in ret:
                            if type(media) == tuple and len(media) > 1:
                                media = media[1]
                                if type(media) == dict:
                                    media = self._get_ts_value(media, 4)
                                    if media not in [None, '']:
                                        medias.append(media)
                        return medias
        return None

    def get_url_info(self):
        ret = self._get_ts_value(self.content, 8)
        if type(ret) == list and len(ret) > 0:
            ret = ret[0]
            if type(ret) == tuple and len(ret) > 1:
                ret = ret[1]
                if type(ret) == dict:
                    title = self._get_ts_value(ret, 3)
                    url = self._get_ts_value(ret, 4)
                    return url, title, None
        return (None, None, None)

    def get_timestamp(self):
        return self._get_ts_value(self.content, 4)

    def get_location(self, feed):
        ret = self._get_ts_value(self.content, 6)
        if type(ret) == list and len(ret) > 0:
            ret = ret[0]
            if type(ret) == tuple and len(ret) > 1:
                ret = ret[1]
                if type(ret) == dict:
                    location = feed.create_location()
                    location.latitude = self._get_ts_value(ret, 2)
                    location.longitude = self._get_ts_value(ret, 1)
                    location.address = self._get_ts_value(ret, 4)

    def get_likes(self, feed):
        ret = self._get_ts_value(self.attr, 9)
        if type(ret) == list and len(ret) > 0:
            for like in ret:
                if canceller.IsCancellationRequested:
                    break
                if type(like) == tuple and len(like) > 1:
                    like = like[1]
                    if type(like) == dict:
                        fl = feed.create_like()
                        fl.sender_id = self._get_ts_value(like, 1)
                        fl.sender_name = self._get_ts_value(like, 2)
                        fl.create_time = self._get_ts_value(like, 6)

    def get_comments(self, feed):
        ret = self._get_ts_value(self.attr, 12)
        if type(ret) == list and len(ret) > 0:
            for comment in ret:
                if canceller.IsCancellationRequested:
                    break
                if type(comment) == tuple and len(comment) > 1:
                    comment = comment[1]
                    if type(comment) == dict:
                        fm = feed.create_comment()
                        fm.sender_id = self._get_ts_value(comment, 1)
                        fm.sender_name = self._get_ts_value(comment, 2)
                        fm.ref_user_id = self._get_ts_value(comment, 9)
                        if fm.ref_user_id == '':
                            fm.ref_user_id = None
                        fm.content = self._get_ts_value(comment, 5)
                        fm.create_time = self._get_ts_value(comment, 6)

    @staticmethod
    def _get_ts_value(ts, key):
        if ts is not None and key in ts:
            ret = ts[key]
            if type(ret) == tuple and len(ret) > 1:
                return ret[1]
            else:
                return ret
        return None


class Decryptor:
    @staticmethod
    def decrypt(src_node, key, dst_db_path):
        if src_node is None:
            return False
        if key in (None, ''):
            return False
        if os.path.exists(dst_db_path):
            try:
                os.remove(dst_db_path)
            except Exception as e:
                print('Decryptor decrypt() error: can not remove dst_db_path(%s)' % dst_db_path)
                return False

        size = src_node.Size
        src_node.Data.seek(0)
        first_page = src_node.read(1024)

        if len(first_page) < 1024:
            print('Decryptor decrypt() error: first_page size less than 1024!')
            return False

        salt = first_page[0:16]
        final_key = hashlib.pbkdf2_hmac('sha1', key.encode(encoding='utf-8'), salt, 4000, 32)
        final_key = Convert.FromBase64String(base64.b64encode(final_key))

        iv = first_page[1008: 1024]
        content = Decryptor.aes_decrypt(final_key,
                                        Convert.FromBase64String(base64.b64encode(iv)),
                                        Convert.FromBase64String(base64.b64encode(first_page[16:1008])))
        if not Decryptor.is_valid_decrypted_header(content):
            print('Decryptor decrypt() error: db(%s) and key(%s) is not valid' % (src_node.AbsolutePath, key))
            return False

        de = open(dst_db_path, 'wb')
        de.write(b'SQLite format 3\0')
        content[2] = 1
        content[3] = 1
        de.write(content)
        de.write(iv)

        for _ in range(1, size // 1024):
            if canceller.IsCancellationRequested:
                break
            content = src_node.read(1024)
            iv = content[1008: 1024]
            de.write(Decryptor.aes_decrypt(final_key,
                                           Convert.FromBase64String(base64.b64encode(iv)),
                                           Convert.FromBase64String(base64.b64encode(content[:1008]))))
            de.write(iv)
        de.close()

        Decryptor.db_fix_header(dst_db_path)
        return True

    @staticmethod
    def is_valid_decrypted_header(header):
        # skip first 16 bytes
        if type(header) is str:  # python2
            header = [ord(x) for x in header]
        return header[21 - 16] == 64 and header[22 - 16] == 32 and header[23 - 16] == 32

    @staticmethod
    def aes_decrypt(key, iv, content):
        aes = Aes.Create()
        aes.Mode = CipherMode.CBC
        aes.Padding = PaddingMode.None
        aes.Key = key
        aes.IV = iv

        result = None
        try:
            transform = aes.CreateDecryptor(key, iv)
            memory_stream = MemoryStream()
            crypto_stream = CryptoStream(memory_stream, transform, CryptoStreamMode.Write)
            crypto_stream.Write(content, 0, content.Length)
            crypto_stream.FlushFinalBlock()
            result = memory_stream.ToArray()
        except Exception as e:
            print('Decryptor aes_decrypt() error: %s' % e)
        finally:
            memory_stream.Close()
            crypto_stream.Close()

        return result

    @staticmethod
    def db_fix_header(db_path):
        if not os.path.exists(db_path):
            return False
        if os.path.getsize(db_path) < 20:
            return False
        if not os.access(db_path, os.W_OK):
            return False

        with open(db_path, 'r+b') as f:
            content = f.read(16)
            if content == 'SQLite format 3\0':
                f.seek(18)
                flag1 = ord(f.read(1))
                flag2 = ord(f.read(1))
                if flag1 != 1:
                    f.seek(18)
                    f.write('\x01')
                if flag2 != 1:
                    f.seek(19)
                    f.write('\x01')
        return True