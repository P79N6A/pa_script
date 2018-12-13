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
except:
    pass
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions
import System.Data.SQLite as SQLite

import os
import hashlib
import json
import string
import sqlite3
import shutil
import base64
import datetime
import model_im
import bcp_im
from base_wechat import *

# EnterPoint: analyze_wechat(root, extract_deleted, extract_source):
# Patterns: '/DB/MM\.sqlite$'
# Models: Common.User, Common.Friend, Common.Group, Generic.Chat, Common.MomentContent

# app数据库版本
VERSION_APP_VALUE = 5

g_app_id = 0
g_app_set = set()


def analyze_wechat(root, extract_deleted, extract_source):
    pr = ParserResults()
    pr.Categories = DescripCategories.Wechat #声明这是微信应用解析的数据集
    models = WeChatParser(root, extract_deleted, extract_source).parse()
    mlm = ModelListMerger()
    
    pr.Models.AddRange(list(mlm.GetUnique(models)))
    pr.Build(get_build(root.Parent.Parent))
    return pr


def get_build(node):
    global g_app_id, g_app_set
    app_path = node.AbsolutePath
    if app_path not in g_app_set:
        g_app_set.add(app_path)
        g_app_id += 1
    build = '微信'
    if g_app_id > 1:
        build += str(g_app_id)
    return build


class WeChatParser(Wechat):
    
    def __init__(self, node, extract_deleted, extract_source):
        super(WeChatParser, self).__init__()
        self.root = node.Parent.Parent
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.progress_value = 0

        self.user_hash = self.get_user_hash()
        self.private_root = self.root.Parent.Parent.GetByPath('/Library/WechatPrivate/'+self.user_hash)
        self.cache_path = os.path.join(ds.OpenCachePath('wechat'), self.get_user_guid())
        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)
        self.cache_db = os.path.join(self.cache_path, self.user_hash + '.db')
        save_cache_path(bcp_im.CONTACT_ACCOUNT_TYPE_IM_WECHAT, self.cache_db, ds.OpenCachePath("tmp"))

    def parse(self):
        if self.im.need_parse(self.cache_db, VERSION_APP_VALUE):
            self.im.db_create(self.cache_db)

            self.contacts = {}
            self.user_account = model_im.Account()

            if not self._get_user_from_setting(self.root.GetByPath('mmsetting.archive')):
                self.user_account.account_id = self.user_hash
                self.user_account.insert_db(self.im)
                self.im.db_commit()
            self.set_progress(1)

            try:
                self._parse_user_contact_db(self.root.GetByPath('/DB/WCDB_Contact.sqlite'))
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))
            self.set_progress(5)
            try:
                self._parse_user_mm_db(self.root.GetByPath('/DB/MM.sqlite'))
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))
            self.set_progress(30)
            try:
                self._parse_user_wc_db(self.root.GetByPath('/wc/wc005_008.db'))
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))
            self.set_progress(50)
            try:
                self._parse_user_fts_db(self.root.GetByPath('/fts/fts_message.db'))
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))
            self.set_progress(65)
            if self.private_root is not None:
                try:
                    self._parse_user_fav_db(self.private_root.GetByPath('/Favorites/fav.db'))
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))
                try:
                    self._parse_user_search(self.private_root.GetByPath('/searchH5/cache/wshistory.pb'))
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))
            self.set_progress(70)
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
        models = model_im.GenerateModel(self.cache_db, get_build(self.root), DescripCategories.Wechat, self.progress_value).get_models()
        return models

    def set_progress(self, value):
        self.progress_value = value
        progress.Value = value

    def get_user_hash(self):
        path = self.root.AbsolutePath
        return os.path.basename(os.path.normpath(path))

    def get_user_guid(self):
        path = self.root.Parent.Parent.AbsolutePath
        return os.path.basename(os.path.normpath(path))
    
    def _get_user_from_setting(self, user_plist):
        if user_plist is None:
            return False

        root = None
        try:
            root = BPReader.GetTree(user_plist)
        except Exception as e:
            return False
        if not root or not root.Children:
            return False

        self.user_account.account_id = self._bpreader_node_get_string_value(root, 'UsrName')
        self.user_account.nickname = self._bpreader_node_get_string_value(root, 'NickName')
        self.user_account.gender = self._convert_gender_type(self._bpreader_node_get_int_value(root, 'Sex'))
        self.user_account.telephone = self._bpreader_node_get_string_value(root, 'Mobile')
        self.user_account.email = self._bpreader_node_get_string_value(root, 'Email')
        self.user_account.city = self._bpreader_node_get_string_value(root, 'City')
        self.user_account.country = self._bpreader_node_get_string_value(root, 'Country')
        self.user_account.province = self._bpreader_node_get_string_value(root, 'Province')
        self.user_account.signature = self._bpreader_node_get_string_value(root, 'Signature')

        if 'new_dicsetting' in root.Children:
            setting_node = root.Children['new_dicsetting']
            if 'headhdimgurl' in setting_node.Children:
                self.user_account.photo = self._bpreader_node_get_string_value(setting_node, 'headhdimgurl')
            else:
                self.user_account.photo = self._bpreader_node_get_string_value(setting_node, 'headimgurl')
        self.user_account.source = user_plist.AbsolutePath
        self.user_account.insert_db(self.im)
        self.im.db_commit()

        if self.user_account.account_id:
            contact = {}
            if self.user_account.nickname:
                contact['nickname'] = self.user_account.nickname
            if self.user_account.photo:
                contact['photo'] = self.user_account.photo
            self.contacts[self.user_account.account_id] = contact

        return True

    def _parse_user_contact_db(self, node):
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
        sql = '''select userName,type,certificationFlag,dbContactRemark,dbContactHeadImage,dbContactChatRoom
                 from Friend '''
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))

        while row is not None:
            if canceller.IsCancellationRequested:
                break
            try:
                username = self._db_column_get_string_value(row[0])
                if username not in [None, '']:
                    contact_type = self._db_column_get_int_value(row[1])
                    certification_flag = self._db_column_get_int_value(row[2])
                    contact_remark = self._db_column_get_blob_value(row[3])
                    contact_head_image = self._db_column_get_blob_value(row[4])
                    contact_chatroom = self._db_column_get_blob_value(row[5])
                    self._parse_user_contact_db_with_value(0, node.AbsolutePath, username, contact_type, certification_flag, contact_remark, contact_head_image, contact_chatroom)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))
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
                return False
            if not db:
                return False

            if 'Friend' in db.Tables:
                ts = SQLiteParser.TableSignature('Friend')
                SQLiteParser.Tools.AddSignatureToTable(ts, "userName", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                for rec in db.ReadTableDeletedRecords(ts, False):
                    if canceller.IsCancellationRequested:
                        break
                    try:
                        username = self._db_record_get_string_value(rec, 'userName')
                        if username in [None, '']:
                            continue
                        contact_type = self._db_record_get_int_value(rec, 'type')
                        certification_flag = self._db_record_get_int_value(rec, 'certificationFlag')
                        contact_remark = self._db_record_get_blob_value(rec, 'dbContactRemark')
                        contact_head_image = self._db_record_get_blob_value(rec, 'dbContactHeadImage')
                        contact_chatroom = self._db_record_get_blob_value(rec, 'dbContactChatRoom')
                        deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                        self._parse_user_contact_db_with_value(deleted, node.AbsolutePath, username, contact_type, certification_flag, contact_remark, contact_head_image, contact_chatroom)
                    except Exception as e:
                        TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))
                self.im.db_commit()
        return True

    def _parse_user_contact_db_with_value(self, deleted, source, username, contact_type, certification_flag, contact_remark, contact_head_image, contact_chatroom):
        nickname = None
        alias = None
        remark = None
        if contact_remark is not None:
            nickname, alias, remark = self._process_parse_contact_remark(contact_remark)
        head = None
        if contact_head_image is not None:
            head, head_hd = self._process_parse_contact_head(contact_head_image)
            if head_hd and len(head_hd) > 0:
                head = head_hd

        contact = {}
        if nickname:
            contact['nickname'] = nickname
        if remark:
            contact['remark'] = remark
        if head:
            contact['photo'] = head
        contact['certification_flag'] = certification_flag

        repeated = 0
        if deleted == 0: 
            self.contacts[username] = contact
        else:
            if username not in self.contacts:
                self.contacts[username] = contact
            else:
                repeated = 1

        if username.endswith("@chatroom"):
            chatroom = model_im.Chatroom()
            chatroom.deleted = deleted
            chatroom.source = source
            chatroom.account_id = self.user_account.account_id
            chatroom.chatroom_id = username
            chatroom.name = nickname
            chatroom.photo = head
            chatroom.type = model_im.CHATROOM_TYPE_NORMAL if contact_type % 2 == 1 else model_im.CHATROOM_TYPE_TEMP

            members, max_count = self._process_parse_group_members(contact_chatroom)
            for member in members:
                if canceller.IsCancellationRequested:
                    break
                try:
                    cm = model_im.ChatroomMember()
                    cm.deleted = deleted
                    cm.source = source
                    cm.account_id = self.user_account.account_id
                    cm.chatroom_id = username
                    cm.member_id = member.get('username')
                    cm.display_name = member.get('display_name')
                    cm.insert_db(self.im)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))

            if len(members) > 0:
                chatroom.owner_id = members[0].get('username')
            chatroom.max_member_count = max_count
            chatroom.member_count = len(members)
            if deleted == 0 or repeated == 0:
                chatroom.insert_db(self.im)
        else:
            ft = model_im.FRIEND_TYPE_STRANGER
            if certification_flag != 0:
                ft = model_im.FRIEND_TYPE_SUBSCRIBE
            elif contact_type % 2 == 1:
                ft = model_im.FRIEND_TYPE_FRIEND
            friend = model_im.Friend()
            friend.deleted = deleted
            friend.source = source
            friend.account_id = self.user_account.account_id
            friend.friend_id = username
            friend.type = ft
            friend.nickname = nickname
            friend.remark = remark
            friend.photo = head
            friend.insert_db(self.im)

    def _parse_user_mm_db(self, node):
        if not node:
            return False
        if canceller.IsCancellationRequested:
            return False

        tables = {}
        db_tables = []
        for username in self.contacts.keys():
            if canceller.IsCancellationRequested:
                break
            m = hashlib.md5()
            m.update(username.encode('utf8'))
            user_hash = m.hexdigest()
            table = 'Chat_' + user_hash
            tables[table] = username
        # add self
        m = hashlib.md5()
        m.update(self.user_account.account_id.encode('utf8'))
        user_hash = m.hexdigest()
        table = 'Chat_' + user_hash
        tables[table] = self.user_account.account_id

        db_path = os.path.join(self.cache_path, 'cache.db')
        self.db_mapping(node.PathWithMountPoint, db_path)
        if not os.path.exists(db_path):
            return False
        db = sqlite3.connect(db_path)
        if db is None:
            return False
        cursor = db.cursor()
        sql = "select name from sqlite_master where (name like 'Chat*_%' escape '*') and type='table'"
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))

        while row is not None:
            if canceller.IsCancellationRequested:
                break
            db_tables.append(row[0])
            row = cursor.fetchone()
        cursor.close()

        for table in db_tables:
            if canceller.IsCancellationRequested:
                break
            if not table.startswith('Chat_'):
                continue
            if table in tables:
                username = tables[table]
            else:
                username = table[5:]
            user_hash = table[5:]

            sql = 'select Message,Type,MesLocalID,Des,CreateTime from {}'.format(table)
            cursor = db.cursor()
            row = None
            try:
                cursor.execute(sql)
                row = cursor.fetchone()
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))

            while row is not None:
                if canceller.IsCancellationRequested:
                    break
                try:
                    msg = self._db_column_get_string_value(row[0])
                    msg_type = self._db_column_get_int_value(row[1], MSG_TYPE_TEXT)
                    msg_local_id = self._db_column_get_string_value(row[2])
                    is_sender = 1 if self._db_column_get_int_value(row[3]) == 0 else 0
                    create_time = self._db_column_get_int_value(row[4], None)
                    self._parse_user_mm_db_with_value(0, node.AbsolutePath, username, msg, msg_type, msg_local_id, is_sender, create_time, user_hash)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))
                row = cursor.fetchone()
            self.im.db_commit()
            cursor.close()
        self.im.db_commit()
        db.close()
        self.db_remove_mapping(db_path)

        self.set_progress(22)
        if self.extract_deleted:
            if canceller.IsCancellationRequested:
                return False
            try:
                db = SQLiteParser.Database.FromNode(node, canceller)
            except Exception as e:
                return False
            if not db:
                return False

            for table in db.Tables:
                if canceller.IsCancellationRequested:
                    break
                if not table.startswith('Chat_'):
                    continue
                if table in tables:
                    username = tables[table]
                else:
                    username = table[5:]
                user_hash = table[5:]
                ts = SQLiteParser.TableSignature(table)
                SQLiteParser.Tools.AddSignatureToTable(ts, "Message", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                for rec in db.ReadTableDeletedRecords(ts, False):
                    if canceller.IsCancellationRequested:
                        break
                    try:
                        msg = self._db_record_get_string_value(rec, 'Message')
                        msg_type = self._db_record_get_int_value(rec, 'Type', MSG_TYPE_TEXT)
                        msg_local_id = self._db_record_get_string_value(rec, 'MesLocalID')
                        is_sender = 1 if self._db_record_get_int_value(rec, 'Des') == 0 else 0
                        create_time = self._db_record_get_int_value(rec, 'CreateTime', None)
                        deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                        self._parse_user_mm_db_with_value(deleted, node.AbsolutePath, username, msg, msg_type, msg_local_id, is_sender, create_time, user_hash)
                    except Exception as e:
                        TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))
                self.im.db_commit()
        return True

    def _parse_user_mm_db_with_value(self, deleted, source, username, msg, msg_type, msg_local_id, is_sender, create_time, user_hash):
        contact = self.contacts.get(username, {})
        certification_flag = contact.get('certification_flag', 0)

        message = model_im.Message()
        message.deleted = deleted
        message.source = source
        message.account_id = self.user_account.account_id
        message.talker_id = username
        message.talker_name = contact.get('nickname')
        message.is_sender = is_sender
        message.msg_id = msg_local_id
        message.type = self._convert_msg_type(msg_type)
        message.send_time = create_time
        if username.endswith("@chatroom"):
            self._process_parse_group_message(msg, msg_type, msg_local_id, is_sender, self.root, user_hash, message)
            message.sender_name = self.contacts.get(message.sender_id, {}).get('nickname')
            message.talker_type = model_im.CHAT_TYPE_GROUP
        else:
            message.sender_id = self.user_account.account_id if is_sender else username
            self._process_parse_friend_message(msg, msg_type, msg_local_id, self.root, user_hash, message)
            message.sender_name = self.contacts.get(message.sender_id, {}).get('nickname')
            message.talker_type = model_im.CHAT_TYPE_FRIEND if certification_flag == 0 else model_im.CHAT_TYPE_SUBSCRIBE
        message.insert_db(self.im)

    def _parse_user_wc_db(self, node):
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

        db_tables = []
        sql = "select name from sqlite_master where (name like 'MyWC01*_%' escape '*') and type='table'"
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))

        while row is not None:
            if canceller.IsCancellationRequested:
                break
            try:
                db_tables.append(row[0])
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))
            row = cursor.fetchone()

        for table in db_tables:
            if canceller.IsCancellationRequested:
                break
            sql = 'select FromUser,Buffer from {}'.format(table)
            row = None
            try:
                cursor.execute(sql)
                row = cursor.fetchone()
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))

            while row is not None:
                if canceller.IsCancellationRequested:
                    break
                try:
                    username = self._db_column_get_string_value(row[0])
                    buffer = self._db_column_get_blob_value(row[1])
                    self._parse_user_wc_db_with_value(0, node.AbsolutePath, username, buffer)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))
                row = cursor.fetchone()
        self.im.db_commit()
        cursor.close()
        db.close()
        self.db_remove_mapping(db_path)

        self.set_progress(40)
        if self.extract_deleted:
            if canceller.IsCancellationRequested:
                return False
            try:
                db = SQLiteParser.Database.FromNode(node, canceller)
            except Exception as e:
                return False
            if not db:
                return False

            tables = [t for t in db.Tables if t.startswith('MyWC01_')]
            for table in tables:
                if canceller.IsCancellationRequested:
                    break
                ts = SQLiteParser.TableSignature(table)
                SQLiteParser.Tools.AddSignatureToTable(ts, "FromUser", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                SQLiteParser.Tools.AddSignatureToTable(ts, "Buffer", SQLiteParser.FieldType.Blob, SQLiteParser.FieldConstraints.NotNull)
            
                for rec in db.ReadTableDeletedRecords(ts, False):
                    if canceller.IsCancellationRequested:
                        break
                    try:
                        username = self._db_record_get_string_value(rec, 'FromUser')
                        buffer = self._db_record_get_blob_value(rec, 'Buffer')
                        deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                        self._parse_user_wc_db_with_value(deleted, node.AbsolutePath, username, buffer)
                    except Exception as e:
                        TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))
            self.im.db_commit()
        return True

    def _parse_user_wc_db_with_value(self, deleted, source, username, buffer):
        if buffer is not None and buffer[:8] == 'bplist00':
            try:
                root_mr = MemoryRange.FromBytes(Convert.FromBase64String(base64.b64encode(buffer)))
                root_mr.seek(0)
                root = BPReader.GetTree(root_mr)
            except Exception as e:
                return

            if not root or not root.Children:
                return

            feed = model_im.Feed()
            feed.deleted = deleted
            feed.source = source
            feed.account_id = self.user_account.account_id
            feed.sender_id = username
            feed.content = self._bpreader_node_get_string_value(root, 'contentDesc', deleted = deleted)
            feed.send_time = self._bpreader_node_get_int_value(root, 'createtime', None)

            if 'locationInfo' in root.Children:
                location_node = root.Children['locationInfo']
                latitude = self._bpreader_node_get_float_value(location_node, 'location_latitude')
                longitude = self._bpreader_node_get_float_value(location_node, 'location_longitude')
                if latitude != 0 or longitude != 0:
                    location = feed.create_location()
                    location.type = model_im.LOCATION_TYPE_GOOGLE
                    location.latitude = latitude
                    location.longitude = longitude
                    location.address = self._bpreader_node_get_string_value(location_node, 'poiName', deleted = feed.deleted)

            if 'contentObj' in root.Children:
                content_node = root.Children['contentObj']
                moment_type = self._bpreader_node_get_int_value(content_node, 'type')
                media_nodes = []
                if 'mediaList' in content_node.Children and content_node.Children['mediaList'].Values:
                    media_nodes = content_node.Children['mediaList'].Values
                    urls = []
                    for media_node in media_nodes:
                        if 'dataUrl' in media_node.Children:
                            data_node = media_node.Children['dataUrl']
                            if 'url' in data_node.Children:
                                urls.append(data_node.Children['url'].Value)
                    if len(urls) > 0:
                        if moment_type == MOMENT_TYPE_VIDEO:
                            feed.video_path = ','.join(str(u) for u in urls)
                        else:
                            feed.image_path = ','.join(str(u) for u in urls)

                if moment_type in [MOMENT_TYPE_MUSIC, MOMENT_TYPE_SHARED]:
                    feed.url = self._bpreader_node_get_string_value(content_node, 'linkUrl', deleted = feed.deleted)
                    feed.url_title = self._bpreader_node_get_string_value(content_node, 'title', deleted = feed.deleted)
                    feed.url_desc = self._bpreader_node_get_string_value(content_node, 'desc', deleted = feed.deleted)

            if 'likeUsers' in root.Children:
                for like_node in root.Children['likeUsers'].Values:
                    if canceller.IsCancellationRequested:
                        break
                    try:
                        sender_id = self._bpreader_node_get_string_value(like_node, 'username', deleted = feed.deleted)
                        if sender_id in [None, '']:
                            continue
                        fl = feed.create_like()
                        fl.sender_id = sender_id
                        fl.sender_name = self._bpreader_node_get_string_value(like_node, 'nickname', deleted = feed.deleted)
                        try:
                            fl.create_time = int(self._bpreader_node_get_int_value(like_node, 'createTime', None))
                        except Exception as e:
                            pass
                        feed.likecount += 1
                    except Exception as e:
                        TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))
            if feed.likecount == 0:
                feed.like_id = 0

            if 'commentUsers' in root.Children:
                for comment_node in root.Children['commentUsers'].Values:
                    if canceller.IsCancellationRequested:
                        break
                    try:
                        sender_id = self._bpreader_node_get_string_value(comment_node, 'username', deleted = feed.deleted)
                        content = self._bpreader_node_get_string_value(comment_node, 'content', deleted = feed.deleted)
                        if type(sender_id) == str and len(sender_id) > 0 and type(content) == str:
                            fc = feed.create_comment()
                            fc.sender_id = sender_id
                            fc.sender_name = self._bpreader_node_get_string_value(comment_node, 'nickname', deleted = feed.deleted)
                            fc.ref_user_id = self._bpreader_node_get_string_value(comment_node, 'refUserName', deleted = feed.deleted)
                            fc.content = content
                            try:
                                fc.create_time = int(self._bpreader_node_get_int_value(comment_node, 'createTime', None))
                            except Exception as e:
                                pass
                            feed.commentcount += 1
                    except Exception as e:
                        TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))
            if feed.commentcount == 0:
                feed.comment_id = 0

            feed.insert_db(self.im)

    def _parse_user_fav_db(self, node):
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
        sql = '''select LocalId,Type,Time,FromUsr,ToUsr,RealChatName,SourceType,Xml
                 from FavoritesItemTable '''
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))

        while row is not None:
            if canceller.IsCancellationRequested:
                break
            try:
                local_id = self._db_column_get_int_value(row[0])
                fav_type = self._db_column_get_int_value(row[1])
                timestamp = self._db_column_get_int_value(row[2])
                from_user = self._db_column_get_string_value(row[3])
                to_user = self._db_column_get_string_value(row[4])
                real_name = self._db_column_get_string_value(row[5])
                source_type = self._db_column_get_int_value(row[6])
                xml = self._db_column_get_string_value(row[7])
                self._parse_user_fav_db_with_value(0, node.AbsolutePath, fav_type, timestamp, from_user, xml)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))
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
                TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))
                return False
            if not db:
                return False

            if 'FavoritesItemTable' in db.Tables:
                ts = SQLiteParser.TableSignature('FavoritesItemTable')
                SQLiteParser.Tools.AddSignatureToTable(ts, "Xml", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                for rec in db.ReadTableDeletedRecords(ts, False):
                    if canceller.IsCancellationRequested:
                        break
                    try:
                        local_id = self._db_record_get_int_value(rec, 'LocalId')
                        fav_type = self._db_record_get_int_value(rec, 'Type')
                        timestamp = self._db_record_get_int_value(rec, 'Time')
                        from_user = self._db_record_get_string_value(rec, 'FromUsr')
                        to_user = self._db_record_get_string_value(rec, 'ToUsr')
                        real_name = self._db_record_get_string_value(rec, 'RealChatName')
                        source_type = self._db_record_get_int_value(rec, 'SourceType')
                        xml = self._db_record_get_string_value(rec, 'Xml')
                        deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                        self._parse_user_fav_db_with_value(deleted, node.AbsolutePath, fav_type, timestamp, from_user, xml)
                    except Exception as e:
                        TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))
                self.im.db_commit()
        return True

    def _parse_user_fav_db_with_value(self, deleted, source, fav_type, timestamp, from_user, xml):
        favorite = model_im.Favorite()
        favorite.source = source
        favorite.deleted = deleted
        favorite.account_id = self.user_account.account_id
        favorite.type = self._convert_fav_type(fav_type)
        favorite.talker = from_user
        favorite.talker_name = self.contacts.get(from_user, {}).get('nickname')
        if from_user.endswith('@chatroom'):
            favorite.talker_type = model_im.CHAT_TYPE_GROUP
        else:
            favorite.talker_type = model_im.CHAT_TYPE_FRIEND
        favorite.timestamp = timestamp
        self._parse_user_fav_xml(xml, favorite)
        favorite.insert_db(self.im)

    def _parse_user_fav_xml(self, xml_str, model):
        xml = None
        try:
            xml = XElement.Parse(xml_str)
        except Exception as e:
            if model.deleted == 0:
                TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))
        if xml is not None and xml.Name.LocalName == 'favitem':
            try:
                fav_type = int(xml.Attribute('type').Value) if xml.Attribute('type') else 0
            except Exception as e:
                fav_type = 0
            if fav_type == FAV_TYPE_TEXT:
                fav_item = model.create_item()
                fav_item.type = fav_type
                if xml.Element('source'):
                    source_info = xml.Element('source')
                    if source_info.Element('createtime'):
                        try:
                            fav_item.timestamp = int(source_info.Element('createtime').Value)
                        except Exception as e:
                            pass
                    if source_info.Element('realchatname'):
                        fav_item.sender = source_info.Element('realchatname').Value
                        fav_item.sender_name = self.contacts.get(fav_item.sender, {}).get('nickname')
                    elif source_info.Element('fromusr'):
                        fav_item.sender = source_info.Element('fromusr').Value
                        fav_item.sender_name = self.contacts.get(fav_item.sender, {}).get('nickname')
                if xml.Element('desc'):
                    fav_item.content = xml.Element('desc').Value
            elif fav_type in [FAV_TYPE_IMAGE, FAV_TYPE_VOICE, FAV_TYPE_VIDEO, FAV_TYPE_VIDEO_2, FAV_TYPE_ATTACHMENT]:
                fav_item = model.create_item()
                fav_item.type = fav_type
                if xml.Element('source'):
                    source_info = xml.Element('source')
                    if source_info.Element('createtime'):
                        try:
                            fav_item.timestamp = int(source_info.Element('createtime').Value)
                        except Exception as e:
                            pass
                    if source_info.Element('realchatname'):
                        fav_item.sender = source_info.Element('realchatname').Value
                        fav_item.sender_name = self.contacts.get(fav_item.sender, {}).get('nickname')
                    elif source_info.Element('fromusr'):
                        fav_item.sender = source_info.Element('fromusr').Value
                        fav_item.sender_name = self.contacts.get(fav_item.sender, {}).get('nickname')
                if xml.Element('title'):
                    fav_item.content = xml.Element('title').Value
                if xml.Element('datalist') and xml.Element('datalist').Element('dataitem'):
                    item = xml.Element('datalist').Element('dataitem')
                    if item.Element('sourcedatapath'):
                        fav_item.media_path = self._parse_user_fav_path(item.Element('sourcedatapath').Value)
                    elif item.Element('sourcethumbpath'):
                        fav_item.media_path = self._parse_user_fav_path(item.Element('sourcedatapath').Value)
            elif fav_type == FAV_TYPE_LINK:
                fav_item = model.create_item()
                fav_item.type = fav_type
                link = fav_item.create_link()
                if xml.Element('source'):
                    source_info = xml.Element('source')
                    if source_info.Element('createtime'):
                        try:
                            fav_item.timestamp = int(source_info.Element('createtime').Value)
                        except Exception as e:
                            pass
                    if source_info.Element('realchatname'):
                        fav_item.sender = source_info.Element('realchatname').Value
                        fav_item.sender_name = self.contacts.get(fav_item.sender, {}).get('nickname')
                    elif source_info.Element('fromusr'):
                        fav_item.sender = source_info.Element('fromusr').Value
                        fav_item.sender_name = self.contacts.get(fav_item.sender, {}).get('nickname')
                    if source_info.Element('link'):
                        link.url = source_info.Element('link').Value
                if xml.Element('weburlitem'):
                    weburlitem = xml.Element('weburlitem')
                    if weburlitem.Element('pagetitle'):
                        link.title = weburlitem.Element('pagetitle').Value
                    if weburlitem.Element('pagethumb_url'):
                        link.image = weburlitem.Element('pagethumb_url').Value
            elif fav_type == FAV_TYPE_LOCATION:
                fav_item = model.create_item()
                fav_item.type = fav_type
                if xml.Element('source'):
                    source_info = xml.Element('source')
                    if source_info.Element('createtime'):
                        try:
                            fav_item.timestamp = int(source_info.Element('createtime').Value)
                        except Exception as e:
                            pass
                    if source_info.Element('realchatname'):
                        fav_item.sender = source_info.Element('realchatname').Value
                        fav_item.sender_name = self.contacts.get(fav_item.sender, {}).get('nickname')
                    elif source_info.Element('fromusr'):
                        fav_item.sender = source_info.Element('fromusr').Value
                        fav_item.sender_name = self.contacts.get(fav_item.sender, {}).get('nickname')
                if xml.Element('locitem'):
                    latitude = 0
                    longitude = 0
                    locitem = xml.Element('locitem')
                    if locitem.Element('lat'):
                        try:
                            latitude = float(locitem.Element('lat').Value)
                        except Exception as e:
                            pass
                    if locitem.Element('lng'):
                        try:
                            longitude = float(locitem.Element('lng').Value)
                        except Exception as e:
                            pass
                    if latitude != 0 or longitude != 0:
                        location = fav_item.create_location()
                        location.type = model_im.LOCATION_TYPE_GOOGLE
                        location.latitude = latitude
                        location.longitude = longitude
                        if locitem.Element('label'):
                            location.address = locitem.Element('label').Value
                        if locitem.Element('poiname'):
                            location.address = locitem.Element('poiname').Value
            elif fav_type == FAV_TYPE_CHAT:
                if xml.Element('datalist'):
                    for item in xml.Element('datalist').Elements('dataitem'):
                        fav_item = model.create_item()
                        if item.Attribute('datatype'):
                            try:
                                fav_item.type = int(item.Attribute('datatype').Value)
                            except Exception as e:
                                 pass
                        if item.Element('dataitemsource'):
                            source_info = item.Element('dataitemsource')
                            if source_info.Element('createtime'):
                                try:
                                    fav_item.timestamp = int(source_info.Element('createtime').Value)
                                except Exception as e:
                                    pass
                            if source_info.Element('realchatname'):
                                fav_item.sender = source_info.Element('realchatname').Value
                                fav_item.sender_name = self.contacts.get(fav_item.sender, {}).get('nickname')
                            elif source_info.Element('fromusr'):
                                fav_item.sender = source_info.Element('fromusr').Value
                                fav_item.sender_name = self.contacts.get(fav_item.sender, {}).get('nickname')
                        if fav_item.type == FAV_TYPE_TEXT:
                            if item.Element('datadesc'):
                                fav_item.content = item.Element('datadesc').Value
                        elif fav_item.type in [FAV_TYPE_IMAGE, FAV_TYPE_VOICE, FAV_TYPE_VIDEO, FAV_TYPE_VIDEO_2, FAV_TYPE_ATTACHMENT]:
                            if item.Element('sourcedatapath'):
                                fav_item.media_path = self._parse_user_fav_path(item.Element('sourcedatapath').Value)
                            elif item.Element('sourcethumbpath'):
                                fav_item.media_path = self._parse_user_fav_path(item.Element('sourcedatapath').Value)
                        elif fav_item.type == FAV_TYPE_LINK:
                            link = fav_item.create_link()
                            if item.Element('dataitemsource'):
                                source_info = item.Element('dataitemsource')
                                if source_info.Element('link'):
                                    link.url = source_info.Element('link').Value
                            if item.Element('weburlitem') and item.Element('weburlitem').Element('pagetitle'):
                                link.title = item.Element('weburlitem').Element('pagetitle').Value
                            if item.Element('sourcethumbpath'):
                                link.image = self._parse_user_fav_path(item.Element('sourcethumbpath').Value)
                        elif fav_item.type == FAV_TYPE_LOCATION:
                            if item.Element('locitem'):
                                latitude = 0
                                longitude = 0
                                locitem = item.Element('locitem')
                                if locitem.Element('lat'):
                                    try:
                                        latitude = float(locitem.Element('lat').Value)
                                    except Exception as e:
                                        pass
                                if locitem.Element('lng'):
                                    try:
                                        longitude = float(locitem.Element('lng').Value)
                                    except Exception as e:
                                        pass
                                if latitude != 0 or longitude != 0:
                                    location = fav_item.create_location()
                                    location.type = model_im.LOCATION_TYPE_GOOGLE
                                    location.latitude = latitude
                                    location.longitude = longitude
                                    if locitem.Element('label'):
                                        location.address = locitem.Element('label').Value
                                    if locitem.Element('poiname'):
                                        location.address = locitem.Element('poiname').Value
                        else:
                            fav_item.content = xml_str
                        if item.Element('datasrcname'):
                            fav_item.sender_name = item.Element('datasrcname').Value
            else:
                fav_item = model.create_item()
                fav_item.type = fav_type
                fav_item.content = xml_str
        return True

    def _parse_user_fav_path(self, path):
        if path.startswith('/var'):
            return '/private' + path
        return path

    def _parse_user_search(self, node):
        if node is None:
            return False
        try:
            node.Data.seek(0)
            content = node.read()
            if content[-2:] == '\x10\x04':
                index = 1
                while index + 5 < len(content):
                    if canceller.IsCancellationRequested:
                        break
                    index += 2
                    size = ord(content[index])
                    index += 1
                    if index + size < len(content):
                        key = content[index:index+size].decode('utf-8')
                        if key is not None and len(key) > 0:
                            search = model_im.Search()
                            search.account_id = self.user_account.account_id
                            search.key = key
                            search.source = node.AbsolutePath
                            search.insert_db(self.im)
                    index += size
                    if content[index:index+2] != '\x10\x04':
                        break
                    index += 2
        except e as Exception:
            pass
        self.im.db_commit()

    def _parse_user_fts_db(self, node):
        if node is None:
            return False
        if canceller.IsCancellationRequested:
            return False

        username_ids = {}

        db_path = os.path.join(self.cache_path, 'cache.db')
        self.db_mapping(node.PathWithMountPoint, db_path)
        if not os.path.exists(db_path):
            return False
        db = SQLite.SQLiteConnection('Data Source = {}'.format(db_path))
        if db is None:
            return False
        db.Open()

        sql = 'select UsrName,usernameid from fts_username_id'
        db_cmd = SQLite.SQLiteCommand(sql, db)
        reader = None
        try:
            reader = db_cmd.ExecuteReader()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))

        if reader is not None:
            while reader.Read():
                if canceller.IsCancellationRequested:
                    break
                username = self._db_reader_get_string_value(reader, 0)
                id = self._db_reader_get_int_value(reader, 1)
                if username != '' and id != 0:
                    username_ids[id] = username
            reader.Close()
            del reader
        db_cmd.Dispose()

        db_tables = []
        sql = 'select name from sqlite_master'
        db_cmd = SQLite.SQLiteCommand(sql, db)
        reader = None
        try:
            reader = db_cmd.ExecuteReader()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))

        if reader is not None:
            while reader.Read():
                if canceller.IsCancellationRequested:
                    break
                table = self._db_reader_get_string_value(reader, 0)
                if table.startswith('fts_message_table_') and table.endswith('_content'):
                    db_tables.append(table)
            reader.Close()
            del reader
        db_cmd.Dispose()

        for table in db_tables:
            if canceller.IsCancellationRequested:
                break
            sql = 'select c0usernameid,c3Message from {}'.format(table)
            db_cmd = SQLite.SQLiteCommand(sql, db)
            reader = None
            try:
                reader = db_cmd.ExecuteReader()
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))

            if reader is not None:
                while reader.Read():
                    if canceller.IsCancellationRequested:
                        break
                    try:
                        id = self._db_reader_get_int_value(reader, 0)
                        content = self._db_reader_get_string_value(reader, 1)
                        if id in username_ids:
                            username = username_ids.get(id)
                        else:
                            username = id
                        self._parse_user_fts_db_with_value(1, node.AbsolutePath, username, content)
                    except Exception as e:
                        TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))
                self.im.db_commit()
                reader.Close()
                del reader
            db_cmd.Dispose()
        db.Close()
        self.db_remove_mapping(db_path)

        self.set_progress(57)
        if self.extract_deleted:
            if canceller.IsCancellationRequested:
                return False
            try:
                db = SQLiteParser.Database.FromNode(node, canceller)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))
                return False
            if not db:
                return False

            if 'fts_username_id' in db.Tables:
                ts = SQLiteParser.TableSignature('fts_username_id')
                SQLiteParser.Tools.AddSignatureToTable(ts, "UsrName", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                SQLiteParser.Tools.AddSignatureToTable(ts, "usernameid", SQLiteParser.FieldType.Int, SQLiteParser.FieldConstraints.NotNull)
                for rec in db.ReadTableDeletedRecords(ts, False):
                    if canceller.IsCancellationRequested:
                        break
                    username = self._db_record_get_string_value(rec, 'UsrName', '')
                    id = self._db_record_get_int_value(rec, 'usernameid')
                    if username != '' and id != 0 and id not in username_ids:
                        username_ids[id] = username

            tables = [t for t in db.Tables if t.startswith('fts_message_table_') and t.endswith('_content')]
            for table in tables:
                if canceller.IsCancellationRequested:
                    break
                ts = SQLiteParser.TableSignature(table)
                SQLiteParser.Tools.AddSignatureToTable(ts, "c0usernameid", SQLiteParser.FieldType.Int, SQLiteParser.FieldConstraints.NotNull)
                SQLiteParser.Tools.AddSignatureToTable(ts, "c3Message", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                for rec in db.ReadTableDeletedRecords(ts, False):
                    if canceller.IsCancellationRequested:
                        break
                    id = self._db_record_get_int_value(rec, 'c0usernameid', 0)
                    if id not in username_ids:
                        continue
                    username = username_ids.get(id)
                    content = self._db_record_get_string_value(rec, 'c3Message', '')
                    self._parse_user_fts_db_with_value(1, node.AbsolutePath, username, content)
                self.im.db_commit()
        return True

    def _parse_user_fts_db_with_value(self, deleted, source, username, content):
        contact = self.contacts.get(username, {})
        certification_flag = contact.get('certification_flag', 0)
                    
        message = model_im.Message()
        message.deleted = deleted
        message.source = source
        message.account_id = self.user_account.account_id
        message.talker_id = username
        message.talker_name = contact.get('nickname')
        message.content = content
        if username.endswith('@chatroom'):
            message.talker_type = model_im.CHAT_TYPE_GROUP
        else:
            message.talker_type = model_im.CHAT_TYPE_FRIEND if certification_flag == 0 else model_im.CHAT_TYPE_SUBSCRIBE
        message.insert_db(self.im)

    @staticmethod
    def _process_parse_contact_remark(blob):
        nickname = ''
        alias = ''
        remark = ''

        try:
            index = 0
            while index + 2 < len(blob):
                flag = ord(blob[index])
                size = ord(blob[index + 1])
                if index + 2 + size > len(blob):
                    break
                content = blob[index + 2: index + 2 + size].decode('utf-8')
                if flag == 0x0a:  # nickname
                    nickname = content
                elif flag == 0x12:  # alias
                    alias = content
                elif flag == 0x1a:  # remark
                    remark = content
                index += 2 + size
        except Exception as e:
            pass
        return nickname, alias, remark

    @staticmethod
    def _process_parse_contact_head(blob):
        head = None
        head_hd = None

        try:
            index = 2
            while index + 1 < len(blob):
                flag = ord(blob[index])
                size = ord(blob[index + 1])
                if size > 0:
                    index += 2
                    if ord(blob[index]) != 0x68:
                        index += 1
                    if index + size > len(blob):
                        break

                    content = blob[index: index + size].decode('utf-8')
                    if flag == 0x12:
                        head = content
                    elif flag == 0x1a:
                        head_hd = content

                    index += size
                else:
                    index += 2
        except Exception as e:
            pass

        return head, head_hd

    @staticmethod
    def _process_parse_group_members(blob):
        members = []
        max_count = 0

        prefix = b'<RoomData>'
        suffix = b'</RoomData>'
        if blob is not None and prefix in blob and suffix in blob:
            index_begin = blob.index(prefix)
            index_end = blob.index(suffix) + len(suffix)
            content = blob[index_begin:index_end].decode('utf-8')
            ms = []
            try:
                xml = XElement.Parse(content)
                max_count = int(xml.Element('MaxCount').Value)
                ms = Enumerable.ToList[XElement](XPathExtensions.XPathSelectElements(xml,"Member[@UserName]"))
            except Exception as e:
                pass
            for m in ms:
                if canceller.IsCancellationRequested:
                    break
                username = None
                display_name = None
                if m.Attribute('UserName'):
                    username = m.Attribute('UserName').Value
                if m.Element("DisplayName"):
                    display_name = m.Element("DisplayName").Value
                if username is not None:
                    members.append({'username': username, 'display_name': display_name})
        return members, max_count

    def _process_parse_friend_message(self, msg, msg_type, msg_local_id, user_node, friend_hash, model):
        content = msg
        img_path = ''

        if msg_type in [MSG_TYPE_TEXT, MSG_TYPE_CONTACT_CARD, MSG_TYPE_EMOJI]:
            pass
        elif msg_type == MSG_TYPE_IMAGE:
            content = '[图片]'
            node = user_node.GetByPath('Img/{0}/{1}.pic'.format(friend_hash, msg_local_id))
            node_thum = user_node.GetByPath('Img/{0}/{1}.pic_thum'.format(friend_hash, msg_local_id))
            if node is not None:
                img_path = node.AbsolutePath
            elif node_thum is not None:
                img_path = node_thum.AbsolutePath
            else:
                img_path = user_node.AbsolutePath + '/Img/{0}/{1}.pic'.format(friend_hash, msg_local_id)
        elif msg_type == MSG_TYPE_VOICE:
            content = '[语音]'
            node = user_node.GetByPath('Audio/{0}/{1}.aud'.format(friend_hash, msg_local_id))
            if node is not None:
                img_path = node.AbsolutePath 
            else:
                img_path = user_node.AbsolutePath + '/Audio/{0}/{1}.aud'.format(friend_hash, msg_local_id)
        elif msg_type == MSG_TYPE_VIDEO or msg_type == MSG_TYPE_VIDEO_2:
            content = '[视频]'
            node = user_node.GetByPath('Video/{0}/{1}.mp4'.format(friend_hash, msg_local_id))
            node_thum = user_node.GetByPath('Video/{0}/{1}.video_thum'.format(friend_hash, msg_local_id))
            if node is not None:
                img_path = node.AbsolutePath
            elif node_thum is not None:
                model.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                img_path = node_thum.AbsolutePath
            else:
                img_path = user_node.AbsolutePath + '/Video/{0}/{1}.mp4'.format(friend_hash, msg_local_id)
        elif msg_type == MSG_TYPE_LOCATION:
            if model is not None:
                self._process_parse_message_location(content, model)
            node = user_node.GetByPath('Location/{0}/{1}.pic_thum'.format(friend_hash, msg_local_id))
            if node is not None:
                img_path = node.AbsolutePath
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
        model.media_path = img_path

    def _process_parse_group_message(self, msg, msg_type, msg_local_id, is_sender, user_node, group_hash, model):
        sender_id = self.user_account.account_id
        content = msg

        if not is_sender:
            index = msg.find(':\n')
            if index != -1:
                sender_id = msg[:index]
                content = msg[index+2:]

        model.sender_id = sender_id
        self._process_parse_friend_message(content, msg_type, msg_local_id, user_node, group_hash, model)
