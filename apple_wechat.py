#coding=utf-8
import PA_runtime
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions
from PA_runtime import *
import System.Data.SQLite as SQLite

import os
import hashlib
import json
import model_im
import gc
import string
import sqlite3
import shutil
import datetime

# EnterPoint: analyze_wechat(root, extract_deleted, extract_source):
# Patterns: '/DB/MM\.sqlite$'
# Models: Common.User, Common.Friend, Common.Group, Generic.Chat, Common.MomentContent

# app数据库版本
VERSION_APP_VALUE = 1

# 消息类型
MSG_TYPE_TEXT = 1
MSG_TYPE_IMAGE = 3
MSG_TYPE_VOICE = 34
MSG_TYPE_CONTACT_CARD = 42
MSG_TYPE_VIDEO = 43
MSG_TYPE_VIDEO_2 = 62
MSG_TYPE_EMOJI = 47
MSG_TYPE_LOCATION = 48
MSG_TYPE_LINK = 49
MSG_TYPE_VOIP = 50
MSG_TYPE_VOIP_GROUP = 64
MSG_TYPE_SYSTEM = 10000
MSG_TYPE_SYSTEM_2 = 10002

# 朋友圈类型
MOMENT_TYPE_IMAGE = 1  # 正常文字图片
MOMENT_TYPE_TEXT_ONLY = 2  # 纯文字
MOMENT_TYPE_SHARED = 3  # 分享
MOMENT_TYPE_MUSIC = 4  # 带音乐的（存的是封面）
MOMENT_TYPE_EMOJI = 10  # 分享了表情包
MOMENT_TYPE_VIDEO = 15  # 视频

def analyze_wechat(root, extract_deleted, extract_source):
    pr = ParserResults()
    pr.Categories = DescripCategories.Wechat #声明这是微信应用解析的数据集
    models = WeChatParser(root, extract_deleted, extract_source).parse()
    mlm = ModelListMerger()
    
    pr.Models.AddRange(list(mlm.GetUnique(models)))
    pr.Build('微信')
    return pr


def execute(node,extracteDeleted):
    return analyze_wechat(node, extracteDeleted, False)


class WeChatParser(model_im.IM):
    
    def __init__(self, node, extract_deleted, extract_source):
        super(WeChatParser, self).__init__()
        self.root = node.Parent.Parent
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source

    def parse(self):
        self.user_hash = self.get_user_hash()
        self.cache_path = os.path.join(ds.OpenCachePath('wechat'), self.get_user_guid())
        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)
        self.cache_db = os.path.join(self.cache_path, self.user_hash + '.db')

        if self.need_parse(self.cache_db, VERSION_APP_VALUE):
            self.db_create(self.cache_db)

            self.contacts = {}
            self.user_account = model_im.Account()

            if not self._get_user_from_setting(self.root.GetByPath('mmsetting.archive')):
                self.user_account.account_id = self.user_hash
                self.db_insert_table_account(self.user_account)
                self.db_commit()

            self._parse_user_contact_db(self.root.GetByPath('/DB/WCDB_Contact.sqlite'))
            self._parse_user_mm_db(self.root.GetByPath('/DB/MM.sqlite'))
            self._parse_user_wc_db(self.root.GetByPath('/wc/wc005_008.db'))
            self._parse_user_fts_db(self.root.GetByPath('/fts/fts_message.db'))

            # 数据库填充完毕，请将中间数据库版本和app数据库版本插入数据库，用来检测app是否需要重新解析
            self.db_insert_table_version(model_im.VERSION_KEY_DB, model_im.VERSION_VALUE_DB)
            self.db_insert_table_version(model_im.VERSION_KEY_APP, VERSION_APP_VALUE)
            self.db_commit()
            self.db_close()
            gc.collect()

        models = self.get_models_from_cache_db()
        return models

    def get_models_from_cache_db(self):
        models = model_im.GenerateModel(self.cache_db).get_models()
        return models

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
        except:
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
        self.db_insert_table_account(self.user_account)
        self.db_commit()

        return True

    def _parse_user_contact_db(self, node):
        if node is None:
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
            print(e)

        while row is not None:
            username = self._db_column_get_string_value(row[0])
            if username not in [None, '']:
                contact_type = self._db_column_get_int_value(row[1])
                certification_flag = self._db_column_get_int_value(row[2])
                contact_remark = self._db_column_get_blob_value(row[3])
                contact_head_image = self._db_column_get_blob_value(row[4])
                contact_chatroom = self._db_column_get_blob_value(row[5])
                self._parse_user_contact_db_with_value(0, node.AbsolutePath, username, contact_type, certification_flag, contact_remark, contact_head_image, contact_chatroom)
            row = cursor.fetchone()
        self.db_commit()
        cursor.close()
        db.close()
        self.db_remove_mapping(db_path)

        if self.extract_deleted:
            db = SQLiteParser.Database.FromNode(node)
            if not db:
                return False

            if 'Friend' in db.Tables:
                ts = SQLiteParser.TableSignature('Friend')
                SQLiteParser.Tools.AddSignatureToTable(ts, "userName", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                for rec in db.ReadTableDeletedRecords(ts, False):
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
                self.db_commit()
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
                cm = model_im.ChatroomMember()
                cm.deleted = deleted
                cm.source = source
                cm.account_id = self.user_account.account_id
                cm.chatroom_id = username
                cm.member_id = member.get('username')
                cm.display_name = member.get('display_name')
                self.db_insert_table_chatroom_member(cm)

            if len(members) > 0:
                chatroom.owner_id = members[0].get('username')
            chatroom.max_member_count = max_count
            chatroom.member_count = len(members)
            if deleted == 0 or repeated == 0:
                self.db_insert_table_chatroom(chatroom)
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
            self.db_insert_table_friend(friend)

    def _parse_user_mm_db(self, node):
        if not node:
            return False

        tables = {}
        db_tables = []
        for username in self.contacts.keys():
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
            print(e)

        while row is not None:
            db_tables.append(row[0])
            row = cursor.fetchone()
        cursor.close()

        for table in db_tables:
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
                print(e)

            while row is not None:
                msg = self._db_column_get_string_value(row[0])
                msg_type = self._db_column_get_int_value(row[1], MSG_TYPE_TEXT)
                msg_local_id = self._db_column_get_string_value(row[2])
                is_sender = 1 if self._db_column_get_int_value(row[3]) == 0 else 0
                create_time = self._db_column_get_int_value(row[4], None)
                self._parse_user_mm_db_with_value(0, node.AbsolutePath, username, msg, msg_type, msg_local_id, is_sender, create_time, user_hash)
                row = cursor.fetchone()
            self.db_commit()
            cursor.close()
        self.db_commit()
        db.close()
        self.db_remove_mapping(db_path)

        if self.extract_deleted:
            db = SQLiteParser.Database.FromNode(node)
            if not db:
                return False

            for table in db.Tables:
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
                    msg = self._db_record_get_string_value(rec, 'Message')
                    msg_type = self._db_record_get_int_value(rec, 'Type', MSG_TYPE_TEXT)
                    msg_local_id = self._db_record_get_string_value(rec, 'MesLocalID')
                    is_sender = 1 if self._db_record_get_int_value(rec, 'Des') == 0 else 0
                    create_time = self._db_record_get_int_value(rec, 'CreateTime', None)
                    deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    self._parse_user_mm_db_with_value(deleted, node.AbsolutePath, username, msg, msg_type, msg_local_id, is_sender, create_time, user_hash)
                self.db_commit()
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
        self.db_insert_table_message(message)

    def _parse_user_wc_db(self, node):
        if node is None:
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
            print(e)

        while row is not None:
            db_tables.append(row[0])
            row = cursor.fetchone()

        for table in db_tables:
            sql = 'select FromUser,Buffer from {}'.format(table)
            row = None
            try:
                cursor.execute(sql)
                row = cursor.fetchone()
            except Exception as e:
                print(e)

            while row is not None:
                username = self._db_column_get_string_value(row[0])
                buffer = self._db_column_get_blob_value(row[1])
                self._parse_user_wc_db_with_value(0, node.AbsolutePath, username, buffer)
                row = cursor.fetchone()
            self.db_commit()
        cursor.close()
        db.close()
        self.db_remove_mapping(db_path)

        if self.extract_deleted:
            db = SQLiteParser.Database.FromNode(node)
            if not db:
                return False

            tables = [t for t in db.Tables if t.startswith('MyWC01_')]
            for table in tables:
                ts = SQLiteParser.TableSignature(table)
                SQLiteParser.Tools.AddSignatureToTable(ts, "FromUser", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                SQLiteParser.Tools.AddSignatureToTable(ts, "Buffer", SQLiteParser.FieldType.Blob, SQLiteParser.FieldConstraints.NotNull)
            
                for rec in db.ReadTableDeletedRecords(ts, False):
                    username = self._db_record_get_string_value(rec, 'FromUser')
                    buffer = self._db_record_get_blob_value(rec, 'Buffer')
                    deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    self._parse_user_wc_db_with_value(deleted, node.AbsolutePath, username, buffer)
                self.db_commit()
        return True

    def _parse_user_wc_db_with_value(self, deleted, source, username, buffer):
        if buffer is not None and buffer[:8] == 'bplist00':
            try:
                root = BPReader.GetTree(root_mr)
            except:
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
                location = model_im.Location()
                location.deleted = feed.deleted
                location.source = source
                location.latitude = self._bpreader_node_get_float_value(location_node, 'location_latitude')
                location.longitude = self._bpreader_node_get_float_value(location_node, 'location_longitude')
                location.address = self._bpreader_node_get_string_value(location_node, 'poiName', deleted = feed.deleted)
                self.db_insert_table_location(location)
                feed.location = location.location_id

            if 'contentObj' in root.Children:
                content_node = root.Children['contentObj']
                feed.type = self._bpreader_node_get_int_value(content_node, 'type')
                media_nodes = []
                if 'mediaList' in content_node.Children and content_node.Children['mediaList'].Values:
                    media_nodes = content_node.Children['mediaList'].Values
                    urls = []
                    preview_urls = []
                    for media_node in media_nodes:
                        if 'dataUrl' in media_node.Children:
                            data_node = media_node.Children['dataUrl']
                            if 'url' in data_node.Children:
                                urls.append(data_node.Children['url'].Value)
                        if 'previewUrls' in media_node.Children:
                            for url_node in media_node.Children['previewUrls'].Values:
                                if 'url' in url_node.Children:
                                    preview_urls.append(url_node.Children['url'].Value)
                    feed.urls = json.dumps(urls)
                    feed.preview_urls = json.dumps(preview_urls)

                if feed.type == MOMENT_TYPE_MUSIC:
                    feed.attachment_title = self._bpreader_node_get_string_value(content_node, 'title', deleted = feed.deleted)
                    feed.attachment_link = self._bpreader_node_get_string_value(content_node, 'linkUrl', deleted = feed.deleted)
                    feed.attachment_desc = self._bpreader_node_get_string_value(content_node, 'desc', deleted = feed.deleted)
                elif feed.type == MOMENT_TYPE_SHARED:
                    for media_node in media_nodes:
                        feed.attachment_title = self._bpreader_node_get_string_value(media_node, 'title', deleted = feed.deleted)

            likes = []
            if 'likeUsers' in root.Children:
                for like_node in root.Children['likeUsers'].Values:
                    sender_id = self._bpreader_node_get_string_value(like_node, 'username', deleted = feed.deleted)
                    if len(sender_id) > 0:
                        fl = model_im.FeedLike()
                        fl.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                        fl.source = node.AbsolutePath
                        fl.sender_id = sender_id
                        fl.sender_name = self._bpreader_node_get_string_value(like_node, 'nickname', deleted = feed.deleted)
                        try:
                            fl.create_time = int(self._bpreader_node_get_int_value(like_node, 'createTime', None))
                        except Exception as e:
                            pass
                        try:
                            self.db_insert_table_feed_like(fl)
                            likes.append(fl.like_id)
                        except Exception as e:
                            pass
            feed.likes = ','.join(str(item) for item in likes)

            comments = []
            if 'commentUsers' in root.Children:
                for comment_node in root.Children['commentUsers'].Values:
                    sender_id = self._bpreader_node_get_string_value(comment_node, 'username', deleted = feed.deleted)
                    content = self._bpreader_node_get_string_value(comment_node, 'content', deleted = feed.deleted)
                    if type(sender_id) == str and len(sender_id) > 0 and type(content) == str:
                        fc = model_im.FeedComment()
                        fc.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                        fc.source = node.AbsolutePath
                        fc.sender_id = sender_id
                        fc.sender_name = self._bpreader_node_get_string_value(comment_node, 'nickname', deleted = feed.deleted)
                        fc.ref_user_id = self._bpreader_node_get_string_value(comment_node, 'refUserName', deleted = feed.deleted)
                        fc.content = content
                        fc.create_time = self._bpreader_node_get_int_value(comment_node, 'createTime', None)
                        try:
                            self.db_insert_table_feed_comment(fc)
                            comments.append(fc.comment_id)
                        except Exception as e:
                            pass
            feed.comments = ','.join(str(item) for item in comments)
            self.db_insert_table_feed(feed)

    def _parse_user_fts_db(self, node):
        if node is None:
            return False

        username_ids = {}

        db_path = os.path.join(self.cache_path, 'cache.db')
        self.db_mapping(node.PathWithMountPoint, db_path)
        if not os.path.exists(db_path):
            return False
        db = sqlite3.connect(db_path)
        if db is None:
            return False
        cursor = db.cursor()

        sql = 'select UsrName,usernameid from fts_username_id'
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            username = self._db_column_get_string_value(row[0])
            id = self._db_column_get_int_value(row[1])
            if username != '' and id != 0:
                username_ids[id] = username

            row = cursor.fetchone()

        db_tables = []
        sql = 'select name from sqlite_master'
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            if row[0].startswith('fts_message_table_') and row[0].endswith('_content'):
                db_tables.append(row[0])
            row = cursor.fetchone()

        for table in db_tables:
            sql = 'select c0usernameid,c3Message from {}'.format(table)
            row = None
            try:
                cursor.execute(sql)
                row = cursor.fetchone()
            except Exception as e:
                print(e)

            while row is not None:
                id = self._db_column_get_int_value(row[0])
                content = self._db_column_get_string_value(row[1])
                if id in username_ids:
                    username = username_ids.get(id)
                    self._parse_user_fts_db_with_value(1, node.AbsolutePath, username, content)
                row = cursor.fetchone()
            self.db_commit()
        cursor.close()
        db.close()
        self.db_remove_mapping(db_path)

        if self.extract_deleted:
            db = SQLiteParser.Database.FromNode(node)
            if not db:
                return False

            if 'fts_username_id' in db.Tables:
                ts = SQLiteParser.TableSignature('fts_username_id')
                SQLiteParser.Tools.AddSignatureToTable(ts, "UsrName", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                SQLiteParser.Tools.AddSignatureToTable(ts, "usernameid", SQLiteParser.FieldType.Int, SQLiteParser.FieldConstraints.NotNull)
                for rec in db.ReadTableDeletedRecords(ts, False):
                    username = self._db_record_get_string_value(rec, 'UsrName', '')
                    id = self._db_record_get_int_value(rec, 'usernameid')
                    if username != '' and id != 0 and id not in username_ids:
                        username_ids[id] = username

            tables = [t for t in db.Tables if t.startswith('fts_message_table_') and t.endswith('_content')]
            for table in tables:
                ts = SQLiteParser.TableSignature(table)
                SQLiteParser.Tools.AddSignatureToTable(ts, "c0usernameid", SQLiteParser.FieldType.Int, SQLiteParser.FieldConstraints.NotNull)
                SQLiteParser.Tools.AddSignatureToTable(ts, "c3Message", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                for rec in db.ReadTableDeletedRecords(ts, False):
                    id = self._db_record_get_int_value(rec, 'c0usernameid', 0)
                    if id not in username_ids:
                        continue
                    username = username_ids.get(id)
                    content = self._db_record_get_string_value(rec, 'c3Message', '')
                    self._parse_user_fts_db_with_value(1, node.AbsolutePath, username, content)
                self.db_commit()
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
        self.db_insert_table_message(message)

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

        if msg_type == MSG_TYPE_IMAGE:
            node = user_node.GetByPath('Img/{0}/{1}.pic'.format(friend_hash, msg_local_id))
            if node is not None:
                img_path = node.AbsolutePath
            else:
                model.deleted = 1
        elif msg_type == MSG_TYPE_VOICE:
            node = user_node.GetByPath('Audio/{0}/{1}.aud'.format(friend_hash, msg_local_id))
            if node is not None:
                img_path = node.AbsolutePath 
            else:
                model.deleted = 1
        #elif msg_type == MSG_TYPE_CONTACT_CARD:
        #    pass
        elif msg_type == MSG_TYPE_VIDEO or msg_type == MSG_TYPE_VIDEO_2:
            node = user_node.GetByPath('Video/{0}/{1}.mp4'.format(friend_hash, msg_local_id))
            if node is None:
                model.deleted = 1
                node = user_node.GetByPath('Video/{0}/{1}.video_thum'.format(friend_hash, msg_local_id))
            if node is not None:
                img_path = node.AbsolutePath
        elif msg_type == MSG_TYPE_EMOJI:
            pass
        elif msg_type == MSG_TYPE_LOCATION:
            if model is not None:
                location = model_im.Location()
                location.deleted = model.deleted
                location.source = model.source
                self._process_parse_message_location(content, location)
                model.extra_id = location.location_id
                self.db_insert_table_location(location)
            node = user_node.GetByPath('Location/{0}/{1}.pic_thum'.format(friend_hash, msg_local_id))
            if node is not None:
                img_path = node.AbsolutePath
        elif msg_type == MSG_TYPE_LINK:
            content = self._process_parse_message_link(content, model)
        elif msg_type == MSG_TYPE_VOIP:
            content = self._process_parse_message_voip(content)
        elif msg_type == MSG_TYPE_VOIP_GROUP:
            content = self._process_parse_message_voip_group(content)
        elif msg_type == MSG_TYPE_SYSTEM or msg_type == MSG_TYPE_SYSTEM_2:
            pass
        else:  # MSG_TYPE_TEXT
            pass

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

    def _process_parse_message_link(self, xml_str, model):
        content = ''
        xml = None
        try:
            xml = XElement.Parse(xml_str)
        except Exception as e:
            pass
        if xml is not None:
            if xml.Name.LocalName == 'msg':
                appmsg = xml.Element('appmsg')
                if appmsg is not None:
                    try:
                        msg_type = int(appmsg.Element('type').Value) if appmsg.Element('type') else 0
                    except Exception as e:
                        msg_type = 0
                    if msg_type in [2000, 2001]:
                        deal, sender_id = self._process_parse_message_deal(xml)
                        deal.deleted = model.deleted
                        deal.source = model.source
                        if deal.type == model_im.DEAL_TYPE_RED_ENVELPOE:
                            model.type = model_im.MESSAGE_CONTENT_TYPE_RED_ENVELPOE
                            model.extra_id = deal.deal_id
                            if model.sender_id in [None, ''] and sender_id not in [None, '']:
                                model.sender_id = sender_id
                            self.db_insert_table_deal(deal)
                        elif deal.type == model_im.DEAL_TYPE_RECEIPT:
                            model.type = model_im.MESSAGE_CONTENT_TYPE_RECEIPT
                            model.extra_id = deal.deal_id
                            if model.sender_id in [None, ''] and sender_id not in [None, '']:
                                model.sender_id = sender_id
                            self.db_insert_table_deal(deal)
                        elif deal.type == model_im.DEAL_TYPE_AA_RECEIPT:
                            model.type = model_im.MESSAGE_CONTENT_TYPE_AA_RECEIPT
                            model.extra_id = deal.deal_id
                            if model.sender_id in [None, ''] and sender_id not in [None, '']:
                                model.sender_id = sender_id
                            self.db_insert_table_deal(deal)
                    else:
                        msg_title = appmsg.Element('title').Value if appmsg.Element('title') else ''
                        mmreader = appmsg.Element('mmreader')
                        if mmreader:
                            category = mmreader.Element('category')
                            if category and category.Element('item'):
                                item = category.Element('item')
                                if item.Element('title'):
                                    content += '[标题]' + item.Element('title').Value + '\n'
                                if item.Element('digest'):
                                    content += '[内容]' + item.Element('digest').Value + '\n'
                                if item.Element('url'):
                                    content += '[链接]' + item.Element('url').Value + '\n'
                        else:
                            if appmsg.Element('title'):
                                content += '[标题]' + appmsg.Element('title').Value + '\n'
                            if appmsg.Element('des'):
                                content += '[内容]' + appmsg.Element('des').Value + '\n'
                            if appmsg.Element('url'):
                                content += '[链接]' + appmsg.Element('url').Value + '\n'
                            appinfo = xml.Element('appinfo')
                            if appinfo and appinfo.Element('appname'):
                                content += '[来自]' + appinfo.Element('appname').Value
                else:
                    pass
            elif xml.Name.LocalName == 'mmreader':
                category = xml.Element('category')
                if category and category.Element('item'):
                    item = category.Element('item')
                    if item.Element('title'):
                        content += '[标题]' + item.Element('title').Value + '\n'
                    if item.Element('digest'):
                        content += '[内容]' + item.Element('digest').Value + '\n'
                    if item.Element('url'):
                        content += '[链接]' + item.Element('url').Value + '\n'
            elif xml.Name.LocalName == 'appmsg':
                if xml.Element('title'):
                    content += '[标题]' + xml.Element('title').Value + '\n'
                if xml.Element('des'):
                    content += '[内容]' + xml.Element('des').Value + '\n'
                if xml.Element('url'):
                    content += '[链接]' + xml.Element('url').Value + '\n'
                appinfo = xml.Element('appinfo')
                if appinfo and appinfo.Element('appname'):
                    content += '[来自]' + appinfo.Element('appname').Value
            else:
                pass
        if len(content) > 0:
            return content
        else:
            return xml_str

    def _process_parse_message_deal(self, xml_element):
        sender_id = None
        deal = model_im.Deal()
        if xml_element.Name.LocalName == 'msg':
            appmsg = xml_element.Element('appmsg')
            if appmsg is not None:
                wcpayinfo = appmsg.Element('wcpayinfo')
                if appmsg.Element('des') is not None:
                    deal.description = appmsg.Element('des').Value
                try:
                    msg_type = int(appmsg.Element('type').Value) if appmsg.Element('type') else 0
                except Exception as e:
                    msg_type = 0
                if msg_type == 2000:
                    deal.type = model_im.DEAL_TYPE_RECEIPT
                    if wcpayinfo is not None:
                        if wcpayinfo.Element('feedesc') is not None:
                            deal.money = wcpayinfo.Element('feedesc').Value
                        if wcpayinfo.Element('invalidtime') is not None:
                            try:
                                deal.expire_time = int(wcpayinfo.Element('invalidtime').Value)
                            except Exception as e:
                                pass
                        if wcpayinfo.Element('pay_memo') is not None:
                            deal.remark = wcpayinfo.Element('pay_memo').Value
                elif msg_type == 2001:
                    if wcpayinfo is not None:
                        newaa = wcpayinfo.Element('newaa')
                        newaatype = 0
                        if newaa and newaa.Element('newaatype'):
                            try:
                                newaatype = int(newaa.Element('newaatype').Value)
                            except Exception as e:
                                pass
                        if newaatype != 0:
                            deal.type = model_im.DEAL_TYPE_AA_RECEIPT
                            if wcpayinfo.Element('receiverdes'):
                                deal.description = wcpayinfo.Element('receiverdes').Value
                            if wcpayinfo.Element('receivertitle'):
                                deal.remark = wcpayinfo.Element('receivertitle').Value
                        else:
                            deal.type = model_im.DEAL_TYPE_RED_ENVELPOE
                            if wcpayinfo.Element('receivertitle'):
                                deal.remark = wcpayinfo.Element('receivertitle').Value

            fromusername = xml_element.Element('fromusername')
            if fromusername is not None:
                sender_id = fromusername.Value
        return deal, sender_id

    def _process_parse_message_location(self, xml_str, model):
        xml = None
        try:
            xml = XElement.Parse(xml_str)
        except Exception as e:
            pass
        if xml is not None:
            location = xml.Element('location')
            if location.Attribute('x'):
                try:
                    model.latitude = float(location.Attribute('x').Value)
                except Exception as e:
                    pass
            if location.Attribute('y'):
                try:
                    model.longitude = float(location.Attribute('y').Value)
                except Exception as e:
                    pass
            if location.Attribute('poiname'):
                model.address = location.Attribute('poiname').Value

    def _process_parse_message_voip(self, xml_str):
        content = ''
        xml = None
        try:
            xml_str = '<root>' + xml_str + '</root>'
            xml = XElement.Parse(xml_str)
        except Exception as e:
            pass
        if xml is not None:
            voipinvitemsg = xml.Element('voipinvitemsg')
            if voipinvitemsg:
                if voipinvitemsg.Element('invitetype'):
                    try:
                        invitetype = int(voipinvitemsg.Element('invitetype').Value)
                    except Exception as e:
                        invitetype = None
                    if invitetype == 0:
                        content += '[视频通话]'
                    elif invitetype == 1:
                        content += '[语音通话]'
            voiplocalinfo = xml.Element('voiplocalinfo')
            if voiplocalinfo:
                duration = 0
                if voiplocalinfo.Element('duration'):
                    duration = voiplocalinfo.Element('duration').Value
                if voiplocalinfo.Element('wordingtype'):
                    try:
                        wordingtype = int(voiplocalinfo.Element('wordingtype').Value)
                    except Exception as e:
                        wordingtype = None
                    if wordingtype == 4:
                        content += '通话时长{0}秒'.format(duration)
                    elif wordingtype == 1:
                        content += '已取消'
                    elif wordingtype == 8:
                        content += '已拒绝'
        if content not in [None, '']:
            return content
        else:
            return xml_str

    def _process_parse_message_voip_group(self, msg):
        content = ''
        info = None
        try:
            info = json.loads(msg)
        except Exception as e:
            pass
        if info is not None:
            content = info.get('msgContent')

        if content not in [None, '']:
            return content
        else:
            return msg

    @staticmethod
    def _db_record_get_value(record, column, default_value=None):
        if not record[column].IsDBNull:
            return record[column].Value
        return default_value

    @staticmethod
    def _db_record_get_string_value(record, column, default_value=''):
        if not record[column].IsDBNull:
            try:
                value = str(record[column].Value)
                if record.Deleted != DeletedState.Intact:
                    value = filter(lambda x: x in string.printable, value)
                return value
            except Exception as e:
                return default_value
        return default_value

    @staticmethod
    def _db_record_get_int_value(record, column, default_value=0):
        if not record[column].IsDBNull:
            try:
                return int(record[column].Value)
            except Exception as e:
                return default_value
        return default_value

    @staticmethod
    def _db_record_get_blob_value(record, column, default_value=None):
        if not record[column].IsDBNull:
            try:
                value = record[column].Value
                return bytes(value)
            except Exception as e:
                return default_value
        return default_value

    @staticmethod
    def _db_column_get_string_value(column, default_value=''):
        if column is not None:
            try:
                return str(column)
            except Exception as e:
                return default_value
        else:
            return default_value

    @staticmethod
    def _db_column_get_int_value(column, default_value=0):
        if column is not None:
            try:
                return int(column)
            except Exception as e:
                return default_value
        else:
            return default_value

    @staticmethod
    def _db_column_get_blob_value(column, default_value=None):
        if column is not None:
            try:
                return bytes(column)
            except Exception as e:
                return default_value
        else:
            return default_value

    @staticmethod
    def _db_reader_get_string_value(reader, index, default_value=''):
        return reader.GetString(idx) if not reader.IsDBNull(idx) else default_value

    @staticmethod
    def _db_reader_get_int_value(reader, index, default_value=0):
        return reader.GetInt64(idx) if not reader.IsDBNull(idx) else default_value

    @staticmethod
    def _db_reader_get_blob_value(reader, index, default_value=None):
        if not reader.IsDBNull(idx):
            try:
                return bytes(reader.GetValue(idx))
            except Exception as e:
                return default_value
        else:
            return default_value

        return reader.GetString(idx) if not reader.IsDBNull(idx) else default_value

    @staticmethod
    def _bpreader_node_get_string_value(node, key, default_value='', deleted=0):
        if key in node.Children and node.Children[key] is not None:
            try:
                value = str(node.Children[key].Value)
                if deleted != 0:
                    value = filter(lambda x: x in string.printable, value)
                return value
            except Exception as e:
                return default_value
        return default_value

    @staticmethod
    def _bpreader_node_get_int_value(node, key, default_value=0):
        if key in node.Children and node.Children[key] is not None:
            try:
                return int(node.Children[key].Value)
            except Exception as e:
                return default_value
        return default_value

    @staticmethod
    def _bpreader_node_get_float_value(node, key, default_value=0):
        if key in node.Children and node.Children[key] is not None:
            try:
                return float(node.Children[key].Value)
            except Exception as e:
                return default_value
        return default_value

    @staticmethod
    def _convert_msg_type(msg_type):
        if msg_type == MSG_TYPE_TEXT:
            return model_im.MESSAGE_CONTENT_TYPE_TEXT
        elif msg_type == MSG_TYPE_IMAGE:
            return model_im.MESSAGE_CONTENT_TYPE_IMAGE
        elif msg_type == MSG_TYPE_VOICE:
            return model_im.MESSAGE_CONTENT_TYPE_VOICE
        elif msg_type in [MSG_TYPE_VIDEO, MSG_TYPE_VIDEO_2]:
            return model_im.MESSAGE_CONTENT_TYPE_VIDEO
        elif msg_type == MSG_TYPE_EMOJI:
            return model_im.MESSAGE_CONTENT_TYPE_EMOJI
        elif msg_type == MSG_TYPE_LOCATION:
            return model_im.MESSAGE_CONTENT_TYPE_LOCATION
        elif msg_type in [MSG_TYPE_VOIP, MSG_TYPE_VOIP_GROUP]:
            return model_im.MESSAGE_CONTENT_TYPE_VOIP
        elif msg_type in [MSG_TYPE_SYSTEM, MSG_TYPE_SYSTEM_2]:
            return model_im.MESSAGE_CONTENT_TYPE_SYSTEM
        else:
            return model_im.MESSAGE_CONTENT_TYPE_LINK

    @staticmethod
    def _convert_gender_type(gender_type):
        if gender_type != 0:
            return model_im.GENDER_FEMALE
        else:
            return model_im.GENDER_MALE

    @staticmethod
    def db_mapping(src_path, dst_path):
        try:
            if os.path.exists(dst_path):
                os.remove(dst_path)
            shutil.copy(src_path, dst_path)
        except Exception as e:
            return False

        try:
            src_shm = src_path + '-shm'
            if os.path.exists(src_shm): 
                dst_shm = dst_path + '-shm'
                if os.path.exists(dst_shm):
                    os.remove(dst_shm)
                shutil.copy(src_shm, dst_shm)
        except Exception as e:
            pass

        try:
            src_wal = src_path + '-wal'
            if os.path.exists(src_wal): 
                dst_wal = dst_path + '-wal'
                if os.path.exists(dst_wal):
                    os.remove(dst_wal)
                shutil.copy(src_wal, dst_wal)
        except Exception as e:
            pass

        WeChatParser.db_fix_header(dst_path)
        return True

    @staticmethod
    def db_remove_mapping(src_path):
        try:
            if os.path.exists(src_path):
                os.remove(src_path)
        except Exception as e:
            pass

        try:
            src_shm = src_path + '-shm'
            if os.path.exists(src_shm):
                os.remove(src_shm)
        except Exception as e:
            pass

        try:
            src_wal = src_path + '-wal'
            if os.path.exists(src_wal):
                os.remove(src_wal)
        except Exception as e:
            pass

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
