#coding=utf-8
import PA_runtime
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions
from PA_runtime import *

import os
import hashlib
import json
import model_im

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
    """
    微信 (/DB/MM.sqlite)
    解析 Account, Contacts, Chats (Attachments)
    """
    pr = ParserResults()
    pr.Categories = DescripCategories.Wechat #声明这是微信应用解析的数据集
    models = WeChatParser(root, extract_deleted, extract_source).parse()
    mlm = ModelListMerger()
    
    pr.Models.AddRange(list(mlm.GetUnique(models)))
    pr.Build('微信')
    return pr

class WeChatParser(model_im.IM):
    
    def __init__(self, node, extract_deleted, extract_source):
        super(WeChatParser, self).__init__()
        self.root = node.Parent.Parent
        self.extract_deleted = False  # extract_deleted
        self.extract_source = extract_source
        self.is_valid_user_dir = self._is_valid_user_dir()

    def parse(self):
        if not self.is_valid_user_dir:
            return []

        self.user_hash = self.get_user_hash()
        self.APP_NAME = self.get_app_name()
        self.mount_dir = self.root.FileSystem.MountPoint
        self.cache_path = ds.OpenCachePath('wechat')
        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)
        self.cache_db = os.path.join(self.cache_path, self.user_hash + '.db')
        self.like_id = 1
        self.comment_id = 1
        self.location_id = 1

        if self.need_parse(self.cache_db, VERSION_APP_VALUE):
            self.db_create(self.cache_db)

            self.contacts = {}
            self.user_account = model_im.Account()

            user_plist = self.root.GetByPath('mmsetting.archive')
            if user_plist is not None and user_plist.Deleted == DeletedState.Intact:
                self._get_user_from_setting(user_plist)
                self._parse_user_contact_db(self.root.GetByPath('/DB/WCDB_Contact.sqlite'))
                self._parse_user_mm_db(self.root.GetByPath('/DB/MM.sqlite'))
                self._parse_user_wc_db(self.root.GetByPath('/wc/wc005_008.db'))
                self._parse_user_fts_db(self.root.GetByPath('/fts/fts_message.db'))

            # 数据库填充完毕，请将中间数据库版本和app数据库版本插入数据库，用来检测app是否需要重新解析
            self.db_insert_table_version(model_im.VERSION_KEY_DB, model_im.VERSION_VALUE_DB)
            self.db_insert_table_version(model_im.VERSION_KEY_APP, VERSION_APP_VALUE)
            self.db_commit()
            self.db_close()

        #self.covert_silk_and_amr()
        models = self.get_models_from_cache_db()
        return models

    def get_models_from_cache_db(self):
        models = model_im.GenerateModel(self.cache_db, self.mount_dir).get_models()
        return models

    def _is_valid_user_dir(self):
        if self.root is None:
            return False
        if self.root.GetByPath('mmsetting.archive') is None:
            return False
        if self.root.GetByPath('/DB/MM.sqlite') is None:
            return False
        return True

    def get_user_hash(self):
        path = self.root.AbsolutePath
        return os.path.basename(os.path.normpath(path))

    def get_app_name(self):
        return '微信'

    #def _get_user_nodes(self):
    #    user_nodes = []
    #    for node in self.root.Search('mmsetting.archive$'):
    #        user_node = node.Parent
    #        if user_node.GetByPath('/DB/MM.sqlite') is not None:
    #            user_nodes.append(user_node)
    #    return user_nodes
    
    def _get_user_from_setting(self, user_plist):
        root = None
        try:
            root = BPReader.GetTree(user_plist)
        except:
            return
        if not root or not root.Children:
            return

        self.user_account.account_id = self._bpreader_node_get_value(root, 'UsrName', '')
        self.user_account.nickname = self._bpreader_node_get_value(root, 'NickName')
        self.user_account.gender = self._bpreader_node_get_value(root, 'Sex')
        self.user_account.telephone = self._bpreader_node_get_value(root, 'Mobile')
        self.user_account.email = self._bpreader_node_get_value(root, 'Email')
        self.user_account.city = self._bpreader_node_get_value(root, 'City')
        self.user_account.country = self._bpreader_node_get_value(root, 'Country')
        self.user_account.province = self._bpreader_node_get_value(root, 'Province')
        self.user_account.signature = self._bpreader_node_get_value(root, 'Signature')

        if 'new_dicsetting' in root.Children:
            setting_node = root.Children['new_dicsetting']
            self.user_account.headhdimgurl = self._bpreader_node_get_value(setting_node, 'headhdimgurl')
            if 'headhdimgurl' in setting_node.Children:
                self.user_account.photo = self._bpreader_node_get_value(setting_node, 'headhdimgurl')
            else:
                self.user_account.photo = self._bpreader_node_get_value(setting_node, 'headimgurl')
        self.user_account.source = user_plist.AbsolutePath
        self.db_insert_table_account(self.user_account)
        self.db_commit()

    def _parse_user_contact_db(self, node):
        if node is None:
            return False
        
        db = SQLiteParser.Database.FromNode(node)
        if not db:
            return False

        if 'Friend' in db.Tables:
            ts = SQLiteParser.TableSignature('Friend')
            SQLiteParser.Tools.AddSignatureToTable(ts, "userName", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                username = self._db_record_get_value(rec, 'userName')
                certification_flag = self._db_record_get_value(rec, 'certificationFlag', 0)
                nickname = None
                alias = None
                remark = None
                if not rec["dbContactRemark"].IsDBNull:
                    nickname, alias, remark = self._process_parse_contact_remark(rec['dbContactRemark'].Value)
                head = None
                if not rec["dbContactHeadImage"].IsDBNull:
                    head, head_hd = self._process_parse_contact_head(rec['dbContactHeadImage'].Value)
                    if head_hd and len(head_hd) > 0:
                        head = head_hd

                contact = {}
                if nickname:
                    contact['nickname'] = nickname
                if remark:
                    contact['remark'] = remark
                if head:
                    contact['photo'] = head
                if rec.Deleted == DeletedState.Intact: 
                    self.contacts[username] = contact
                else:
                    if username not in self.contacts:
                        self.contacts[username] = contact

                if username.endswith("@chatroom"):
                    chatroom = model_im.Chatroom()
                    chatroom.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    chatroom.source = node.AbsolutePath
                    chatroom.account_id = self.user_account.account_id
                    chatroom.chatroom_id = username
                    chatroom.name = nickname
                    chatroom.photo = head

                    members, max_count = self._process_parse_group_members(self._db_record_get_value(rec, 'dbContactChatRoom'))
                    for member in members:
                        cm = model_im.ChatroomMember()
                        cm.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                        cm.source = node.AbsolutePath
                        cm.account_id = self.user_account.account_id
                        cm.chatroom_id = username
                        cm.member_id = member.get('username')
                        cm.display_name = member.get('display_name')
                        try:
                            self.db_insert_table_chatroom_member(cm)
                        except Exception as e:
                            pass

                    if len(members) > 0:
                        chatroom.owner_id = members[0].get('username')
                    chatroom.max_member_count = max_count
                    chatroom.member_count = len(members)
                    try:
                        self.db_insert_table_chatroom(chatroom)
                    except Exception as e:
                        pass
                else:
                    friend = model_im.Friend()
                    friend.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    friend.source = node.AbsolutePath
                    friend.account_id = self.user_account.account_id
                    friend.friend_id = username
                    friend.type = model_im.FRIEND_TYPE_FRIEND if certification_flag == 0 else model_im.FRIEND_TYPE_FOLLOW
                    friend.nickname = nickname
                    friend.remark = remark
                    friend.photo = head
                    try:
                        self.db_insert_table_friend(friend)
                    except Exception as e:
                        pass
            self.db_commit()
        return True

    def _parse_user_mm_db(self, node):
        if not node:
            return False

        db = SQLiteParser.Database.FromNode(node)
        if not db:
            return False

        for username in self.contacts.keys():
            m = hashlib.md5()
            m.update(username.encode('utf8'))
            user_hash = m.hexdigest()
            table = 'Chat_' + user_hash
            if table not in db.Tables:
                continue
            ts = SQLiteParser.TableSignature(table)
            SQLiteParser.Tools.AddSignatureToTable(ts, "Message", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                msg = self._db_record_get_value(rec, 'Message')
                msg_type = self._db_record_get_value(rec, 'Type', MSG_TYPE_TEXT)
                msg_local_id = self._db_record_get_value(rec, 'MesLocalID')
                is_sender = 1 if self._db_record_get_value(rec, 'Des', 0) == 0 else 0
                contact = self.contacts.get(username, {})

                message = model_im.Message()
                message.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                message.source = node.AbsolutePath
                message.account_id = self.user_account.account_id
                message.talker_id = username
                message.talker_name = contact.get('nickname')
                message.is_sender = is_sender
                message.msg_id = msg_local_id
                message.type = self._convert_msg_type(msg_type)
                message.send_time = self._db_record_get_value(rec, 'CreateTime')
                if username.endswith("@chatroom"):
                    content, media_path, sender_id = self._process_parse_group_message(msg, msg_type, msg_local_id, is_sender, self.root, user_hash, message)
                    message.sender_id = sender_id
                    message.sender_name = self.contacts.get(message.sender_id, {}).get('nickname')
                    message.content = content
                    message.media_path = media_path
                    message.talker_type = model_im.USER_TYPE_CHATROOM
                else:
                    content, media_path = self._process_parse_friend_message(msg, msg_type, msg_local_id, self.root, user_hash, message)
                    message.sender_id = self.user_account.account_id if is_sender else username
                    message.sender_name = self.contacts.get(message.sender_id, {}).get('nickname')
                    message.content = content
                    message.media_path = media_path
                    message.talker_type = model_im.USER_TYPE_FRIEND
                try:
                    self.db_insert_table_message(message)
                except Exception as e:
                    pass
            try:
                self.db_commit()
            except Exception as e:
                pass
        return True

    def _parse_user_wc_db(self, node):
        if not node:
            return False

        db = SQLiteParser.Database.FromNode(node)
        if not db:
            return False

        tables = [t for t in db.Tables if t.startswith('MyWC01_')]
        for table in tables:
            ts = SQLiteParser.TableSignature(table)
            SQLiteParser.Tools.AddSignatureToTable(ts, "FromUser", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            SQLiteParser.Tools.AddSignatureToTable(ts, "Buffer", SQLiteParser.FieldType.Blob, SQLiteParser.FieldConstraints.NotNull)
            
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                username = self._db_record_get_value(rec, 'FromUser')
                if not rec["Buffer"].IsDBNull:
                    root_mr = MemoryRange(rec["Buffer"].Source)
                    root_mr.seek(0)
                    if root_mr.Length < 8 or root_mr.read(8) != "bplist00":
                        continue
                    root_mr.seek(0)
                    try:
                        root = BPReader.GetTree(root_mr)
                    except:
                        continue
                    if not root or not root.Children:
                        continue

                    feed = model_im.Feed()
                    feed.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    feed.source = node.AbsolutePath
                    feed.account_id = self.user_account.account_id
                    feed.sender_id = username
                    feed.content = self._bpreader_node_get_value(root, 'contentDesc')
                    feed.send_time = self._bpreader_node_get_value(root, 'createtime')

                    if 'locationInfo' in root.Children:
                        location_node = root.Children['locationInfo']
                        location = model_im.Location()
                        location.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                        location.source = node.AbsolutePath
                        location.location_id = self.location_id
                        try:
                            location.latitude = float(self._bpreader_node_get_value(location_node, 'location_latitude', 0))
                        except Exception as e:
                                pass
                        try:
                            location.longitude = float(self._bpreader_node_get_value(location_node, 'location_longitude', 0))
                        except Exception as e:
                                pass
                        location.address = self._bpreader_node_get_value(location_node, 'poiName')
                        self.db_insert_table_location(location)
                        self.location_id += 1

                    if 'contentObj' in root.Children:
                        content_node = root.Children['contentObj']
                        try:
                            feed.type = int(self._bpreader_node_get_value(content_node, 'type', 0))
                        except Exception as e:
                            feed.type = 0
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
                            feed.attachment_title = self._bpreader_node_get_value(content_node, 'title')
                            feed.attachment_link = self._bpreader_node_get_value(content_node, 'linkUrl')
                            feed.attachment_desc = self._bpreader_node_get_value(content_node, 'desc')
                        elif feed.type == MOMENT_TYPE_SHARED:
                            for media_node in media_nodes:
                                feed.attachment_title = self._bpreader_node_get_value(media_node, 'title')

                    likes = []
                    if 'likeUsers' in root.Children:
                        for like_node in root.Children['likeUsers'].Values:
                            sender_id = self._bpreader_node_get_value(like_node, 'username', '')
                            if len(sender_id) > 0:
                                fl = model_im.FeedLike()
                                fl.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                                fl.source = node.AbsolutePath
                                fl.like_id = self.like_id
                                fl.sender_id = sender_id
                                fl.sender_name = self._bpreader_node_get_value(like_node, 'nickname')
                                try:
                                    fl.create_time = int(self._bpreader_node_get_value(like_node, 'createTime'))
                                except Exception as e:
                                    pass
                                try:
                                    self.db_insert_table_feed_like(fl)
                                    likes.append(self.like_id)
                                except Exception as e:
                                    pass

                                self.like_id += 1
                    feed.likes = ','.join(str(item) for item in likes)

                    comments = []
                    if 'commentUsers' in root.Children:
                        for comment_node in root.Children['commentUsers'].Values:
                            sender_id = self._bpreader_node_get_value(comment_node, 'username', '')
                            content = self._bpreader_node_get_value(comment_node, 'content', '')
                            if type(sender_id) == str and len(sender_id) > 0 and type(content) == str:
                                fc = model_im.FeedComment()
                                fc.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                                fc.source = node.AbsolutePath
                                fc.comment_id = self.comment_id
                                fc.sender_id = sender_id
                                fc.sender_name = self._bpreader_node_get_value(comment_node, 'nickname')
                                fc.ref_user_id = self._bpreader_node_get_value(comment_node, 'refUserName')
                                fc.content = content
                                try:
                                    fc.create_time = int(self._bpreader_node_get_value(comment_node, 'createTime'))
                                except Exception as e:
                                    pass
                                try:
                                    self.db_insert_table_feed_comment(fc)
                                    comments.append(self.comment_id)
                                except Exception as e:
                                    pass

                                self.comment_id += 1
                    feed.comments = ','.join(str(item) for item in comments)

                    try:
                        self.db_insert_table_feed(feed)
                    except Exception as e:
                        pass
            try:
                self.db_commit()
            except Exception as e:
                pass
        return True

    def _parse_user_fts_db(self, node):
        if node is None:
            return False
        
        db = SQLiteParser.Database.FromNode(node)
        if not db:
            return False

        username_ids = {}
        if 'fts_username_id' in db.Tables:
            ts = SQLiteParser.TableSignature('fts_username_id')
            SQLiteParser.Tools.AddSignatureToTable(ts, "UsrName", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            SQLiteParser.Tools.AddSignatureToTable(ts, "usernameid", SQLiteParser.FieldType.Int, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                username = self._db_record_get_value(rec, 'UsrName', '')
                id = self._db_record_get_value(rec, 'usernameid', 0)
                if username != '' and id != 0:
                    username_ids[id] = username

        tables = [t for t in db.Tables if t.startswith('fts_message_table_') and t.endswith('_content')]
        for table in tables:
            ts = SQLiteParser.TableSignature(table)
            SQLiteParser.Tools.AddSignatureToTable(ts, "c0usernameid", SQLiteParser.FieldType.Int, SQLiteParser.FieldConstraints.NotNull)
            SQLiteParser.Tools.AddSignatureToTable(ts, "c3Message", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                id = self._db_record_get_value(rec, 'c0usernameid', 0)
                if id not in username_ids:
                    continue
                username = username_ids.get(id)
                contact = self.contacts.get(username, {})
                certification_flag = contact.get('certification_flag', 0)
                content = self._db_record_get_value(rec, 'c3Message', '')

                message = model_im.Message()
                message.deleted = 1
                message.repeated = contact.get('repeated', 0)
                message.source = node.AbsolutePath
                message.account_id = self.user_account.account_id
                message.talker_id = username
                message.talker_name = contact.get('nickname')
                if username.endswith('@chatroom'):
                    message.talker_type = model_im.USER_TYPE_CHATROOM
                else:
                    message.talker_type = model_im.USER_TYPE_FRIEND
                message.content = content
                try:
                    self.db_insert_table_message(message)
                except Exception as e:
                    pass
        try:
            self.db_commit()
        except Exception as e:
            pass
        return True

    @staticmethod
    def _process_parse_contact_remark(blob):
        nickname = ''
        alias = ''
        remark = ''

        index = 0
        while index + 2 < len(blob):
            flag = blob[index]
            size = blob[index + 1]
            if index + 2 + size > len(blob):
                break
            try:
                content = bytes(blob[index + 2: index + 2 + size]).decode('utf-8')
                if flag == 0x0a:  # nickname
                    nickname = content
                elif flag == 0x12:  # alias
                    alias = content
                elif flag == 0x1a:  # remark
                    remark = content
                index += 2 + size
            except Exception as e:
                # print('_process_parse_contact_remark error: %s' % e)
                break
        return nickname, alias, remark

    @staticmethod
    def _process_parse_contact_head(blob):
        head = None
        head_hd = None

        index = 2
        while index + 1 < len(blob):
            flag = blob[index]
            size = blob[index + 1]
            if size > 0:
                index += 2
                if blob[index] != 0x68:
                    index += 1
                if index + size > len(blob):
                    break
                try:
                    content = bytes(blob[index: index + size]).decode('utf-8')
                    if flag == 0x12:
                        head = content
                    elif flag == 0x1a:
                        head_hd = content
                except Exception as e:
                    break
                index += size
            else:
                index += 2

        return head, head_hd

    @staticmethod
    def _process_parse_group_members(blob):
        members = []
        max_count = 0

        try:
            data = bytes(blob)
        except Exception as e:
            return members, max_count

        prefix = b'<RoomData>'
        suffix = b'</RoomData>'
        if prefix in data and suffix in data:
            index_begin = data.index(prefix)
            index_end = data.index(suffix) + len(suffix)
            content = data[index_begin:index_end].decode('utf-8')
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

                #for username, display_name in [(tg.Attribute('UserName').Value, tg.Element("DisplayName").Value if tg.Element("DisplayName") else None) for tg in ms]:
                #    if username:
                #        members.append({'username': username, 'display_name': display_name})
        return members, max_count

    def _process_parse_friend_message(self, msg, msg_type, msg_local_id, user_node, friend_hash, model):
        content = msg
        img_path = ''

        if msg_type == MSG_TYPE_IMAGE:
            node = user_node.GetByPath('Img/{0}/{1}.pic'.format(friend_hash, msg_local_id))
            if node is not None:
                img_path = node.AbsolutePath
        elif msg_type == MSG_TYPE_VOICE:
            node = user_node.GetByPath('Audio/{0}/{1}.aud'.format(friend_hash, msg_local_id))
            if node is not None:
                img_path = node.AbsolutePath 
        #elif msg_type == MSG_TYPE_CONTACT_CARD:
        #    pass
        elif msg_type == MSG_TYPE_VIDEO or msg_type == MSG_TYPE_VIDEO_2:
            node = user_node.GetByPath('Video/{0}/{1}.mp4'.format(friend_hash, msg_local_id))
            if node is None:
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
                location.location_id = self.location_id
                self._process_parse_message_location(content, location)
                model.location = self.location_id
                self.location_id += 1
                self.db_insert_table_location(location)
            node = user_node.GetByPath('Location/{0}/{1}.pic_thum'.format(friend_hash, msg_local_id))
            if node is not None:
                img_path = node.AbsolutePath
        elif msg_type == MSG_TYPE_LINK:
            content = self._process_parse_message_link(content)
        elif msg_type == MSG_TYPE_VOIP:
            pass
        elif msg_type == MSG_TYPE_VOIP_GROUP:
            pass
        elif msg_type == MSG_TYPE_SYSTEM or msg_type == MSG_TYPE_SYSTEM_2:
            pass
        else:  # MSG_TYPE_TEXT
            pass

        return content, img_path

    def _process_parse_group_message(self, msg, msg_type, msg_local_id, is_sender, user_node, group_hash, model):
        sender = self.user_account.account_id
        content = msg
        img_path = ''

        if not is_sender:
            index = msg.find(':\n')
            if index != -1:
                sender = msg[:index]
                content = msg[index+2:]

        content, img_path = self._process_parse_friend_message(content, msg_type, msg_local_id, user_node, group_hash, model)
        
        return content, img_path, sender

    def _process_parse_message_link(self, xml_str):
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
                    msg_title = appmsg.Element('title').Value if appmsg.Element('title') else ''
                    mmreader = appmsg.Element('mmreader')
                    if msg_title == '微信红包':
                        content += '[标题]' + msg_title + '\n'  # type 2001  wcpayinfo
                    elif msg_title == '微信转账':
                        # type 2000  wcpayinfo
                        content += '[标题]' + msg_title + '\n'
                        if appmsg.Element('des'):
                            content += '[内容]' + appmsg.Element('des').Value + '\n'
                    elif mmreader:
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
            xml = XElement.Parse(xml_str)
        except Exception as e:
            pass
        if xml is not None:
            pass
        return content

    def _process_parse_message_voip_group(self, msg):
        pass

    @staticmethod
    def _db_record_get_value(record, column, default_value=None):
        if not record[column].IsDBNull:
            return record[column].Value
        return default_value

    @staticmethod
    def _bpreader_node_get_value(node, key, default_value=None):
        if key in node.Children and node.Children[key] is not None:
            return node.Children[key].Value
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
