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
from System.Security.Cryptography import *
from System.Text import *
from System.IO import *
from System import Convert
from PA_runtime import *

import os
import hashlib
import json
import base64
import sqlite3
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
        self.root = node.Parent.Parent.Parent
        self.user_node = node.Parent
        self.extract_deleted = False  # extract_deleted
        self.extract_source = extract_source
        self.is_valid_user_dir = self._is_valid_user_dir()
        self.uin = self._get_uin()
        self.imei = self._get_imei(self.root.GetByPath('/MicroMsg/CompatibleInfo.cfg'))
        self.user_hash = self._get_user_hash()

    def parse(self):
        if not self.is_valid_user_dir:
            return []
        if not self._can_decrypt(self.uin, self.user_hash):
            return []
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

            node = self.user_node.GetByPath('/EnMicroMsg.db')
            mm_db_path = os.path.join(self.cache_path, self.user_hash + '_mm.db')
            if Decryptor.decrypt(node, self._get_db_key(self.imei, self.uin), mm_db_path):
                self._parse_mm_db(mm_db_path, node.AbsolutePath)

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
        if self.root is None or self.user_node is None:
            return False
        if self.root.GetByPath('/shared_prefs/auth_info_key_prefs.xml') is None and self.root.GetByPath('/shared_prefs/com.tencent.mm_preferences.xml'):
            return False
        return True

    @staticmethod
    def _can_decrypt(uin, user_hash):
        m = hashlib.md5()
        m.update(('mm' + uin).encode('utf8'))
        return m.hexdigest() == user_hash

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
            pass
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
            pass
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
            node.Data.seek(0)
            content = node.read()
            try:
                pos = content.find(b'\x01\x02\x74\x00')
                if pos != -1:
                    size = ord(content[pos+4:pos+5])
                    imei = content[pos+5:pos+5+size]
            except Exception as e:
                print(e)
        return imei

    @staticmethod
    def _get_db_key(imei, uin):
        m = hashlib.md5()
        m.update((imei + uin).encode('utf8'))
        return m.hexdigest()[:7]

    def _parse_mm_db(self, mm_db_path, source):
        db = sqlite3.connect(mm_db_path)
        cursor = db.cursor()

        self.user_account.source = source
        self._parse_mm_db_user_info(cursor)
        self._parse_mm_db_contact(cursor, source)
        self._parse_mm_db_chatroom_member(cursor, source)
        self._parse_mm_db_message(cursor, source)

        cursor.close()
        db.close()

    def _parse_mm_db_user_info(self, cursor):
        self.user_account.account_id = self._parse_mm_db_get_user_info_from_userinfo(cursor, 2)
        self.user_account.nickname = self._parse_mm_db_get_user_info_from_userinfo(cursor, 4)
        self.user_account.email = self._parse_mm_db_get_user_info_from_userinfo(cursor, 5)
        self.user_account.telephone = self._parse_mm_db_get_user_info_from_userinfo(cursor, 6)
        self.user_account.signature = self._parse_mm_db_get_user_info_from_userinfo(cursor, 12291)
        self.user_account.username = self._parse_mm_db_get_user_info_from_userinfo2(cursor, 'USERINFO_LAST_LOGIN_USERNAME_STRING')
        self.user_account.photo = self._parse_mm_db_get_user_info_from_userinfo2(cursor, 'USERINFO_SELFINFO_SMALLIMGURL_STRING')
        self.db_insert_table_account(self.user_account)
        self.db_commit()

    def _parse_mm_db_get_user_info_from_userinfo(self, cursor, id):
        sql = 'select value from userinfo where id = {}'.format(id)
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)
        if row is not None:
            return row[0]
        return None

    def _parse_mm_db_get_user_info_from_userinfo2(self, cursor, sid):
        sql = "select value from userinfo2 where sid = '{}'".format(sid)
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)
        if row is not None:
            return row[0]
        return None

    def _parse_mm_db_contact(self, cursor, source):
        sql = '''select rcontact.username,alias,nickname,conRemark,type,verifyFlag,reserved1,reserved2 
                 from rcontact 
                 left outer join img_flag 
                 on rcontact.username = img_flag.username'''
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)
        while row is not None:
            username = row[0]
            alias = row[1]
            nickname = row[2]
            remark = row[3]
            contact_type = row[4]
            verify_flag = row[5]
            portrait_hd = row[6]
            portrait = row[7]

            if username in [None, '']:
                continue

            head = portrait
            if portrait_hd and len(portrait_hd) > 0:
                head = portrait_hd

            contact = {}
            if nickname:
                contact['nickname'] = nickname
            if remark:
                contact['remark'] = remark
            if head:
                contact['photo'] = head
            self.contacts[username] = contact

            if username.endswith('@chatroom'):
                chatroom = model_im.Chatroom()
                chatroom.source = source
                chatroom.account_id = self.user_account.account_id
                chatroom.chatroom_id = username
                chatroom.name = nickname
                chatroom.photo = head
                self.db_insert_table_chatroom(chatroom)
            else:
                friend = model_im.Friend()
                friend.source = source
                friend.account_id = self.user_account.account_id
                friend.friend_id = username
                friend.type = model_im.FRIEND_TYPE_FRIEND if verify_flag == 0 else model_im.FRIEND_TYPE_FOLLOW
                friend.nickname = nickname
                friend.remark = remark
                friend.photo = head
                self.db_insert_table_friend(friend)
            row = cursor.fetchone()
        self.db_commit()

    def _parse_mm_db_chatroom_member(self, cursor, source):
        sql = '''select chatroomname,memberlist,displayname,selfDisplayName,roomowner
                 from chatroom'''
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)
        
        while row is not None:
            chatroom_id = row[0]
            member_list = row[1]
            display_name_list = row[2]
            room_owner = row[4]

            room_members = member_list.split(';')
            display_names = display_name_list.split('、')
            
            cm = model_im.ChatroomMember()
            cm.source = source
            cm.account_id = self.user_account.account_id
            cm.chatroom_id = chatroom_id
            for i, room_member in enumerate(room_members):
                cm.member_id = room_member
                if i < len(display_names) and display_names[i] != room_member:
                    cm.display_name = display_names[i]
                self.db_insert_table_chatroom_member(cm)
            row = cursor.fetchone()
        self.db_commit()

    def _parse_mm_db_message(self, cursor, source):
        sql = 'select talker,content,imgPath,isSend,status,type,createTime,msgId,lvbuffer,msgSvrId from message'
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)
        
        while row is not None:
            talker = row[0]
            msg = row[1]
            img_path = row[2]
            is_send = row[3]
            status = row[4]
            msg_type = row[5]
            create_time = row[6] / 1000
            msg_id = row[7]
            lv_buffer = row[8]
            msg_svr_id = row[9]

            #if msg_type == MSG_TYPE_VOICE:
            #    img_path = self._process_db_tranlate_voice_path(img_path)
            #elif msg_type == MSG_TYPE_VIDEO:
            #    img_path = self._process_db_tranlate_video_path(img_path)
            #elif msg_type == MSG_TYPE_VOIP:
            #    img_path = self._process_db_tranlate_lv_buffer(lv_buffer)
            #else:
            #    img_path = self._process_db_tranlate_img_path(img_path)

            message = model_im.Message()
            message.source = source
            message.account_id = self.user_account.account_id
            message.talker_id = talker
            message.talker_name = self.contacts.get(talker, {}).get('nickname')
            message.is_sender = is_send
            message.msg_id = msg_svr_id
            message.type = self._convert_msg_type(msg_type)
            message.send_time = create_time
            message.media_path = img_path
            if talker.endswith("@chatroom"):
                content, sender_id = self._process_parse_group_message(msg, msg_type, is_send, message)
                message.sender_id = sender_id
                message.sender_name = self.contacts.get(sender_id, {}).get('nickname')
                message.content = content
                message.talker_type = model_im.USER_TYPE_CHATROOM
            else:
                content = self._process_parse_friend_message(msg, msg_type, message)
                message.sender_id = self.user_account.account_id if is_send != 0 else talker
                message.sender_name = self.contacts.get(message.sender_id, {}).get('nickname')
                message.content = content
                message.talker_type = model_im.USER_TYPE_FRIEND
            self.db_insert_table_message(message)
            row = cursor.fetchone()
        self.db_commit()

    def _process_parse_group_message(self, msg, msg_type, is_sender, model):
        sender_id = self.user_account.account_id
        content = msg

        if is_sender == 0:
            index = msg.find(':\n')
            if index != -1:
                sender_id = msg[:index]
                content = msg[index+2:]

        content = self._process_parse_friend_message(content, msg_type, model)
        return content, sender_id

    def _process_parse_friend_message(self, msg, msg_type, model):
        content = msg
        return content

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
            content = src_node.read(1024)
            iv = content[1008: 1024]
            de.write(Decryptor.aes_decrypt(final_key,
                                           Convert.FromBase64String(base64.b64encode(iv)),
                                           Convert.FromBase64String(base64.b64encode(content[:1008]))))
            de.write(iv)
        de.close()

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
