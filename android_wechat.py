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

# EnterPoint: analyze_wechat(root, extract_deleted, extract_source):
# Patterns: '/MicroMsg/.+/EnMicroMsg.db$'

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

        self.cache_path = ds.OpenCachePath('wechat')
        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)
        self.cache_db = os.path.join(self.cache_path, self.user_hash + '.db')

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
            try:
                node.Data.seek(0)
                content = node.read()
                pos = content.find(b'\x01\x02\x74\x00')
                if pos != -1:
                    size = ord(content[pos+4:pos+5])
                    imei = content[pos+5:pos+5+size]
            except Exception as e:
                print(e)
        return imei

    @staticmethod
    def _get_db_key(imei, uin):
        if imei is None or uin is None:
            return None

        m = hashlib.md5()
        m.update((imei + uin).encode('utf8'))
        return m.hexdigest()[:7]

    def _parse_mm_db(self, mm_db_path, source):
        db = sqlite3.connect(mm_db_path)
        self._parse_mm_db_user_info(db, source)
        self._parse_mm_db_contact(db, source)
        self._parse_mm_db_chatroom_member(db, source)
        self._parse_mm_db_message(db, source)
        db.close()

    def _parse_mm_db_user_info(self, db, source):
        cursor = db.cursor()
        self.user_account.source = source
        self.user_account.account_id = self._parse_mm_db_get_user_info_from_userinfo(cursor, 2)
        self.user_account.nickname = self._parse_mm_db_get_user_info_from_userinfo(cursor, 4)
        self.user_account.email = self._parse_mm_db_get_user_info_from_userinfo(cursor, 5)
        self.user_account.telephone = self._parse_mm_db_get_user_info_from_userinfo(cursor, 6)
        self.user_account.signature = self._parse_mm_db_get_user_info_from_userinfo(cursor, 12291)
        self.user_account.username = self._parse_mm_db_get_user_info_from_userinfo2(cursor, 'USERINFO_LAST_LOGIN_USERNAME_STRING')
        self.user_account.photo = self._parse_mm_db_get_user_info_from_userinfo2(cursor, 'USERINFO_SELFINFO_SMALLIMGURL_STRING')
        self.db_insert_table_account(self.user_account)
        self.db_commit()
        cursor.close()

    def _parse_mm_db_get_user_info_from_userinfo(self, cursor, id):
        sql = 'select value from userinfo where id = {}'.format(id)
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)
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
            print(e)
        if row is not None:
            return self._db_column_get_string_value(row[0])
        return None

    def _parse_mm_db_contact(self, db, source):
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
            print(e)
        while row is not None:
            username = self._db_column_get_string_value(row[0])
            alias = self._db_column_get_string_value(row[1])
            nickname = self._db_column_get_string_value(row[2])
            remark = self._db_column_get_string_value(row[3])
            contact_type = self._db_column_get_int_value(row[4])
            verify_flag = self._db_column_get_int_value(row[5])
            portrait_hd = self._db_column_get_string_value(row[6])
            portrait = self._db_column_get_string_value(row[7])

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
        cursor.close()

    def _parse_mm_db_chatroom_member(self, db, source):
        sql = '''select chatroomname,memberlist,displayname,selfDisplayName,roomowner
                 from chatroom'''
        row = None
        cursor = db.cursor()
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)
        
        while row is not None:
            chatroom_id = self._db_column_get_string_value(row[0])
            member_list = self._db_column_get_string_value(row[1])
            display_name_list = self._db_column_get_string_value(row[2])
            room_owner = self._db_column_get_string_value(row[4])

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
        cursor.close()

    def _parse_mm_db_message(self, db, source):
        sql = 'select talker,content,imgPath,isSend,status,type,createTime,msgId,lvbuffer,msgSvrId from message'
        row = None
        cursor = db.cursor()
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)
        
        while row is not None:
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

            message = model_im.Message()
            message.source = source
            message.account_id = self.user_account.account_id
            message.talker_id = talker
            message.talker_name = self.contacts.get(talker, {}).get('nickname')
            message.is_sender = is_send
            message.msg_id = msg_svr_id
            message.type = self._convert_msg_type(msg_type)
            message.send_time = create_time
            message.media_path = self._process_parse_message_img_path(msg_type, img_path)
            if talker.endswith("@chatroom"):
                self._process_parse_group_message(msg, msg_type, is_send, 'hash', message)
                message.sender_name = self.contacts.get(message.sender_id, {}).get('nickname')
                message.talker_type = model_im.CHAT_TYPE_GROUP
            else:
                message.sender_id = self.user_account.account_id if is_send != 0 else talker
                self._process_parse_friend_message(msg, msg_type, 'hash', message)
                message.sender_name = self.contacts.get(message.sender_id, {}).get('nickname')
                message.talker_type = model_im.CHAT_TYPE_FRIEND
            self.db_insert_table_message(message)
            row = cursor.fetchone()
        self.db_commit()
        cursor.close()

    def _process_parse_friend_message(self, msg, msg_type, friend_hash, model):
        content = msg

        if msg_type == MSG_TYPE_LOCATION:
            if model is not None:
                location = model_im.Location()
                location.deleted = model.deleted
                location.source = model.source
                self._process_parse_message_location(content, location)
                model.extra_id = location.location_id
                self.db_insert_table_location(location)
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
        if img_path in (None, ''):
            return img_path
        media_path = img_path
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
            media_path = '/image2/{0}/{1}/{2}'.format(m1, m2, img_name)
        return media_path

    def _process_parse_message_tranlate_voice_path(self, voice_id):
        m = hashlib.md5()
        m.update(voice_id.encode('utf8'))
        hash = m.hexdigest()
        m1 = hash[0:2]
        m2 = hash[2:4]
        voice_relative_path = '/voice2/{0}/{1}/msg_{2}.amr'.format(m1, m2, voice_id)
        return voice_relative_path

    def _process_parse_message_tranlate_video_path(self, video_id):
        voice_relative_path = '/video/{}.mp4'.format(video_id)
        return voice_relative_path

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
