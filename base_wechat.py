#coding=utf-8
import operator

__author__ = "sumeng"

from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference('model_wechat')
    clr.AddReference('ResourcesExp')
    clr.AddReference('ScriptUtils')
except:
    pass
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions
import System.Data.SQLite as SQLite

from PA.InfraLib.ModelsV2 import *
from PA.InfraLib.ModelsV2.IM import *

import os
import hashlib
import json
import string
import sqlite3
import shutil
import base64
import datetime
import model_wechat

from ResourcesExp import AppResources
from ScriptUtils import SemiXmlParser

# 消息类型
MSG_TYPE_POSITION_SHARE = -1879048186
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
MSG_TYPE_SYSTEM_3 = 570425393  # xml
MSG_TYPE_LINK_SEMI = 285212721  # semi_xml
MSG_TYPE_APP_MESSAGE = 318767153

# 朋友圈类型
MOMENT_TYPE_IMAGE = 1  # 正常文字图片
MOMENT_TYPE_TEXT_ONLY = 2  # 纯文字
MOMENT_TYPE_SHARED = 3  # 分享
MOMENT_TYPE_MUSIC = 4  # 带音乐的（存的是封面）
MOMENT_TYPE_EMOJI = 10  # 分享了表情包
MOMENT_TYPE_VIDEO = 15  # 视频


class Wechat(object):
    def __init__(self):
        pass
        #self.im = model_wechat.IM()
        #self.build = '微信'
        #self.models = []
        #self.user_account_model = None
        #self.friend_models = {}
        #self.chatroom_models = {}
        #self.ar = AppResources()
        #self.ar.set_thum_config("pic_thum", "Image")
        #self.ar.set_thum_config("video_thum", "Video")
        #self.ar.set_thum_config("jpg", "Video")
        #self.ar.set_thum_config("thumb", "Video")
        #self.ar.set_thum_config("cover", "Video")
        #self.ar.set_thum_config("extern", "Video")
        #self.ar.set_thum_config("pic", "Video")

    def _process_parse_message_deal(self, xml_element, model):
        if xml_element.Name.LocalName == 'msg':
            fromusername = xml_element.Element('fromusername')
            if fromusername and len(fromusername.Value) > 0:
                model.sender_id = fromusername.Value
            appmsg = xml_element.Element('appmsg')
            if appmsg is not None:
                wcpayinfo = appmsg.Element('wcpayinfo')
                if appmsg.Element('des') is not None:
                    model.deal_description = appmsg.Element('des').Value
                try:
                    msg_type = int(appmsg.Element('type').Value) if appmsg.Element('type') else 0
                except Exception as e:
                    msg_type = 0
                if msg_type == 2000:
                    model.type = model_wechat.MESSAGE_CONTENT_TYPE_TRANSFER
                    model.deal_status = model_wechat.DEAL_STATUS_TRANSFER_NONE
                    if wcpayinfo is not None:
                        if wcpayinfo.Element('feedesc') is not None:
                            model.deal_money = wcpayinfo.Element('feedesc').Value
                        if wcpayinfo.Element('invalidtime') is not None:
                            try:
                                model.deal_expire_time = int(wcpayinfo.Element('invalidtime').Value)
                            except Exception as e:
                                pass
                        if wcpayinfo.Element('pay_memo') is not None:
                            model.deal_remark = wcpayinfo.Element('pay_memo').Value
                        if wcpayinfo.Element('paysubtype') is not None:
                            paysubtype = wcpayinfo.Element('pay_memo').Value
                            if paysubtype == 1:
                                model.deal_status = model_wechat.DEAL_STATUS_TRANSFER_UNRECEIVED
                            elif paysubtype in [3, 8]:
                                model.deal_status = model_wechat.DEAL_STATUS_TRANSFER_RECEIVED
                            elif paysubtype in [4, 9]:
                                model.deal_status = model_wechat.DEAL_STATUS_TRANSFER_BACK
                elif msg_type == 2001:
                    if wcpayinfo is not None:
                        newaa = wcpayinfo.Element('newaa')
                        newaatype = 0
                        receiverlist = ''
                        payerlist = ''
                        if newaa:
                            if newaa.Element('newaatype'):
                                try:
                                    newaatype = int(newaa.Element('newaatype').Value)
                                except Exception as e:
                                    pass
                            if newaa.Element('receiverlist'):
                                receiverlist = newaa.Element('receiverlist').Value
                            if newaa.Element('payerlist'):
                                payerlist = newaa.Element('payerlist').Value
                        if newaatype != 0:
                            model.type = model_wechat.MESSAGE_CONTENT_TYPE_SPLIT_BILL
                            model.deal_status = model_wechat.DEAL_STATUS_SPLIT_BILL_NONE
                            if newaatype == 2:
                                model.deal_mode = model_wechat.DEAL_MODE_IDENTICAL
                            elif newaatype == 3:
                                model.deal_mode = model_wechat.DEAL_MODE_SPECIFIED
                            else:
                                model.deal_mode = model_wechat.DEAL_MODE_NONE
                            if wcpayinfo.Element('receiverdes'):
                                model.deal_description = wcpayinfo.Element('receiverdes').Value
                            if wcpayinfo.Element('receivertitle'):
                                model.deal_remark = wcpayinfo.Element('receivertitle').Value
                            if model.sender_id == self.user_account_model.Account:
                                rs = receiverlist.split(',')
                                if len(rs) >= 4:
                                    try:
                                        status = int(rs[1])
                                        if status in [1, 2]:
                                            model.deal_status = model_wechat.DEAL_STATUS_SPLIT_BILL_UNDONE
                                        elif status >= 3:
                                            model.deal_status = model_wechat.DEAL_STATUS_SPLIT_BILL_DONE
                                    except Exception as e:
                                        pass
                            else:
                                model.deal_status = model_wechat.DEAL_STATUS_SPLIT_BILL_NONEED
                                payers = payerlist.split('|')
                                for payer in payers:
                                    ps = payer.split(',')
                                    if len(ps) >= 3 and ps[0] == self.user_account_model.Account:
                                        try:
                                            status = int(ps[2])
                                            if status == 1:
                                                model.deal_status = model_wechat.DEAL_STATUS_SPLIT_BILL_UNPAID
                                            elif status >= 2:
                                                model.deal_status = model_wechat.DEAL_STATUS_SPLIT_BILL_PAID
                                        except Exception as e:
                                            pass
                                        break
                        else:
                            model.type = model_wechat.MESSAGE_CONTENT_TYPE_RED_ENVELPOE
                            if wcpayinfo.Element('receivertitle'):
                                model.deal_remark = wcpayinfo.Element('receivertitle').Value
                            status = model_wechat.DEAL_STATUS_RED_ENVELOPE_NONE
                            if wcpayinfo.Element('redenvelopereceiveamount'):
                                try:
                                    amount = int(wcpayinfo.Element('redenvelopereceiveamount').Value)
                                    status = model_wechat.DEAL_STATUS_RED_ENVELOPE_OPENED if amount > 0 else model_wechat.DEAL_STATUS_RED_ENVELOPE_UNOPENED
                                except Exception as e:
                                    pass
                            model.deal_status = status 

    def _process_parse_message_location(self, xml_str, model):
        content = xml_str
        xml = None
        try:
            xml = XElement.Parse(xml_str)
        except Exception as e:
            pass
            #if model.deleted == 0:
            #    TraceService.Trace(TraceLevel.Error, "base_wechat.py Error: LINE {}".format(traceback.format_exc()))
        if xml is not None:
            latitude = 0
            longitude = 0
            loc = xml.Element('location')
            if loc.Attribute('x'):
                try:
                    latitude = float(loc.Attribute('x').Value)
                except Exception as e:
                    pass
            if loc.Attribute('y'):
                try:
                    longitude = float(loc.Attribute('y').Value)
                except Exception as e:
                    pass
            if latitude != 0 or longitude != 0:
                content = ''
                model.location_type = model_wechat.LOCATION_TYPE_GOOGLE
                model.location_latitude = latitude
                model.location_longitude = longitude
                model.location_address = ''
                if loc.Attribute('label'):
                    model.location_address += loc.Attribute('label').Value + ' '
                if loc.Attribute('poiname') and loc.Attribute('poiname').Value not in ['[位置]', '[Location]']:
                    model.location_address += loc.Attribute('poiname').Value
        return content

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

    def _strip_message_content(self, content):
        try:
            com = re.compile(r'<.*?>')
            return com.sub('', content)
        except Exception as e:
            print(e)
            return content

    def _process_parse_message_system_xml(self, xml_str):
        content = xml_str
        revoke_content = None
        xml = None
        try:
            xml = XElement.Parse(xml_str)
        except Exception as e:
            pass
        if xml:
            if xml.Element('sysmsgtemplate') and xml.Element('sysmsgtemplate').Element('content_template'):
                content_template = xml.Element('sysmsgtemplate').Element('content_template')
                template = content_template.Element('template').Value
                username = ''
                names = ''
                if content_template.Element('link_list'):
                    links = content_template.Element('link_list').Elements('link')
                    for link in links:
                        if link.Attribute('name'):
                            if link.Attribute('name').Value == 'username':
                                username = self._process_parse_message_system_xml_templete_link_name(link)
                            elif link.Attribute('name').Value == 'names':
                                names = self._process_parse_message_system_xml_templete_link_name(link)
                content = template.replace('$username$', username).replace('$names$', names)
            elif xml.Element('editrevokecontent'):
                revoke = xml.Element('editrevokecontent')
                if revoke.Element('text'):
                    content = revoke.Element('text').Value
                if revoke.Element('link') and revoke.Element('link').Element('revokecontent'):
                    revoke_content = revoke.Element('link').Element('revokecontent').Value
        return content, revoke_content

    def _process_parse_message_system_xml_templete_link_name(self, link):
        content = ''
        if link and link.Element('memberlist'):
            members = link.Element('memberlist').Elements('member')
            for member in members:
                if member.Element('nickname'):
                    if len(content) > 0:
                        content += '、'
                    content += member.Element('nickname').Value
        return content

    def _process_parse_message_contact_card(self, xml_str, model):
        content = xml_str
        xml = None
        try:
            xml = XElement.Parse(xml_str)
        except Exception as e:
            pass
        if xml and xml.Name.LocalName == 'msg':
            content = ''
            if xml.Attribute('username'):
                model.business_card_username = xml.Attribute('username').Value
            if xml.Attribute('nickname'):
                model.business_card_nickname = xml.Attribute('nickname').Value
            if xml.Attribute('sex'):
                try:
                    model.business_card_gender = int(xml.Attribute('sex').Value)
                except Exception as e:
                    pass
            if xml.Attribute('bigheadimgurl'):
                model.business_card_photo = xml.Attribute('bigheadimgurl').Value
            if model.business_card_photo in [None, ''] and xml.Attribute('smallheadimgurl'):
                model.business_card_photo = xml.Attribute('smallheadimgurl').Value
            if xml.Attribute('regionCode'):
                model.business_card_region = xml.Attribute('regionCode').Value
            if xml.Attribute('sign'):
                model.business_card_signature = xml.Attribute('sign').Value
        return content

    def _parse_segment(self, comment_row):
        try:
            sign_head_length = 0x0c

            sender_length = comment_row[sign_head_length]
            sender_start_index = sign_head_length + 0x01
            sender_end_index = sign_head_length + sender_length
            sender_id = comment_row[sender_start_index:(sender_end_index + 0x01)]

            receiver_id_length = comment_row[(sender_end_index + 0x02)]
            receiver_id_start_index = sender_end_index + 0x03
            receiver_id_end_index = receiver_id_start_index + receiver_id_length
            receiver_id = comment_row[receiver_id_start_index: receiver_id_end_index]

            receiver_nickname_length = comment_row[(receiver_id_end_index + 0x01)]
            receiver_nickname_start_index = receiver_id_end_index + 0x02
            receiver_nickname_end_index = receiver_nickname_start_index + receiver_nickname_length
            receiver_nickname = comment_row[receiver_nickname_start_index:receiver_nickname_end_index]

            try:
                comment_length = comment_row[receiver_nickname_end_index + 0x07]
                comment_start_index = receiver_nickname_end_index + 0x08
                comment_end_index = comment_start_index + comment_length
                comment = comment_row[comment_start_index:comment_end_index]
            except Exception as e:
                comment = b''

            content = comment.decode(encoding="utf-8")
            sender_id = sender_id.decode(encoding="utf-8")
            return content, sender_id
        except Exception as e:
            return None

    def _process_parse_story_comment(self, data):
        if not data:
            return
        sign_length = 0x0c
        length = len(data)
        prefix = data[:sign_length]
        index = 0x00
        segment_index = []
        while index < length:
            compare_data = data[index:(index + sign_length)]
            if operator.eq(compare_data, prefix):
                segment_index.append(index)
            index += 0x01
        segment_data = [data[segment_index[i]:segment_index[i + 1]] for i in range(len(segment_index) - 1)]
        story_comments_row = segment_data[1:]
        for i in story_comments_row:
            comment = self._parse_segment(i)
            if comment:
                yield comment

    def _parse_user_type_is_blocked(self, user_type):
        return (user_type & 1<<3) != 0

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
                #if record.Deleted != DeletedState.Intact:
                #    value = filter(lambda x: x in string.printable, value)
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
        return reader.GetString(index) if not reader.IsDBNull(index) else default_value

    @staticmethod
    def _db_reader_get_int_value(reader, index, default_value=0):
        return reader.GetInt64(index) if not reader.IsDBNull(index) else default_value

    @staticmethod
    def _db_reader_get_blob_value(reader, index, default_value=None):
        if not reader.IsDBNull(index):
            try:
                return bytes(reader.GetValue(index))
            except Exception as e:
                return default_value
        else:
            return default_value

        return reader.GetString(index) if not reader.IsDBNull(index) else default_value

    @staticmethod
    def _db_record_get_blob_value_to_ba(record, column, default_value=None):
        if not record[column].IsDBNull:
            try:
                value = record[column].Value
                return bytearray(value)
            except Exception as e:
                return default_value
        return default_value

    @staticmethod
    def _bpreader_node_get_string_value(node, key, default_value='', deleted=0):
        if key in node.Children and node.Children[key] is not None:
            try:
                value = str(node.Children[key].Value)
                #if deleted != 0:
                #    value = filter(lambda x: x in string.printable, value)
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
        if msg_type in [MSG_TYPE_TEXT, MSG_TYPE_VOIP, MSG_TYPE_VOIP_GROUP]:
            return model_wechat.MESSAGE_CONTENT_TYPE_TEXT
        elif msg_type in [MSG_TYPE_IMAGE, MSG_TYPE_EMOJI]:
            return model_wechat.MESSAGE_CONTENT_TYPE_IMAGE
        elif msg_type == MSG_TYPE_VOICE:
            return model_wechat.MESSAGE_CONTENT_TYPE_VOICE
        elif msg_type in [MSG_TYPE_VIDEO, MSG_TYPE_VIDEO_2]:
            return model_wechat.MESSAGE_CONTENT_TYPE_VIDEO
        elif msg_type == MSG_TYPE_LOCATION:
            return model_wechat.MESSAGE_CONTENT_TYPE_LOCATION
        elif msg_type in [MSG_TYPE_SYSTEM, MSG_TYPE_SYSTEM_2, MSG_TYPE_SYSTEM_3]:
            return model_wechat.MESSAGE_CONTENT_TYPE_SYSTEM
        elif msg_type == MSG_TYPE_APP_MESSAGE:
            return model_wechat.MESSAGE_CONTENT_TYPE_APPMESSAGE
        elif msg_type == MSG_TYPE_LINK_SEMI:
            return model_wechat.MESSAGE_CONTENT_TYPE_LINK_SET
        else:
            return model_wechat.MESSAGE_CONTENT_TYPE_LINK

    @staticmethod
    def _convert_gender_type(gender_type):
        if gender_type != 0:
            return model_wechat.GENDER_FEMALE
        else:
            return model_wechat.GENDER_MALE

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

        Wechat.db_fix_header(dst_path)
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

    @staticmethod
    def create_memory_node(parent, rfs_path, vfs_name):
        """
            rfs_path:REAL FILE SYSTEM FILE PATH(ABSOLUTE)
            vfs_name:file_name in virtual file system
            ret:node which compact with vfs
        """
        mem_range = MemoryRange.CreateFromFile(rfs_path)
        r_node = Node(vfs_name, Files.NodeType.Embedded)
        r_node.Data = mem_range
        parent.Children.Add(r_node) # ^_^ must add this to virtual file system
        return r_node

    @staticmethod
    def _md5(src):
        try:
            m = hashlib.md5()
            m.update(src.encode('utf8'))
            return m.hexdigest()
        except Exception as e:
            return None

    def set_progress(self, value):
        v = value
        if v > 100:
            v = 100
        elif v < 0:
            v = 0
        if self.progress is not None and v != self.progress.Value:
            self.progress.Value = v
            #print('set_progress() %d' % v)

    def add_model(self, model):
        if model is not None:
            self.models.append(model)
            if len(self.models) >= 1000:
                self.push_models()

    def push_models(self):
        if len(self.models) > 0:
            pr = ParserResults()
            pr.Categories = DescripCategories.Wechat
            pr.Models.AddRange(self.models)
            pr.Build(self.build)
            ds.Add(pr)
            self.models = []

    def get_account_model(self, account):
        try:
            model = WeChat.UserAccount()
            model.SourceFile = account.source
            model.Deleted = model_wechat.GenerateModel._convert_deleted_status(account.deleted)
            model.Account = account.account_id
            model.CustomAccount = account.account_id_alias
            model.NickName = account.nickname
            model.HeadPortraitPath = account.photo
            model.Gender = model_wechat.GenerateModel._convert_gender(account.gender)
            model.Region = account.city
            model.Signature = account.signature
            model.PhoneNumber = account.telephone
            model.Email = account.email
            return model
        except Exception as e:
            #print(e)
            return None

    def get_login_device_model(self, login_device):
        try:
            model = IM.LoginDevice()
            model.SourceFile = login_device.source
            model.Deleted = model_wechat.GenerateModel._convert_deleted_status(login_device.deleted)
            model.AppUserAccount = self.user_account_model
            model.Id = login_device.id
            model.Name = login_device.name
            model.Type = login_device.type
            model.LastLoginTime = model_wechat.GenerateModel._get_timestamp(login_device.last_time)
            return model
        except Exception as e:
            #print(e)
            return None

    def get_friend_model(self, friend):
        try:
            model = WeChat.Friend()
            model.SourceFile = friend.source
            model.Deleted = model_wechat.GenerateModel._convert_deleted_status(friend.deleted)
            model.AppUserAccount = self.user_account_model
            model.Account = friend.friend_id
            model.CustomAccount = friend.friend_id_alias
            model.NickName = friend.nickname
            model.HeadPortraitPath = friend.photo
            model.Gender = model_wechat.GenerateModel._convert_gender(friend.gender)
            model.Region = friend.region
            model.Signature = friend.signature
            model.RemarkName = friend.remark
            model.Type = model_wechat.GenerateModel._convert_friend_type(friend.type)
            return model
        except Exception as e:
            #print(e)
            return None

    def get_chatroom_models(self, cache_db):
        db = SQLite.SQLiteConnection('Data Source = {}'.format(cache_db))
        db.Open()

        sql = '''select account_id, chatroom_id, name, photo, is_saved, notice, owner_id, create_time, join_time, 
                        sp_id, source, deleted, repeated
                 from chatroom'''
        try:
            cmd = db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                deleted = 0
                try:
                    source = self._db_reader_get_string_value(r, 10)
                    deleted = self._db_reader_get_int_value(r, 11, None)
                    account_id = self._db_reader_get_string_value(r, 0)
                    user_id = self._db_reader_get_string_value(r, 1)
                    nickname = self._db_reader_get_string_value(r, 2)
                    photo = self._db_reader_get_string_value(r, 3, None)
                    is_saved = self._db_reader_get_int_value(r, 4)
                    notice = self._db_reader_get_string_value(r, 5)
                    owner_id = self._db_reader_get_string_value(r, 6)
                    create_time = self._db_reader_get_int_value(r, 7)
                    join_time = self._db_reader_get_int_value(r, 8)
                    sp_id = self._db_reader_get_int_value(r, 9)

                    if account_id in [None, ''] or user_id in [None, '']:
                        continue

                    model = WeChat.Group()
                    model.SourceFile = source
                    model.Deleted = model_wechat.GenerateModel._convert_deleted_status(deleted)
                    model.AppUserAccount = self.user_account_model
                    model.Account = user_id
                    model.NickName = nickname
                    model.HeadPortraitPath = photo
                    model.Notice = notice
                    model.IsSave = is_saved != 0
                    member_models, owner_model = self.get_chatroom_member_models(db, account_id, user_id, sp_id, deleted, owner_id)
                    model.GroupOwner = owner_model
                    model.Members.AddRange(member_models)
                    model.JoinTime = model_wechat.GenerateModel._get_timestamp(join_time)
                    self.add_model(model)

                    if deleted == 0 or user_id not in self.chatroom_models:
                        self.chatroom_models[user_id] = model
                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "base_wechat.py Error: LINE {}".format(traceback.format_exc()))
            self.push_models()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "base_wechat.py Error: LINE {}".format(traceback.format_exc()))

        self.push_models()
        db.Close()

    def get_chatroom_member_models(self, db,  account_id, chatroom_id, sp_id, deleted, owner_id):
        models = []
        owner_model = None
        if sp_id not in [None, 0]:
            sql = '''select member_id, display_name
                     from chatroom_member
                     where account_id='{0}' and chatroom_id='{1}' and sp_id='{2}' '''.format(account_id, chatroom_id, sp_id)
        else:
            sql = '''select member_id, display_name
                     from chatroom_member
                     where account_id='{0}' and chatroom_id='{1}' '''.format(account_id, chatroom_id)
        try:
            cmd = db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                try:
                    member_id = self._db_reader_get_string_value(r, 0)
                    display_name = self._db_reader_get_string_value(r, 1)
                    if member_id not in [None, '']:
                        model = GroupMember()
                        model.User = self.friend_models.get(member_id)
                        if model.User is not None:
                            model.SourceFile = model.User.SourceFile
                            model.Deleted = model.User.Deleted
                        model.NickName = display_name
                        models.append(model)
                        if member_id == owner_id:
                            owner_model = model
                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "base_wechat.py Error: LINE {}".format(traceback.format_exc()))
        except Exception as e:
            if deleted == 0:
                TraceService.Trace(TraceLevel.Error, "base_wechat.py Error: LINE {}".format(traceback.format_exc()))
        return models, owner_model

    def get_chatroom_model(self, chatroom):
        try:
            model = IM.WeChat.Group()
            model.SourceFile = chatroom.source
            model.Deleted = model_wechat.GenerateModel._convert_deleted_status(chatroom.deleted)
            model.AppUserAccount = self.user_account_model
            model.Account = chatroom.chatroom_id
            model.NickName = chatroom.name
            model.HeadPortraitPath = chatroom.photo
            model.Notice = chatroom.notice
            model.IsSave = chatroom.is_saved != 0
            return model
        except Exception as e:
            #print(e)
            return None

    def get_chatroom_member_model(self, chatroom_member):
        try:
            model = self.friend_models.get(chatroom_member.member_id)
            return model
        except Exception as e:
            #print(e)
            return None

    def get_message_model(self, message):
        try:
            timeline_model = None
            if message.talker_id.endswith("@chatroom"):
                model = WeChat.GroupMessage()
                model.Group = self.chatroom_models.get(message.talker_id)
            else:
                model = WeChat.FriendMessage()
                model.Friend = self.friend_models.get(message.talker_id)
            model.SourceFile = message.source
            model.Deleted = model_wechat.GenerateModel._convert_deleted_status(message.deleted)
            model.AppUserAccount = self.user_account_model
            model.Sender = self.friend_models.get(message.sender_id)
            #model.SourceData = message.content
            model.CreateTime = model_wechat.GenerateModel._get_timestamp(message.timestamp)
            if message.type == model_wechat.MESSAGE_CONTENT_TYPE_SYSTEM:
                model.Way = CommonEnum.MessageWay.System
            elif message.sender_id == self.user_account_model.Account:
                model.Way = CommonEnum.MessageWay.Send
            else:
                model.Way = CommonEnum.MessageWay.Receive
            
            if message.type == model_wechat.MESSAGE_CONTENT_TYPE_IMAGE:
                model.Content = Base.Content.ImageContent(model)
                media_model = Base.MediaFile.ImageFile(model)
                media_model.Path = message.media_path
                model.Content.Value = media_model
                if model_wechat.is_valid_media_model_path(message.media_path):
                    self.ar.save_media_model(media_model)
            elif message.type == model_wechat.MESSAGE_CONTENT_TYPE_VOICE:
                model.Content = Base.Content.VoiceContent(model)
                media_model = Base.MediaFile.AudioFile(model)
                media_model.Path = message.media_path
                model.Content.Value = media_model
                if model_wechat.is_valid_media_model_path(message.media_path):
                    self.ar.save_media_model(media_model)
            elif message.type == model_wechat.MESSAGE_CONTENT_TYPE_VIDEO:
                model.Content = Base.Content.VideoContent(model)
                if message.media_path not in [None, '']:
                    media_model = Base.MediaFile.VideoFile(model)
                    media_model.Path = message.media_path
                    model.Content.Value = media_model
                    if model_wechat.is_valid_media_model_path(message.media_path):
                        self.ar.save_media_model(media_model)
                elif model_wechat.is_valid_media_model_path(message.media_thum_path):
                    media_model = Base.MediaFile.VideoThumbnailFile(model)
                    media_model.Deleted = model_wechat.GenerateModel._convert_deleted_status(1)
                    media_model.Path = message.media_thum_path
                    model.Content.Value = media_model
                    self.ar.save_media_model(media_model)
                else:
                    media_model = Base.MediaFile.VideoFile(model)
                    media_model.Path = message.media_path
                    model.Content.Value = media_model
            elif message.type == model_wechat.MESSAGE_CONTENT_TYPE_CONTACT_CARD:
                model.Content = Base.Content.BusinessCardContent(model)
                model.Content.Value = WeChat.BusinessCard()
                #model.Content.Value.AppUserAccount = 
                model.Content.Value.UserID = message.business_card_username
                model.Content.Value.NickName = message.business_card_nickname
                model.Content.Value.Gender = model_wechat.GenerateModel._convert_gender(message.business_card_gender)
                model.Content.Value.Region = message.business_card_region
                model.Content.Value.Signature = message.business_card_signature
            elif message.type == model_wechat.MESSAGE_CONTENT_TYPE_LOCATION:
                model.Content = Base.Content.LocationContent(model)
                model.Content.Value = Base.Location()
                model.Content.Value.SourceType = LocationSourceType.App
                model.Content.Value.Time = model.CreateTime
                model.Content.Value.AddressName = message.location_address
                model.Content.Value.Coordinate = Base.Coordinate(message.location_longitude, message.location_latitude, model_wechat.GenerateModel._convert_location_type(message.location_type))
                timeline_model = model.Content.Value
            elif message.type == model_wechat.MESSAGE_CONTENT_TYPE_LINK:
                model.Content = Base.Content.LinkContent(model)
                model.Content.Value = Base.Link()
                model.Content.Value.Title = message.link_title
                model.Content.Value.Description = message.link_content
                model.Content.Value.Url = message.link_url
                model.Content.Value.ImagePath = message.link_image
            elif message.type == model_wechat.MESSAGE_CONTENT_TYPE_ATTACHMENT:
                model.Content = Base.Content.AttachmentContent(model)
                model.Content.Value = Base.Attachment()
                model.Content.Value.FileName = message.content
                model.Content.Value.Path = message.media_path
            elif message.type == model_wechat.MESSAGE_CONTENT_TYPE_RED_ENVELPOE:
                model.Content = Base.Content.RedEnvelopeContent(model)
                model.Content.Value = WeChat.RedEnvelope()
                model.Content.Value.Expiration = model_wechat.GenerateModel._get_timestamp(message.deal_expire_time)
                model.Content.Value.Title = message.deal_description
                model.Content.Value.Remark = message.deal_remark
                model.Content.Value.Status = model_wechat.GenerateModel._convert_deal_status(message.deal_status)
            elif message.type == model_wechat.MESSAGE_CONTENT_TYPE_SPLIT_BILL:
                model.Content = Base.Content.SplitBillContent(model)
                model.Content.Value = WeChat.SplitBill()
                model.Content.Value.Expiration = model_wechat.GenerateModel._get_timestamp(message.deal_expire_time)
                model.Content.Value.Title = message.deal_description
                model.Content.Value.Remark = message.deal_remark
                model.Content.Value.Mode = model_wechat.GenerateModel._convert_deal_mode(message.deal_mode)
                model.Content.Value.Status = model_wechat.GenerateModel._convert_deal_status(message.deal_status)
            elif message.type == model_wechat.MESSAGE_CONTENT_TYPE_TRANSFER:
                model.Content = Base.Content.TransferContent(model)
                model.Content.Value = WeChat.Transfer()
                model.Content.Value.Expiration = model_wechat.GenerateModel._get_timestamp(message.deal_expire_time)
                model.Content.Value.Title = message.deal_description
                model.Content.Value.Remark = message.deal_remark
                model.Content.Value.MoneyOfString = message.deal_money
                model.Content.Value.Status = model_wechat.GenerateModel._convert_deal_status(message.deal_status)
            elif message.type == model_wechat.MESSAGE_CONTENT_TYPE_APPMESSAGE:
                model.Content = Base.Content.TemplateContent(model)
                model.Content.Title = message.link_title
                model.Content.Content = message.link_content
                model.Content.InfoUrl = message.link_url
                model.Content.SendTime = model_wechat.GenerateModel._get_timestamp(message.timestamp)
            elif message.type == model_wechat.MESSAGE_CONTENT_TYPE_LINK_SET:
                model.Content = Base.Content.LinkSetContent(model)
                items = []
                try:
                    items = json.loads(message.content)
                except Exception as e:
                    #print(e)
                    pass
                for item in items:
                    link = Base.Link()
                    link.Title = item.get('title')
                    link.Description = item.get('description')
                    link.Url = item.get('url')
                    link.ImagePath = item.get('image')
                    model.Content.Values.Add(link)
            else:
                model.Content = Base.Content.TextContent(model)
                model.Content.Value = message.content

            return model, timeline_model
        except Exception as e:
            #print(e)
            return None, None

    def get_feed_model(self, feed):
        try:
            timeline_model = None
            model = WeChat.Dynamic()
            model.SourceFile = feed.source
            model.Deleted = model_wechat.GenerateModel._convert_deleted_status(feed.deleted)
            model.AppUserAccount = self.user_account_model
            #model.SourceData
            model.Sender = self.friend_models.get(feed.sender_id)
            model.CreateTime = model_wechat.GenerateModel._get_timestamp(feed.timestamp)
            if feed.content not in [None, '']:
                text_content = Base.Content.TextContent(model)
                text_content.Value = feed.content
                model.Contents.Add(text_content)
            if feed.image_path not in [None, '']:
                images_content = Base.Content.ImagesContent(model)
                images = feed.image_path.split(',')
                for image in images:
                    if image not in [None, '']:
                        media_model = Base.MediaFile.ImageFile(model)
                        media_model.Path = image
                        images_content.Values.Add(media_model)
                        if model_wechat.is_valid_media_model_path(image):
                            self.ar.save_media_model(media_model)
                model.Contents.Add(images_content)
            if feed.video_path not in [None, '']:
                videos = feed.video_path.split(',')
                for video in videos:
                    if video not in [None, '']:
                        video_content = Base.Content.VideoContent(model)
                        media_model = Base.MediaFile.VideoFile(model)
                        media_model.Path = video
                        video_content.Value = media_model
                        model.Contents.Add(video_content)
                        if model_wechat.is_valid_media_model_path(video):
                            self.ar.save_media_model(media_model)
            if feed.link_url not in [None, '']:
                link_content = Base.Content.LinkContent(model)
                link_content.Value = Base.Link()
                link_content.Value.Title = feed.link_title
                link_content.Value.Description = feed.link_content
                link_content.Value.Url = feed.link_url
                link_content.Value.ImagePath = feed.link_image
                model.Contents.Add(link_content)
            if feed.location_latitude != 0 or feed.location_longitude != 0:
                location_content = Base.Content.LocationContent(model)
                location_content.Value = Base.Location()
                location_content.Value.SourceType = LocationSourceType.App
                location_content.Value.Time = model.CreateTime
                location_content.Value.AddressName = feed.location_address
                location_content.Value.Coordinate = Base.Coordinate(feed.location_longitude, feed.location_latitude, model_wechat.GenerateModel._convert_location_type(feed.location_type))
                model.Contents.Add(location_content)
                timeline_model = location_content
            for like in feed.likes:
                model.Likers.Add(self.get_feed_like_model(model, like))
            for comment in feed.comments:
                model.Comments.Add(self.get_feed_comment_model(model, comment))
            return model, timeline_model
        except Exception as e:
            print(e)
            return None, None

    def get_feed_like_model(self, feed_model, feed_like):
        try:
            model = Base.Like(feed_model)
            model.SourceFile = feed_like.source
            model.Deleted = model_wechat.GenerateModel._convert_deleted_status(feed_like.deleted)
            model.Sender = self.friend_models.get(feed_like.sender_id)
            model.CreateTime = model_wechat.GenerateModel._get_timestamp(feed_like.timestamp)
            return model
        except Exception as e:
            #print(e)
            return None

    def get_feed_comment_model(self, feed_model, feed_comment):
        try:
            model = Base.Comment(feed_model)
            model.SourceFile = feed_comment.source
            model.Deleted = model_wechat.GenerateModel._convert_deleted_status(feed_comment.deleted)
            model.From = self.friend_models.get(feed_comment.sender_id)
            model.To = self.friend_models.get(feed_comment.ref_user_id)
            model.Content = feed_comment.content
            model.CreateTime = model_wechat.GenerateModel._get_timestamp(feed_comment.timestamp)
            return model
        except Exception as e:
            #print(e)
            return None

    def get_search_model(self, search):
        try:
            model = SearchRecord()
            model.SourceFile = search.source
            model.Deleted = model_wechat.GenerateModel._convert_deleted_status(search.deleted)
            model.AppUserAccount = self.user_account_model
            model.Keyword = search.key
            model.CreateTime = model_wechat.GenerateModel._get_timestamp(search.timestamp)
            return model
        except Exception as e:
            #print(e)
            return None

    def get_favorite_model(self, favorite):
        try:
            model = Base.Favorites()
            model.SourceFile = favorite.source
            model.Deleted = model_wechat.GenerateModel._convert_deleted_status(favorite.deleted)
            model.AppUserAccount = self.user_account_model
            model.CreateTime = model_wechat.GenerateModel._get_timestamp(favorite.timestamp)
            for item in favorite.items:
                model.Contents.Add(self.get_favorite_item_model(item))
            return model
        except Exception as e:
            #print(e)
            return None

    def get_favorite_item_model(self, favorite_item):
        try:
            model = Base.FavoritesContent()
            model.Sender = self.friend_models.get(favorite_item.sender_id)
            model.CreateTime = model_wechat.GenerateModel._get_timestamp(favorite_item.timestamp)
            if favorite_item.type == model_wechat.FAV_TYPE_IMAGE:
                model.Content = Base.Content.ImageContent(model)
                media_model = Base.MediaFile.ImageFile(model)
                media_model.Path = favorite_item.media_path
                model.Content.Value = media_model
                if model_wechat.is_valid_media_model_path(favorite_item.media_path):
                    self.ar.save_media_model(media_model)
            elif favorite_item.type == model_wechat.FAV_TYPE_VOICE:
                model.Content = Base.Content.VoiceContent(model)
                media_model = Base.MediaFile.AudioFile(model)
                media_model.Path = favorite_item.media_path
                model.Content.Value = media_model
                if model_wechat.is_valid_media_model_path(favorite_item.media_path):
                    self.ar.save_media_model(media_model)
            elif favorite_item.type == model_wechat.FAV_TYPE_VIDEO:
                model.Content = Base.Content.VideoContent(model)
                media_model = Base.MediaFile.VideoFile(model)
                media_model.Path = favorite_item.media_path
                model.Content.Value = media_model
                if model_wechat.is_valid_media_model_path(favorite_item.media_path):
                    self.ar.save_media_model(media_model)
            elif favorite_item.type in [model_wechat.FAV_TYPE_LINK, model_wechat.FAV_TYPE_MUSIC]:
                model.Content = Base.Content.LinkContent(model)
                model.Content.Value = Base.Link()
                model.Content.Value.Title = favorite_item.link_title
                model.Content.Value.Description = favorite_item.link_content
                model.Content.Value.Url = favorite_item.link_url
                model.Content.Value.ImagePath = favorite_item.link_image
            elif favorite_item.type == model_wechat.FAV_TYPE_LOCATION:
                model.Content = Base.Content.LocationContent(model)
                model.Content.Value = Base.Location()
                model.Content.Value.SourceType = LocationSourceType.App
                model.Content.Value.Time = model_wechat.GenerateModel._get_timestamp(favorite_item.timestamp)
                model.Content.Value.AddressName = favorite_item.location_address
                model.Content.Value.Coordinate = Base.Coordinate(favorite_item.location_longitude, favorite_item.location_latitude, model_wechat.GenerateModel._convert_location_type(favorite_item.location_type))
            elif favorite_item.type == model_wechat.FAV_TYPE_ATTACHMENT:
                model.Content = Base.Content.AttachmentContent(model)
                model.Content.Value = Base.Attachment()
                model.Content.Value.FileName = favorite_item.content
                model.Content.Value.Path = favorite_item.media_path
            else:
                model.Content = Base.Content.TextContent(model)
                model.Content.Value = favorite_item.content
            return model
        except Exception as e:
            #print(e)
            return None

    def get_contact_label_model(self, label):
        try:
            model = None
            if label.type == model_wechat.CONTACT_LABEL_TYPE_GROUP:
                model = FriendGroup()
                model.SourceFile = label.source
                model.Deleted = model_wechat.GenerateModel._convert_deleted_status(label.deleted)
                model.AppUserAccount = self.user_account_model
                model.Name = label.name
                for user_id in label.users:
                    friend = self.friend_models.get(user_id)
                    if friend is not None:
                        model.Friends.Add(friend)
            elif label.type == model_wechat.CONTACT_LABEL_TYPE_EMERGENCY:
                model = Base.EmergencyContacts()
                model.SourceFile = label.source
                model.Deleted = model_wechat.GenerateModel._convert_deleted_status(label.deleted)
                model.AppUserAccount = self.user_account_model
                for user_id in label.users:
                    friend = self.friend_models.get(user_id)
                    if friend is not None:
                        model.Friends.Add(friend)
            return model
        except Exception as e:
            #print(e)
            return None

    def get_bank_card_model(self, card):
        try:
            model = Base.BankCard()
            model.SourceFile = card.source
            model.Deleted = model_wechat.GenerateModel._convert_deleted_status(card.deleted)
            model.AppUserAccount = self.user_account_model
            model.BankName = card.bank_name
            model.CardType = card.card_type
            model.CardNumber = card.card_number
            model.PhoneNumber = card.phone_number
            return model
        except Exception as e:
            #print(e)
            return None

    def get_story_model(self, story):
        try:
            timeline_model = None
            model = WeChat.Story()
            model.SourceFile = story.source
            model.Deleted = model_wechat.GenerateModel._convert_deleted_status(story.deleted)
            model.AppUserAccount = self.user_account_model
            model.Sender = self.friend_models.get(story.sender_id)
            model.CreateTime = model_wechat.GenerateModel._get_timestamp(story.timestamp)
            if story.media_path not in [None, '']:
                video_content = Base.Content.VideoContent(model)
                media_model = Base.MediaFile.VideoFile()
                media_model.Path = story.media_path
                video_content.Value = media_model
                model.Contents.Add(video_content)
                if model_wechat.is_valid_media_model_path(story.media_path):
                    self.ar.save_media_model(media_model)
            if story.location_latitude != 0 or story.location_longitude != 0:
                location_content = Base.Content.LocationContent(model)
                location_content.Value = Base.Location()
                location_content.Value.SourceType = LocationSourceType.App
                location_content.Value.Time = model.CreateTime
                location_content.Value.AddressName = story.location_address
                location_content.Value.Coordinate = Base.Coordinate(story.location_longitude, story.location_latitude, model_wechat.GenerateModel._convert_location_type(story.location_type))
                model.Contents.Add(location_content)
                timeline_model = location_content
            for comment in story.comments:
                model.Comments.Add(self.get_story_comment_model(model, comment))
            return model, timeline_model
        except Exception as e:
            print(e)
            return None, None

    def get_story_comment_model(self, story_model, story_comment):
        try:
            model = Base.Comment(story_model)
            model.SourceFile = story_comment.source
            model.Deleted = model_wechat.GenerateModel._convert_deleted_status(story_comment.deleted)
            model.From = self.friend_models.get(story_comment.sender_id)
            model.Content = story_comment.content
            model.CreateTime = model_wechat.GenerateModel._get_timestamp(story_comment.timestamp)
            return model
        except Exception as e:
            print(e)
            return None
