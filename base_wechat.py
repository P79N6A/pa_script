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
MSG_TYPE_SYSTEM_3 = 570425393  # xml
MSG_TYPE_LINK_SEMI = 285212721  # semi_xml

# 朋友圈类型
MOMENT_TYPE_IMAGE = 1  # 正常文字图片
MOMENT_TYPE_TEXT_ONLY = 2  # 纯文字
MOMENT_TYPE_SHARED = 3  # 分享
MOMENT_TYPE_MUSIC = 4  # 带音乐的（存的是封面）
MOMENT_TYPE_EMOJI = 10  # 分享了表情包
MOMENT_TYPE_VIDEO = 15  # 视频

# 收藏类型
FAV_TYPE_TEXT = 1  # 文本
FAV_TYPE_IMAGE = 2  # 图片
FAV_TYPE_VOICE = 3  # 语音
FAV_TYPE_VIDEO = 4  # 视频
FAV_TYPE_LINK = 5  # 链接
FAV_TYPE_LOCATION = 6  # 位置
FAV_TYPE_ATTACHMENT = 8  # 附件
FAV_TYPE_CHAT = 14  # 聊天记录
FAV_TYPE_VIDEO_2 = 16 # 视频


class Wechat(object):
    def __init__(self):
        self.im = model_im.IM()
        
    def _process_parse_message_link(self, xml_str, model):
        xml = None
        try:
            xml = XElement.Parse(xml_str)
        except Exception as e:
            if model.deleted == 0:
                TraceService.Trace(TraceLevel.Error, "base_wechat.py Error: LINE {}".format(traceback.format_exc()))
            model.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
            return

        if xml is not None:
            if xml.Name.LocalName == 'msg':
                appmsg = xml.Element('appmsg')
                if appmsg is not None:
                    try:
                        msg_type = int(appmsg.Element('type').Value) if appmsg.Element('type') else 0
                    except Exception as e:
                        msg_type = 0
                    if msg_type in [2000, 2001]:
                        self._process_parse_message_deal(xml, model)
                    else:
                        link = model.create_link()
                        msg_title = appmsg.Element('title').Value if appmsg.Element('title') else ''
                        mmreader = appmsg.Element('mmreader')
                        if mmreader:
                            category = mmreader.Element('category')
                            if category and category.Element('item'):
                                item = category.Element('item')
                                if item.Element('title'):
                                    link.title = item.Element('title').Value
                                if item.Element('digest'):
                                    link.content = item.Element('digest').Value
                                if item.Element('url'):
                                    link.url = item.Element('url').Value
                        else:
                            if appmsg.Element('title'):
                                link.title = appmsg.Element('title').Value
                            if appmsg.Element('des'):
                                link.content = appmsg.Element('des').Value
                            if appmsg.Element('url'):
                                link.url = appmsg.Element('url').Value
                            appinfo = xml.Element('appinfo')
                            if appinfo and appinfo.Element('appname'):
                                link.from_app = appinfo.Element('appname').Value
                else:
                    pass
            elif xml.Name.LocalName == 'mmreader':
                link = model.create_link()
                category = xml.Element('category')
                if category and category.Element('item'):
                    item = category.Element('item')
                    if item.Element('title'):
                        link.title = item.Element('title').Value
                    if item.Element('digest'):
                        link.content = item.Element('digest').Value
                    if item.Element('url'):
                        link.url = item.Element('url').Value
            elif xml.Name.LocalName == 'appmsg':
                link = model.create_link()
                if xml.Element('title'):
                    link.title = xml.Element('title').Value
                if xml.Element('des'):
                    link.content = xml.Element('des').Value
                if xml.Element('url'):
                    link.url = xml.Element('url').Value
                appinfo = xml.Element('appinfo')
                if appinfo and appinfo.Element('appname'):
                    link.from_app = appinfo.Element('appname').Value
            else:
                pass

    def _process_parse_message_deal(self, xml_element, model):
        deal = model.create_deal()
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
                    model.type = model_im.MESSAGE_CONTENT_TYPE_RECEIPT
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
                            model.type = model_im.MESSAGE_CONTENT_TYPE_AA_RECEIPT
                            if wcpayinfo.Element('receiverdes'):
                                deal.description = wcpayinfo.Element('receiverdes').Value
                            if wcpayinfo.Element('receivertitle'):
                                deal.remark = wcpayinfo.Element('receivertitle').Value
                        else:
                            deal.type = model_im.DEAL_TYPE_RED_ENVELPOE
                            model.type = model_im.MESSAGE_CONTENT_TYPE_RED_ENVELPOE
                            if wcpayinfo.Element('receivertitle'):
                                deal.remark = wcpayinfo.Element('receivertitle').Value

            fromusername = xml_element.Element('fromusername')
            if fromusername and len(fromusername.Value) > 0:
                model.sender_id = fromusername.Value

    def _process_parse_message_location(self, xml_str, model):
        xml = None
        try:
            xml = XElement.Parse(xml_str)
        except Exception as e:
            if model.deleted == 0:
                TraceService.Trace(TraceLevel.Error, "base_wechat.py Error: LINE {}".format(traceback.format_exc()))
        if xml is not None:
            location = model.create_location()
            loc = xml.Element('location')
            if loc.Attribute('x'):
                try:
                    location.latitude = float(loc.Attribute('x').Value)
                except Exception as e:
                    pass
            if loc.Attribute('y'):
                try:
                    location.longitude = float(loc.Attribute('y').Value)
                except Exception as e:
                    pass
            if loc.Attribute('poiname'):
                location.address = loc.Attribute('poiname').Value

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

    def _process_parse_message_system_xml(self, xml_str):
        content = ''
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
                content = template.replace('\"$username$\"', username).replace('\"$names$\"', names)
            elif xml.Element('editrevokecontent'):
                revoke = xml.Element('editrevokecontent')
                if revoke.Element('text'):
                    content += revoke.Element('text').Value
                if revoke.Element('link') and revoke.Element('link').Element('revokecontent'):
                    content += '[' + revoke.Element('link').Element('revokecontent').Value +']'
        if content not in [None, '']:
            return content
        else:
            return xml_str

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
        if msg_type in [MSG_TYPE_TEXT, MSG_TYPE_LINK_SEMI]:
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
        elif msg_type in [MSG_TYPE_SYSTEM, MSG_TYPE_SYSTEM_2, MSG_TYPE_SYSTEM_3]:
            return model_im.MESSAGE_CONTENT_TYPE_SYSTEM
        else:
            return model_im.MESSAGE_CONTENT_TYPE_LINK

    @staticmethod
    def _convert_fav_type(fav_type):
        if fav_type == FAV_TYPE_IMAGE:
            return model_im.FAVORITE_TYPE_IMAGE
        elif fav_type == FAV_TYPE_VOICE:
            return model_im.FAVORITE_TYPE_VOICE
        elif fav_type == FAV_TYPE_VIDEO:
            return model_im.FAVORITE_TYPE_VIDEO
        elif fav_type == FAV_TYPE_LINK:
            return model_im.FAVORITE_TYPE_LINK
        elif fav_type == FAV_TYPE_LOCATION:
            return model_im.FAVORITE_TYPE_LOCATION
        elif fav_type == FAV_TYPE_ATTACHMENT:
            return model_im.FAVORITE_TYPE_ATTACHMENT
        elif fav_type == FAV_TYPE_CHAT:
            return model_im.FAVORITE_TYPE_CHAT
        else:
            return model_im.FAVORITE_TYPE_TEXT

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
        m = hashlib.md5()
        m.update(src.encode('utf8'))
        return m.hexdigest()
