# coding=utf-8
__author__ = "sumeng"

from PA_runtime import *
import clr

clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference('model_wechat')
    clr.AddReference('bcp_wechat')
    clr.AddReference('base_wechat')
    clr.AddReference('tencent_struct')
    clr.AddReference('ResourcesExp')
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
from PA.InfraLib.Services import ServiceGetter, IWechatCrackUin

import os
import hashlib
import json
import base64
import sqlite3
import shutil
import model_wechat
import bcp_wechat
from base_wechat import *
import tencent_struct
import time
from ResourcesExp import AppResources
import gc

# EnterPoint: analyze_wechat(root, extract_deleted, extract_source):
# Patterns: '/MicroMsg/.+/EnMicroMsg.db$'

# app数据库版本
VERSION_APP_VALUE = 2

g_app_build = {}

# 第三方的分身工具
THIRDBUILDPARTTERN = {
    'LBE平行空间': r'/data/data/com.lbe.parallel/parallel/\d+/com.tencent.mm',
    '360分身大师': r'/data/data/com.qihoo.magic/Plugin/com.tencent.mm/data/com.tencent.mm',
    '双开助手多开分身': r'/data/data/com.excelliance.dualaid/gameplugins/com.tencent.mm',
    '多开助手': r'/data/data/com.kzshuankia.rewq/virtual/data/user/\d+/com.tencent.mm'
}

DEBUG = False


def analyze_wechat(root, extract_deleted, extract_source):
    nodes = root.FileSystem.Search('/MicroMsg/.+/EnMicroMsg.db$')
    if len(nodes) > 0:
        progress.Start()
        WeChatParser(process_nodes(nodes), extract_deleted, extract_source).process()
        progress.Finish(True)
    else:
        progress.Skip()

    pr = ParserResults()
    pr.Categories = DescripCategories.Wechat
    return pr


def process_nodes(nodes):
    ret = {}  # key: 节点名称  value: 节点对应的node数组
    app_dict = {}  # key: app路径  value: 节点名称
    app_tail = 1
    for node in nodes:
        try:
            user_node = node.Parent  # 获取{user_hash}
            try:
                app_node = user_node.Parent.Parent  # 获取app_path
            except Exception as e:
                app_node = None  # 没有包含app_path，可能是直接拷贝{user_hash}目录

            app_path = None
            app_info = None
            if app_node is not None and app_node.AbsolutePath != '/':  # 如果能获取到app_path，从ds里获取app路径对应的app信息
                app_path = app_node.AbsolutePath
                try:
                    app_info = ds.GetApplication(app_node.AbsolutePath)
                except Exception as e:
                    pass
            else:
                app_path = user_node.AbsolutePath

            if app_path in app_dict:
                build = app_dict.get(app_path, '微信')
            else:
                build = None
                if app_info and app_info.Name:
                    build = app_info.Name.Value  # app_info里获取app名称
                    if build in app_dict:  # app名称如果和app_dict里的冲突，使用微信+数字的模式命名节点
                        build = None
                if build in [None, '']:  # 没有获取到app名称，使用微信+数字的模式命名节点
                    build = get_build(app_path)
                    if build == '微信(第三方分身)':
                        if app_tail >= 2:
                            build = build[:-1] + str(app_tail) + ']'
                        app_tail += 1
                app_dict[app_path] = build

            value = ret.get(build, [])
            value.append(node)
            ret[build] = value
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
    return ret


def get_build(app_path):
    build = '微信'
    if not app_path:
        return build

    if app_path == r'/data/data/com.tencent.mm':
        return build

    if re.match(r'/data/user/\d+/com.tencent.mm', app_path) is not None:
        build = '微信(系统分身)'

    if build != '微信(系统分身)':
        build = '微信(第三方分身)'
        for k, v in THIRDBUILDPARTTERN.items():
            if re.match(v, app_path) is not None:
                build = '微信({name})'.format(name=k)
                break

    return build


def get_uin_from_cache(cache_path, user_hash):
    file_path = os.path.join(cache_path, 'cache_uin.json')
    if not os.path.exists(file_path):
        with open(file_path, 'w') as f:
            data = {}
            wxCrack = ServiceGetter.Get[IWechatCrackUin]()
            uin = wxCrack.CrackUinFromMd5(user_hash)
            data[user_hash] = uin
            json.dump(data, f)
            return uin
    else:
        with open(file_path, 'r') as f:
            data = json.load(f)
        if data.get(user_hash, None):
            return data[user_hash]
        else:
            with open(file_path, 'w') as f:
                wxCrack = ServiceGetter.Get[IWechatCrackUin]()
                uin = wxCrack.CrackUinFromMd5(user_hash)
                data[user_hash] = uin
                json.dump(data, f)
                return uin


def print_error():
    if DEBUG:
        TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))


class WeChatParser(Wechat):

    def __init__(self, node_dict, extract_deleted, extract_source):
        super(WeChatParser, self).__init__()
        self.node_dict = node_dict
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source

    def process(self):
        for build in self.node_dict:
            self.ar = AppResources(progress['APP', build]['MEDIA', '多媒体'])
            self.ar.set_thum_config("jpg", "Video")
            self.ar.set_thum_config("thumb", "Video")
            self.ar.set_thum_config("cover", "Video")
            self.ar.set_thum_config("extern", "Video")
            self.ar.set_thum_config("pic", "Video")

            nodes = self.node_dict.get(build, [])
            for node in nodes:
                self.parse_user_node(node, build)
                gc.collect()

            pr = ParserResults()
            pr.Categories = DescripCategories.Wechat
            pr.Models.AddRange(self.ar.parse())
            pr.Build(build)
            ds.Add(pr)
            
            self.ar = None

    def parse_user_node(self, node, build):
        self.user_node = node.Parent
        try:
            self.root = self.user_node.Parent.Parent
        except Exception as e:
            self.root = None
        self.build = build

        self.im = model_wechat.IM()
        self.models = []
        self.user_account = None
        self.user_account_model = None
        self.friend_models = {}
        self.chatroom_models = {}

        self.is_valid_user_dir = self._is_valid_user_dir()
        self.uin = self._get_uin()
        self.imei = self._get_imei(self.root.GetByPath('/MicroMsg/CompatibleInfo.cfg'))
        self.user_hash = self._get_user_hash()
        self.cache_path = os.path.join(ds.OpenCachePath('wechat'), self._get_user_guid())
        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)
        self.cache_db = os.path.join(self.cache_path, self.user_hash + '.db')
        save_cache_path(bcp_wechat.CONTACT_ACCOUNT_TYPE_IM_WECHAT, self.cache_db, ds.OpenCachePath("tmp"))

        self.chatroom_owners = dict()
        self.progress = None

        if not self.is_valid_user_dir:
            return []
        if not self._can_decrypt(self.uin, self.user_hash):
            uin = get_uin_from_cache(self.cache_path, self.user_hash)
            if uin not in [None, 0]:
                self.uin = uin
            else:
                return []

        if DEBUG or self.im.need_parse(self.cache_db, VERSION_APP_VALUE):
            self.im.db_create(self.cache_db)

            self.extend_nodes = []
            extend_nodes = self.root.FileSystem.Search('/Tencent/MicroMsg/{}$'.format(self.user_hash))
            for extend_node in extend_nodes:
                self.extend_nodes.append(extend_node)

            node = self.user_node.GetByPath('/EnMicroMsg.db')
            mm_db_path = os.path.join(self.cache_path, self.user_hash + '_mm.db')
            try:
                # print('%s android_wechat() decrypt EnMicroMsg.db' % time.asctime(time.localtime(time.time())))
                if Decryptor.decrypt(node, self._get_db_key(self.imei, self.uin), mm_db_path):
                    # print('%s android_wechat() parse MicroMsg.db' % time.asctime(time.localtime(time.time())))
                    self._parse_mm_db(mm_db_path, node.AbsolutePath)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
            self.set_progress(65)

            fav_node = self.user_node.GetByPath('/enFavorite.db')
            fav_db_path = os.path.join(self.cache_path, self.user_hash + '_fav.db')
            try:
                # print('%s android_wechat() decrypt enFavorite.db' % time.asctime(time.localtime(time.time())))
                if Decryptor.decrypt(fav_node, self._get_db_key(self.imei, self.uin), fav_db_path):
                    # print('%s android_wechat() parse enFavorite.db' % time.asctime(time.localtime(time.time())))
                    self._parse_fav_db(fav_db_path, fav_node.AbsolutePath)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
            self.set_progress(70)

            try:
                # print('%s android_wechat() parse SnsMicroMsg.db' % time.asctime(time.localtime(time.time())))
                self._parse_wc_db(self.user_node.GetByPath('/SnsMicroMsg.db'))
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
            self.set_progress(85)
            try:
                # print('%s android_wechat() parse FTS5IndexMicroMsg.db' % time.asctime(time.localtime(time.time())))
                self._parse_fts_db(self.user_node.GetByPath('/FTS5IndexMicroMsg.db'))
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
            try:
                self._parse_story_db(self.user_node.GetByPath('/StoryMicroMsg.db'))
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
            self.set_progress(99)
            self.im.db_create_index()
            # 数据库填充完毕，请将中间数据库版本和app数据库版本插入数据库，用来检测app是否需要重新解析
            if not canceller.IsCancellationRequested:
                self.im.db_insert_table_version(model_wechat.VERSION_KEY_DB, model_wechat.VERSION_VALUE_DB)
                self.im.db_insert_table_version(model_wechat.VERSION_KEY_APP, VERSION_APP_VALUE)
            self.im.db_commit()
            self.im.db_close()
            self.set_progress(100)
            self.progress.Finish(True)
            # print('%s android_wechat() parse end' % time.asctime(time.localtime(time.time())))
        else:
            self.extend_nodes = []
            extend_nodes = self.root.FileSystem.Search('/Tencent/MicroMsg/{}$'.format(self.user_hash))
            for extend_node in extend_nodes:
                self.extend_nodes.append(extend_node)

            model_wechat.GenerateModel(self.cache_db, self.build, self.ar).get_models()

        try:
            self.get_wechat_res(self.ar)
        except Exception as e:
            print(e)

        self.im = None
        self.build = None
        self.models = None
        self.user_account = None
        self.user_account_model = None
        self.friend_models = None
        self.chatroom_models = None
        self.user_node = None
        self.app_node = None
        self.user_hash = None
        self.private_user_node = None
        self.cache_path = None

    def _is_valid_user_dir(self):
        if self.root is None or self.user_node is None:
            return False
        if self.root.GetByPath('/shared_prefs/auth_info_key_prefs.xml') is None and self.root.GetByPath(
                '/shared_prefs/com.tencent.mm_preferences.xml'):
            return False
        return True

    @staticmethod
    def _is_blocked_usr(type_):
        is_blocked = False
        if (not type_) or (not isinstance(type_, int)):
            return
        try:
            type_ = hex(type_)
            if type_[-1] == 'b':
                is_blocked = True
        except Exception as e:
            print_error()
        finally:
            return is_blocked

    @staticmethod
    def _add_label(label_relation, contact_label_id, username):
        if not contact_label_id:
            return
        try:
            label_list = contact_label_id.split(",")
            for l in label_list:
                if label_relation.get(l, None):
                    label_relation[l].append(username)
                else:
                    label_relation[l] = [username]
        except Exception as e:
            print_error()

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

    def _get_user_guid(self):
        return self._md5(self.root.AbsolutePath)

    @staticmethod
    def _get_imei(node):
        imei = None
        if node is not None:
            try:
                node.Data.seek(0)
                content = node.read()
                pos = content.find(b'\x01\x02\x74\x00')
                if pos != -1:
                    size = ord(content[pos + 4:pos + 5])
                    imei = content[pos + 5:pos + 5 + size]
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
        return imei

    @staticmethod
    def _get_db_key(imei, uin):
        if imei is None or uin is None:
            return None
        return WeChatParser._md5(imei + uin)[:7]

    def _parse_mm_db(self, mm_db_path, source):
        db = None
        try:
            node = self.create_memory_node(self.user_node, mm_db_path, os.path.basename(mm_db_path))
            db = SQLiteParser.Database.FromNode(node, canceller)
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
            return

        self._parse_mm_db_user_info(db, source)
        self.progress = progress['APP', self.build]['ACCOUNT', self.user_account_model.Account, self.user_account_model]
        self.progress.Start()
        self.set_progress(15)
        self._parse_mm_db_chatroom_member(db, source)
        self._parse_mm_db_contact(db, source)
        self.set_progress(25)
        self.get_chatroom_models(self.cache_db)
        self.set_progress(30)
        self._parse_mm_db_message(db, source)
        self._parse_mm_db_bank_cards(db, source)
        self._parse_mm_db_login_devices(db, source)
        self.push_models()

    def _parse_fav_db(self, fav_db_path, source):
        db = None
        try:
            node = self.create_memory_node(self.user_node, fav_db_path, os.path.basename(fav_db_path))
            db = SQLiteParser.Database.FromNode(node, canceller)
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
            return False
        if not db:
            return False

        if 'FavItemInfo' in db.Tables:
            ts = SQLiteParser.TableSignature('FavItemInfo')
            SQLiteParser.Tools.AddSignatureToTable(ts, "xml", SQLiteParser.FieldType.Text,
                                                   SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted, False, ''):
                if canceller.IsCancellationRequested:
                    break
                if rec is None:
                    continue
                try:
                    local_id = self._db_record_get_int_value(rec, 'localId')
                    fav_type = self._db_record_get_int_value(rec, 'type')
                    timestamp = self._db_record_get_int_value(rec, 'updateTime')
                    from_user = self._db_record_get_string_value(rec, 'fromUser')
                    to_user = self._db_record_get_string_value(rec, 'toUser')
                    real_name = self._db_record_get_string_value(rec, 'realChatName')
                    source_type = self._db_record_get_int_value(rec, 'sourceType')
                    xml = self._db_record_get_string_value(rec, 'xml')
                    deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    self._parse_fav_db_with_value(deleted, node.AbsolutePath, fav_type, timestamp, from_user, xml)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error,
                                       "apple_wechat.py Error: LINE {}".format(traceback.format_exc()))
            self.im.db_commit()
            self.push_models()
        return True

    def _parse_fav_db_with_value(self, deleted, source, fav_type, timestamp, from_user, xml):
        favorite = model_wechat.Favorite()
        favorite.source = source
        favorite.deleted = deleted
        favorite.account_id = self.user_account_model.Account
        favorite.type = fav_type
        favorite.talker_id = from_user
        if from_user.endswith('@chatroom'):
            favorite.talker_type = model_wechat.CHAT_TYPE_GROUP
        else:
            favorite.talker_type = model_wechat.CHAT_TYPE_FRIEND
        favorite.timestamp = timestamp
        self._parse_user_fav_xml(xml, favorite)
        favorite.insert_db(self.im)
        model = self.get_favorite_model(favorite)
        self.add_model(model)

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
            if fav_type == model_wechat.FAV_TYPE_TEXT:
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
                    elif source_info.Element('fromusr'):
                        fav_item.sender = source_info.Element('fromusr').Value
                if xml.Element('desc'):
                    fav_item.content = xml.Element('desc').Value
            elif fav_type in [model_wechat.FAV_TYPE_IMAGE, model_wechat.FAV_TYPE_VOICE, model_wechat.FAV_TYPE_VIDEO,
                              model_wechat.FAV_TYPE_VIDEO_2, model_wechat.FAV_TYPE_ATTACHMENT]:

                if xml.Element('datalist') and xml.Element('datalist').Element('dataitem'):
                    items = xml.Element('datalist').Elements('dataitem')
                    for item in items:
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
                            elif source_info.Element('fromusr'):
                                fav_item.sender = source_info.Element('fromusr').Value
                        if xml.Element('title'):
                            fav_item.content = xml.Element('title').Value
                        data_id = item.Attribute('dataid').Value
                        node = self._search_fav_file(data_id)
                        if node is not None:
                            fav_item.media_path = node.AbsolutePath
            elif fav_type == model_wechat.FAV_TYPE_LINK:
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
                    elif source_info.Element('fromusr'):
                        fav_item.sender = source_info.Element('fromusr').Value
                    if source_info.Element('link'):
                        fav_item.link_url = source_info.Element('link').Value
                if xml.Element('weburlitem'):
                    weburlitem = xml.Element('weburlitem')
                    if weburlitem.Element('pagetitle'):
                        fav_item.link_title = weburlitem.Element('pagetitle').Value
                    if weburlitem.Element('pagethumb_url'):
                        fav_item.link_image = weburlitem.Element('pagethumb_url').Value
            elif fav_type == model_wechat.FAV_TYPE_LOCATION:
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
                    elif source_info.Element('fromusr'):
                        fav_item.sender = source_info.Element('fromusr').Value
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
                        fav_item.location_latitude = latitude
                        fav_item.location_longitude = longitude
                        if locitem.Element('label'):
                            fav_item.location_address = locitem.Element('label').Value
                        if locitem.Element('poiname'):
                            fav_item.location_address = locitem.Element('poiname').Value
                        fav_item.location_type = model_wechat.LOCATION_TYPE_GOOGLE
            elif fav_type == model_wechat.FAV_TYPE_CHAT:
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
                            elif source_info.Element('fromusr'):
                                fav_item.sender = source_info.Element('fromusr').Value
                        if fav_item.type == model_wechat.FAV_TYPE_TEXT:
                            if item.Element('datadesc'):
                                fav_item.content = item.Element('datadesc').Value
                        elif fav_item.type in [model_wechat.FAV_TYPE_IMAGE, model_wechat.FAV_TYPE_VOICE,
                                               model_wechat.FAV_TYPE_VIDEO, model_wechat.FAV_TYPE_VIDEO_2,
                                               model_wechat.FAV_TYPE_ATTACHMENT]:
                            if item.Element('sourcedatapath'):
                                fav_item.media_path = self._parse_user_fav_path(item.Element('sourcedatapath').Value)
                            elif item.Element('sourcethumbpath'):
                                fav_item.media_path = self._parse_user_fav_path(item.Element('sourcedatapath').Value)
                        elif fav_item.type == model_wechat.FAV_TYPE_LINK:
                            if item.Element('dataitemsource'):
                                source_info = item.Element('dataitemsource')
                                if source_info.Element('link'):
                                    fav_item.link_url = source_info.Element('link').Value
                            if item.Element('weburlitem') and item.Element('weburlitem').Element('pagetitle'):
                                fav_item.link_title = item.Element('weburlitem').Element('pagetitle').Value
                            if item.Element('sourcethumbpath'):
                                fav_item.link_image = self._parse_user_fav_path(item.Element('sourcethumbpath').Value)
                        elif fav_item.type == model_wechat.FAV_TYPE_LOCATION:
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
                                    fav_item.location_type = model_wechat.LOCATION_TYPE_GOOGLE
                                    fav_item.location_latitude = latitude
                                    fav_item.location_longitude = longitude
                                    if locitem.Element('label'):
                                        fav_item.location_address = locitem.Element('label').Value
                                    if locitem.Element('poiname'):
                                        fav_item.location_address = locitem.Element('poiname').Value
                        else:
                            fav_item.content = xml_str
                        if item.Element('datasrcname'):
                            fav_item.sender_name = item.Element('datasrcname').Value
            else:
                fav_item = model.create_item()
                fav_item.type = fav_type
                fav_item.content = xml_str
        return True

    def _parse_wc_db(self, node):
        if node is None:
            return False
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
            SQLiteParser.Tools.AddSignatureToTable(ts, "userName", SQLiteParser.FieldType.Text,
                                                   SQLiteParser.FieldConstraints.NotNull)
            SQLiteParser.Tools.AddSignatureToTable(ts, "content", SQLiteParser.FieldType.Blob,
                                                   SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted, False, ''):
                if canceller.IsCancellationRequested:
                    break
                try:
                    feed_id = self._db_record_get_int_value(rec, 'snsId')
                    username = self._db_record_get_string_value(rec, 'userName')
                    content = self._db_record_get_blob_value(rec, 'content')
                    attr = self._db_record_get_blob_value(rec, 'attrBuf')
                    deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    self._parse_wc_db_with_value(deleted, node.AbsolutePath, username, content, attr)
                except Exception as e:
                    print_error()
            self.im.db_commit()
            self.push_models()

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

        feed = model_wechat.Feed()
        feed.deleted = deleted
        feed.source = source
        feed.account_id = self.user_account_model.Account
        feed.sender_id = username
        feed.content = sns.get_content_text()
        feed.timestamp = sns.get_timestamp()

        if moment_type == MOMENT_TYPE_IMAGE:
            medias = sns.get_content_medias()
            feed.image_path = ','.join(str(m) for m in medias)
        elif moment_type == MOMENT_TYPE_VIDEO:
            medias = sns.get_content_medias()
            if medias is not None:
                feed.video_path = ','.join(str(m) for m in medias)
        elif moment_type in [MOMENT_TYPE_SHARED, MOMENT_TYPE_MUSIC]:
            feed.link_url, feed.link_title, feed.link_content = sns.get_url_info()

        sns.get_location(feed)
        sns.get_likes(feed)
        sns.get_comments(feed)
        feed.insert_db(self.im)
        model, tl_model = self.get_feed_model(feed)
        self.add_model(model)
        self.add_model(tl_model)

    def _parse_story_db(self, node):
        if node is None:
            return False

        db_path = os.path.join(self.cache_path, 'cache.db')
        self.db_mapping(node.PathWithMountPoint, db_path)
        if not os.path.exists(db_path):
            return False
        db = SQLite.SQLiteConnection('Data Source = {}'.format(db_path))
        if db is None:
            return False
        db.Open()

        sql = '''select MMStoryInfo.storyID, 
                        MMStoryInfo.userName, 
                        MMStoryInfo.createTime, 
                        MMStoryInfo.commentListCount, 
                        MMStoryInfo.content, 
                        MMStoryInfo.attrBuf, 
                        MMStoryInfo.postBuf, 
                        StoryExtItem.newThumbUrl, 
                        StoryExtItem.newVideoUrl, 
                        StoryVideoCacheInfo.filePath 
                    from MMStoryInfo 
                    left join StoryExtItem on MMStoryInfo.storyID = StoryExtItem.syncId 
                    left join StoryVideoCacheInfo on MMStoryInfo.storyId = StoryVideoCacheInfo.storyId;'''
        db_cmd = SQLite.SQLiteCommand(sql, db)
        reader = None
        try:
            reader = db_cmd.ExecuteReader()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))

        if reader is not None:
            while reader.Read():
                try:
                    story = model_wechat.Story()
                    story.account_id = self.user_account_model.Account
                    story.sender_id = self._db_reader_get_string_value(reader, 1)
                    local_path = self._db_reader_get_string_value(reader, 9)
                    url = self._db_reader_get_string_value(reader, 8)
                    story.media_path = local_path if local_path is not None else url
                    story.timestamp = self._db_reader_get_int_value(reader, 2)
                    for comment in self._process_parse_story_comment(self._db_reader_get_blob_value(reader, 5)):
                        story_comment = story.create_comment()
                        story_comment.content = str(comment.content)
                        story_comment.sender_id = comment.sender_id
                        story_comment.timestamp = comment.timestamp
                    story.insert_db(self.im)
                    story_model, story_timeline_model = self.get_story_model(story)
                    self.add_model(story_model)
                    self.add_model(story_timeline_model)
                except Exception as e:
                    print_error()
            reader.Close()
        db_cmd.Dispose()
        self.im.db_commit()
        db.Close()
        self.db_remove_mapping(db_path)

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

        # self._parse_fts_contacts(db, node.AbsolutePath)
        # self._parse_fts_chatroom_member(db, node.AbsolutePath)
        self._parse_fts_message(db, node.AbsolutePath)

        db.Close()
        self.db_remove_mapping(db_path)

    def _parse_fts_contacts(self, db, source):
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
            if canceller.IsCancellationRequested:
                break
            try:
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
                self._parse_mm_db_contact_with_value(1, source, username, alias, nickname, '', 0, 0, None, None)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
        self.im.db_commit()
        del contacts

    def _parse_fts_chatroom_member(self, db, source):
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
                try:
                    chatroom = self._db_reader_get_string_value(reader, 0)
                    member = self._db_reader_get_string_value(reader, 1)
                    if chatroom != '' and member != '':
                        self._parse_mm_db_chatroom_member_with_value(1, source, chatroom, member, '', None)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error,
                                       "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
            reader.Close()
        db_cmd.Dispose()
        self.im.db_commit()

    def _parse_fts_message(self, db, source):
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
                try:
                    msg = self._db_reader_get_string_value(reader, 0)
                    talker_id = self._db_reader_get_string_value(reader, 1)
                    sender_id = self._db_reader_get_string_value(reader, 2)
                    timestamp = self._db_reader_get_int_value(reader, 3) / 1000
                    if talker_id != '':
                        message = model_wechat.Message()
                        message.deleted = 1
                        message.source = source
                        message.account_id = self.user_account_model.Account
                        message.talker_id = talker_id
                        message.sender_id = sender_id
                        message.type = model_wechat.MESSAGE_CONTENT_TYPE_TEXT
                        message.timestamp = timestamp
                        message.content = msg
                        message.talker_type = model_wechat.CHAT_TYPE_GROUP if talker_id.endswith(
                            "@chatroom") else model_wechat.CHAT_TYPE_FRIEND
                        message.insert_db(self.im)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error,
                                       "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
            reader.Close()
        db_cmd.Dispose()
        self.im.db_commit()

    def _parse_mm_db_user_info(self, db, source):
        user_account = model_wechat.Account()
        user_account.source = source
        user_account.account_id = self.user_hash

        if 'userinfo' in db.Tables:
            ts = SQLiteParser.TableSignature('userinfo')
            for rec in db.ReadTableRecords(ts, False, False, ''):
                if canceller.IsCancellationRequested:
                    break
                if rec is None:
                    continue
                try:
                    id = self._db_record_get_int_value(rec, 'id')
                    value = self._db_record_get_string_value(rec, 'value')
                    if id == 2:
                        user_account.account_id = value
                    elif id == 4:
                        user_account.nickname = value
                    elif id == 5:
                        user_account.email = value
                    elif id == 6:
                        user_account.telephone = value
                    elif id == 42:
                        user_account.account_id_alias = value
                    elif id == 12291:
                        user_account.signature = value
                    elif id == 12293:
                        user_account.address = value
                    elif id == 352277:
                        self._parse_mm_db_emergency(user_account.account_id, value, source)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error,
                                       "android_wechat.py Error: LINE {}".format(traceback.format_exc()))

        if 'userinfo2' in db.Tables:
            ts = SQLiteParser.TableSignature('userinfo2')
            for rec in db.ReadTableRecords(ts, False, False, ''):
                if canceller.IsCancellationRequested:
                    break
                if rec is None:
                    continue
                try:
                    sid = self._db_record_get_string_value(rec, 'sid')
                    value = self._db_record_get_string_value(rec, 'value')
                    if sid == 'USERINFO_LAST_LOGIN_USERNAME_STRING':
                        user_account.username = value
                    elif sid == 'USERINFO_SELFINFO_SMALLIMGURL_STRING':
                        user_account.photo = value
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error,
                                       "android_wechat.py Error: LINE {}".format(traceback.format_exc()))

        user_account.insert_db(self.im)
        self.im.db_commit()
        self.user_account_model = self.get_account_model(user_account)
        self.add_model(self.user_account_model)
        # add self to friend
        if self.user_account_model is not None:
            model = WeChat.Friend()
            model.SourceFile = self.user_account_model.SourceFile
            model.Deleted = self.user_account_model.Deleted
            model.AppUserAccount = self.user_account_model
            model.Account = self.user_account_model.Account
            model.NickName = self.user_account_model.NickName
            model.HeadPortraitPath = self.user_account_model.HeadPortraitPath
            model.Gender = self.user_account_model.Gender
            model.Signature = self.user_account_model.Signature
            model.Type = WeChat.FriendType.Friend
            self.friend_models[self.user_account_model.Account] = model
            self.add_model(model)
        self.push_models()

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

    def _parse_mm_db_contact(self, db, source):
        heads = {}
        tag_relation = {}
        if 'img_flag' in db.Tables:
            if canceller.IsCancellationRequested:
                return
            ts = SQLiteParser.TableSignature('img_flag')
            SQLiteParser.Tools.AddSignatureToTable(ts, "username", SQLiteParser.FieldType.Text,
                                                   SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted, False, ''):
                if canceller.IsCancellationRequested:
                    break
                try:
                    username = self._db_record_get_string_value(rec, 'username')
                    if username in [None, '']:
                        continue

                    portrait_hd = self._db_record_get_string_value(rec, 'reserved1')
                    portrait = self._db_record_get_string_value(rec, 'reserved2')
                    deleted = 0 if rec.Deleted == DeletedState.Intact else 1

                    if deleted == 0 or username not in heads:
                        head = portrait_hd
                        if head in [None, '']:
                            head = portrait
                        heads[username] = head
                except Exception as e:
                    pass

        if 'rcontact' in db.Tables:
            if canceller.IsCancellationRequested:
                return
            ts = SQLiteParser.TableSignature('rcontact')
            SQLiteParser.Tools.AddSignatureToTable(ts, "username", SQLiteParser.FieldType.Text,
                                                   SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted, False, ''):
                if canceller.IsCancellationRequested:
                    break
                try:
                    username = self._db_record_get_string_value(rec, 'username')
                    if username in [None, '']:
                        continue
                    alias = self._db_record_get_string_value(rec, 'alias')
                    nickname = self._db_record_get_string_value(rec, 'nickname')
                    remark = self._db_record_get_string_value(rec, 'conRemark')
                    contact_type = self._db_record_get_int_value(rec, 'type')
                    verify_flag = self._db_record_get_int_value(rec, 'verifyFlag')
                    lvbuff = self._db_record_get_blob_value_to_ba(rec, 'lvbuff')
                    signature, region, gender = Decryptor.parse_lvbuff(lvbuff)
                    deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    contact_label_id = self._db_record_get_string_value(rec, 'contactLabelIds')
                    if contact_label_id and (deleted == 0):
                        self._add_label(tag_relation, contact_label_id, username)
                    self._parse_mm_db_contact_with_value(deleted, source, username, alias, nickname, remark,
                                                         contact_type, verify_flag, heads.get(username),
                                                         signature, region, gender)
                except Exception as e:
                    pass

            self._parse_mm_db_group(tag_relation, db, source)

            self.im.db_commit()
            self.push_models()

    def _parse_mm_db_contact_with_value(self, deleted, source, username, alias, nickname, remark,
                                        contact_type, verify_flag, head, signature, region, gender):
        if username.endswith("@chatroom"):
            chatroom = model_wechat.Chatroom()
            chatroom.deleted = deleted
            chatroom.source = source
            chatroom.sp_id = 0
            chatroom.account_id = self.user_account_model.Account
            chatroom.chatroom_id = username
            chatroom.owner_id = self.chatroom_owners.get(chatroom.chatroom_id, None)
            chatroom.name = nickname
            chatroom.photo = head
            chatroom.is_saved = contact_type % 2
            chatroom.insert_db(self.im)
        else:
            # TODO 这块应该能合并
            friend_type = model_wechat.FRIEND_TYPE_NONE
            if verify_flag != 0:
                friend_type = model_wechat.FRIEND_TYPE_OFFICIAL
            elif contact_type % 2 == 1:
                if self._parse_user_type_is_blocked(contact_type):
                    friend_type = model_wechat.FRIEND_TYPE_BLOCKED
                else:
                    friend_type = model_wechat.FRIEND_TYPE_FRIEND
            elif contact_type == 0:
                friend_type = model_wechat.FRIEND_TYPE_PROGRAM
            friend = model_wechat.Friend()
            friend.deleted = deleted
            friend.source = source
            friend.account_id = self.user_account_model.Account
            friend.friend_id_alias = alias
            friend.friend_id = username
            friend.type = friend_type
            friend.nickname = nickname
            friend.remark = remark
            friend.photo = head
            friend.signature = signature
            friend.region = region
            friend.gender = gender
            friend.insert_db(self.im)
            model = self.get_friend_model(friend)
            self.add_model(model)
            if (deleted == 0 or username not in self.friend_models) and username != self.user_account_model.Account:
                self.friend_models[username] = model

    def _parse_mm_db_chatroom_member(self, db, source):
        if 'chatroom' in db.Tables:
            ts = SQLiteParser.TableSignature('chatroom')
            SQLiteParser.Tools.AddSignatureToTable(ts, "chatroomname", SQLiteParser.FieldType.Text,
                                                   SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted, False, ''):
                if canceller.IsCancellationRequested:
                    break
                try:
                    chatroom_id = self._db_record_get_string_value(rec, 'chatroomname')
                    if chatroom_id in [None, '']:
                        continue
                    member_list = self._db_record_get_string_value(rec, 'memberlist')
                    display_name_list = self._db_record_get_string_value(rec, 'displayname')
                    room_owner = self._db_record_get_string_value(rec, 'roomowner')
                    # 通过这一步挂载各个room群主的关系
                    self.chatroom_owners[chatroom_id] = room_owner
                    deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    self._parse_mm_db_chatroom_member_with_value(deleted, source, chatroom_id, member_list,
                                                                 display_name_list, room_owner)
                except Exception as e:
                    pass
            self.im.db_commit()

    def _parse_mm_db_chatroom_member_with_value(self, deleted, source, chatroom_id, member_list, display_name_list,
                                                room_owner):
        room_members = member_list.split(';')
        display_names = display_name_list.split('、')

        cm = model_wechat.ChatroomMember()
        cm.deleted = deleted
        cm.source = source
        cm.account_id = self.user_account_model.Account
        cm.chatroom_id = chatroom_id
        for i, room_member in enumerate(room_members):
            if canceller.IsCancellationRequested:
                break
            cm.member_id = room_member
            if i < len(display_names) and display_names[i] != room_member:
                cm.display_name = display_names[i]
            else:
                cm.display_name = None
            cm.insert_db(self.im)

    def _parse_mm_db_message(self, db, source):
        if 'message' in db.Tables:
            if canceller.IsCancellationRequested:
                return
            ts = SQLiteParser.TableSignature('message')
            SQLiteParser.Tools.AddSignatureToTable(ts, "talker", SQLiteParser.FieldType.Text,
                                                   SQLiteParser.FieldConstraints.NotNull)
            SQLiteParser.Tools.AddSignatureToTable(ts, "content", SQLiteParser.FieldType.Text,
                                                   SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted, False, ''):
                if canceller.IsCancellationRequested:
                    break
                try:
                    talker_id = self._db_record_get_string_value(rec, 'talker')
                    if talker_id in [None, '']:
                        continue
                    msg = self._db_record_get_string_value(rec, 'content')
                    img_path = self._db_record_get_string_value(rec, 'imgPath')
                    is_send = self._db_record_get_int_value(rec, 'isSend')
                    status = self._db_record_get_int_value(rec, 'status')
                    msg_type = self._db_record_get_int_value(rec, 'type')
                    timestamp = self._db_record_get_int_value(rec, 'createTime') / 1000
                    msg_id = self._db_record_get_string_value(rec, 'msgId')
                    lv_buffer = self._db_record_get_blob_value(rec, 'lvbuffer')
                    msg_svr_id = self._db_record_get_string_value(rec, 'msgSvrId')
                    deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    self._parse_mm_db_message_with_value(deleted, source, talker_id, msg, img_path, is_send, status,
                                                         msg_type, timestamp, msg_id, lv_buffer, msg_svr_id)
                except Exception as e:
                    pass
            self.im.db_commit()
            self.push_models()

    def _parse_mm_db_message_with_value(self, deleted, source, talker_id, msg, img_path, is_send, status, msg_type,
                                        timestamp, msg_id, lv_buffer, msg_svr_id):
        revoke_content = None
        message = model_wechat.Message()
        message.deleted = deleted
        message.source = source
        message.account_id = self.user_account_model.Account
        message.talker_id = talker_id
        message.msg_id = msg_svr_id
        message.type = self._convert_msg_type(msg_type)
        message.timestamp = timestamp
        if talker_id.endswith("@chatroom"):
            message.talker_type = model_wechat.CHAT_TYPE_GROUP
            revoke_content = self._process_parse_group_message(msg, msg_type, img_path, is_send != 0, message)
        else:
            message.talker_type = model_wechat.CHAT_TYPE_FRIEND
            message.sender_id = self.user_account_model.Account if is_send != 0 else talker_id
            revoke_content = self._process_parse_friend_message(msg, msg_type, img_path, message)
        message.insert_db(self.im)
        model, tl_model = self.get_message_model(message)
        self.add_model(model)
        self.add_model(tl_model)

        if revoke_content is not None:
            revoke_message = model_wechat.Message()
            revoke_message.IsRecall = True
            revoke_message.deleted = message.deleted
            revoke_message.source = message.source
            revoke_message.account_id = message.account_id
            revoke_message.talker_id = message.talker_id
            revoke_message.talker_type = message.talker_type
            revoke_message.msg_id = message.msg_id
            revoke_message.timestamp = message.timestamp
            revoke_message.type = model_wechat.MESSAGE_CONTENT_TYPE_TEXT
            revoke_message.sender_id = self.user_account_model.Account
            revoke_message.content = revoke_content
            revoke_message.insert_db(self.im)
            model, tl_model = self.get_message_model(revoke_message)
            self.add_model(model)
            self.add_model(tl_model)

    def _parse_mm_db_emergency(self, account_id, emergency, source):
        if (not emergency) or (emergency == ''):
            return
        try:
            contact_label = model_wechat.ContactLabel()
            contact_label.account_id = account_id
            contact_label.source = source
            contact_label.name = '紧急联系人'
            contact_label.type = model_wechat.CONTACT_LABEL_TYPE_EMERGENCY
            contact_label.users = emergency
            contact_label.insert_db(self.im)
            contact_label_model = self.get_contact_label_model(contact_label)
            if contact_label_model is not None:
                self.add_model(contact_label_model)
        except Exception as e:
            print_error()
        finally:
            return True

    def _parse_mm_db_group(self, tag, db, source):
        if 'ContactLabel' in db.Tables:
            if canceller.IsCancellationRequested:
                return
            ts = SQLiteParser.TableSignature('ContactLabel')
            SQLiteParser.Tools.AddSignatureToTable(ts, "labelName", SQLiteParser.FieldType.Text,
                                                   SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted, False, ''):
                if canceller.IsCancellationRequested:
                    break
                try:
                    contact_label = model_wechat.ContactLabel()
                    contact_label.account_id = self.user_account_model.Account
                    contact_label.source = source
                    contact_label.name = self._db_record_get_string_value(rec, 'labelName')
                    contact_label.id = self._db_record_get_int_value(rec, 'labelID')
                    contact_label.type = model_wechat.CONTACT_LABEL_TYPE_GROUP
                    contact_label.users = ",".join(tag.get(str(contact_label.id), []))
                    contact_label.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    contact_label.insert_db(self.im)
                    contact_label_model = self.get_contact_label_model(contact_label)
                    if contact_label_model is not None:
                        self.add_model(contact_label_model)
                except Exception as e:
                    print_error()

    def _parse_mm_db_bank_cards(self, db, source):
        if 'WalletBankcard' in db.Tables:
            if canceller.IsCancellationRequested:
                return
            ts = SQLiteParser.TableSignature('WalletBankcard')
            SQLiteParser.Tools.AddSignatureToTable(ts, "bankName", SQLiteParser.FieldType.Text,
                                                   SQLiteParser.FieldConstraints.NotNull)
            SQLiteParser.Tools.AddSignatureToTable(ts, "bankcardTail", SQLiteParser.FieldType.Text,
                                                   SQLiteParser.FieldConstraints.NotNull)
            SQLiteParser.Tools.AddSignatureToTable(ts, "mobile", SQLiteParser.FieldType.Text,
                                                   SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted, False, ''):
                if canceller.IsCancellationRequested:
                    break
                try:
                    bank_card = model_wechat.BankCard()
                    bank_card.account_id = self.user_account_model.Account
                    bank_card.source = source
                    bank_card.bank_name = self._db_record_get_string_value(rec, 'bankName')
                    bank_card.phone_number = self._db_record_get_string_value(rec, 'mobile')
                    bank_card.card_number = self._db_record_get_string_value(rec, 'bankcardTail')
                    bank_card.card_type = self._db_record_get_string_value(rec, 'bankcardTypeName')
                    bank_card.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    bank_card.insert_db(self.im)
                    bank_card_model = self.get_contact_label_model(bank_card)
                    if bank_card_model is not None:
                        self.add_model(bank_card_model)
                except Exception as e:
                    print_error()

    def _parse_mm_db_login_devices(self, db, source):
        if 'SafeDeviceInfo' in db.Tables:
            if canceller.IsCancellationRequested:
                return
            ts = SQLiteParser.TableSignature('SafeDeviceInfo')
            SQLiteParser.Tools.AddSignatureToTable(ts, "name", SQLiteParser.FieldType.Text,
                                                   SQLiteParser.FieldConstraints.NotNull)
            SQLiteParser.Tools.AddSignatureToTable(ts, "createtime", SQLiteParser.FieldType.Int,
                                                   SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted, False, ''):
                if canceller.IsCancellationRequested:
                    break
                try:
                    device = model_wechat.LoginDevice()
                    device.source = source
                    device.name = self._db_record_get_string_value(rec, 'name')
                    device.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    device.account_id = self.user_account_model.Account
                    device.type = self._db_record_get_string_value(rec, 'devicetype')
                    device.last_time = self._db_record_get_int_value(rec, 'createtime')
                    device.insert_db(self.im)
                    device_model = self.get_login_device_model(device)
                    if device_model is not None:
                        self.add_model(device_model)
                except Exception as e:
                    print_error()

    def _process_parse_friend_message(self, msg, msg_type, img_path, model):
        content = msg
        revoke_content = None

        if msg_type == MSG_TYPE_TEXT:
            pass
        elif msg_type == MSG_TYPE_EMOJI:
            content = '[Emoji]'
            model.media_path = self._process_parse_message_emoji_img_path(msg, img_path)
        elif msg_type == MSG_TYPE_IMAGE:
            content = '[图片]'
            model.media_path = self._process_parse_message_tranlate_img_path(img_path)
        elif msg_type == MSG_TYPE_VOICE:
            content = '[语音]'
            model.media_path = self._process_parse_message_tranlate_voice_path(img_path)
        elif msg_type in [MSG_TYPE_VIDEO, MSG_TYPE_VIDEO_2]:
            content = '[视频]'
            model.media_path, model.media_thum_path = self._process_parse_message_tranlate_video_path(img_path, model)
        elif msg_type == MSG_TYPE_LOCATION:
            content = self._process_parse_message_location(content, model)
        elif msg_type == MSG_TYPE_CONTACT_CARD:
            content = self._process_parse_message_contact_card(content, model)
        elif msg_type == MSG_TYPE_VOIP:
            content = self._process_parse_message_voip(content)
        elif msg_type == MSG_TYPE_VOIP_GROUP:
            content = self._process_parse_message_voip_group(content)
        elif msg_type == MSG_TYPE_SYSTEM:
            pass
        elif msg_type in [MSG_TYPE_SYSTEM_2, MSG_TYPE_SYSTEM_3]:
            content, revoke_content = self._process_parse_message_system_xml(content)
        elif msg_type == MSG_TYPE_LINK_SEMI:
            pass
        elif msg_type == MSG_TYPE_APP_MESSAGE:
            content = self._process_parse_message_app_message(content)
        else:  # MSG_TYPE_LINK
            self._process_parse_message_link(content, model)

        model.content = content
        return revoke_content

    def _process_parse_group_message(self, msg, msg_type, img_path, is_sender, model):
        sender_id = self.user_account_model.Account
        content = msg

        if not is_sender:
            seps = [':\n', '*#*\n']
            for sep in seps:
                index = msg.find(sep)
                if index != -1:
                    sender_id = msg[:index]
                    content = msg[index + len(sep):]
                    break

        model.sender_id = sender_id
        return self._process_parse_friend_message(content, msg_type, img_path, model)

    def _process_parse_message_emoji_img_path(self, msg_content, img_path):
        media_path = None
        emoji_dir = None
        if not img_path:
            return media_path

        msg_content_pattern = r'<msg>[\s\S]*/msg>'
        res = re.findall(msg_content_pattern, msg_content)
        if res:
            try:
                xml = XElement.Parse(res[0])
                emoji_dir = xml.Element('emoji').Attribute('productid').Value
            except Exception as e:
                print(e)
        for extend_node in self.extend_nodes:
            if emoji_dir:
                path = '/emoji/{}/{}'.format(emoji_dir, img_path)
            else:
                path = '/emoji/{}'.format(img_path)
            node = extend_node.GetByPath(path)
            if node is not None:
                media_path = node.AbsolutePath
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
                m1 = img_name[len(TH_PREFIX):len(TH_PREFIX) + 2]
                m2 = img_name[len(TH_PREFIX) + 2:len(TH_PREFIX) + 4]
            else:
                m1 = img_name[0:2]
                m2 = img_name[2:4]
            for extend_node in self.extend_nodes:
                if canceller.IsCancellationRequested:
                    break
                node = extend_node.GetByPath('/image2/{0}/{1}/{2}'.format(m1, m2, img_name))
                if node is not None:
                    media_path = node.AbsolutePath

                    p_node = extend_node.GetByPath('/image2/{0}/{1}'.format(m1, m2))
                    if img_name.startswith(TH_PREFIX):
                        hd_file = img_name[len(TH_PREFIX):]
                    else:
                        hd_file = img_name
                    hd_nodes = p_node.Search('/{}[.].+$'.format(hd_file))
                    if hd_nodes is not None:
                        for hd_node in hd_nodes:
                            media_path = hd_node.AbsolutePath
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

    def _process_parse_message_tranlate_video_path(self, video_id, model):
        media_path = None
        media_thumbnail_path = None
        for extend_node in self.extend_nodes:
            if canceller.IsCancellationRequested:
                break
            node = extend_node.GetByPath('/video/{}.mp4'.format(video_id))
            if node is not None:
                media_path = node.AbsolutePath

            node = extend_node.GetByPath('/video/{}.jpg'.format(video_id))
            if node is not None:
                media_thumbnail_path = node.AbsolutePath

            if any((media_thumbnail_path, media_path)):
                break

        return media_path, media_thumbnail_path

    def _process_parse_message_link(self, xml_str, model):
        xml = None
        try:
            xml_content = xml_str.replace('\b', '')  # remove '\b' char
            index = xml_content.find('<?xml version="1.0"?>')
            if index > 0:
                xml_content = xml_content.replace('<?xml version="1.0"?>', '')  # remove xml declare not in front of content
            xml = XElement.Parse(xml_content)
        except Exception as e:
            if model.deleted == 0:
                TraceService.Trace(TraceLevel.Error, "base_wechat.py Error: LINE {} \nxml: {}".format(traceback.format_exc(), xml_str))
            model.type = model_wechat.MESSAGE_CONTENT_TYPE_TEXT
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
                        msg_title = appmsg.Element('title').Value if appmsg.Element('title') else ''
                        mmreader = appmsg.Element('mmreader')
                        if mmreader:
                            category = mmreader.Element('category')
                            if category and category.Element('item'):
                                item = category.Element('item')
                                if item.Element('title'):
                                    model.link_title = item.Element('title').Value
                                if item.Element('digest'):
                                    model.link_content = item.Element('digest').Value
                                if item.Element('url'):
                                    model.link_url = item.Element('url').Value
                        else:
                            if appmsg.Element('title'):
                                model.link_title = appmsg.Element('title').Value
                            if appmsg.Element('des'):
                                model.link_content = appmsg.Element('des').Value
                            if appmsg.Element('url'):
                                model.link_url = appmsg.Element('url').Value
                            appinfo = xml.Element('appinfo')
                            if appinfo and appinfo.Element('appname'):
                                model.link_from = appinfo.Element('appname').Value
                else:
                    pass
            elif xml.Name.LocalName == 'mmreader':
                category = xml.Element('category')
                if category and category.Element('item'):
                    item = category.Element('item')
                    if item.Element('title'):
                        model.link_title = item.Element('title').Value
                    if item.Element('digest'):
                        model.link_content = item.Element('digest').Value
                    if item.Element('url'):
                        model.link_url = item.Element('url').Value
            elif xml.Name.LocalName == 'appmsg':
                if xml.Element('title'):
                    model.link_title = xml.Element('title').Value
                if xml.Element('des'):
                    model.link_content = xml.Element('des').Value
                if xml.Element('url'):
                    model.link_url = xml.Element('url').Value
                appinfo = xml.Element('appinfo')
                if appinfo and appinfo.Element('appname'):
                    model.link_from = appinfo.Element('appname').Value
            else:
                pass

    def _process_parse_message_app_message(self, row_content):
        title_pattern = '<title><!\[CDATA\[(.+?)\]\]></title>'
        content_pattern = '<des><!\[CDATA\[([\s\S]*)\]\]></des>'

        title = ''
        content = ''

        ans = re.findall(title_pattern, row_content)
        if ans:
            title = ans[0]
        ans = re.findall(content_pattern, row_content)
        if ans:
            content = ans[0]

        return "#*#".join((title, content))

    def _search_file(self, file_name):
        """搜索函数"""
        search_nodes = self.extend_nodes + self._search_nodes

        for node in search_nodes:
            results = node.Search(file_name)
            for result in results:
                if os.path.isfile(result.PathWithMountPoint):
                    if result.Parent not in self._search_nodes:
                        self._search_nodes.insert(0, result.Parent)
                    return result
        return None

    def _search_fav_file(self, file_name):
        """搜索函数"""
        for node in self.extend_nodes:
            target_node = node.GetByPath('/favorite')
            fav = target_node.Search(file_name)
            return next(iter(fav), None)

    def _search_media_nodes(self, dir_name):
        ret_nodes = []
        for node in self.extend_nodes:
            target = node.GetByPath(dir_name)
            if target is not None:
                ret_nodes.append(target)
        return ret_nodes

    def _save_ar_nodes(self, ar, nodes, type_):
        for n in nodes:
            ar.save_res_folder(n, type_)

    def get_wechat_res(self, ar):
        # 收藏
        fav_dir_nodes = self._search_media_nodes('/favorite')
        self._save_ar_nodes(ar, fav_dir_nodes, 'Other')

        # Emoji
        emoji_dir_nodes = self._search_media_nodes('/emoji')
        self._save_ar_nodes(ar, emoji_dir_nodes, 'Image')

        # image
        image_dir_nodes = self._search_media_nodes('/image')
        image_2_dir_nodes = self._search_media_nodes('/image2')
        self._save_ar_nodes(ar, image_dir_nodes, 'Image')
        self._save_ar_nodes(ar, image_2_dir_nodes, 'Image')

        # 消息 - video
        message_videos_dir_nodes = self._search_media_nodes('/video')
        self._save_ar_nodes(ar, message_videos_dir_nodes, 'Video')

        # 消息 - 语音
        message_voice_dir_nodes = self._search_media_nodes('/voice2')
        self._save_ar_nodes(ar, message_voice_dir_nodes, 'Audio')

        # story
        story_dir_nodes = self._search_media_nodes('/story')
        self._save_ar_nodes(ar, story_dir_nodes, 'Video')

        # res = ar.parse()
        # pr = ParserResults()
        # pr.Categories = DescripCategories.Wechat
        # pr.Models.AddRange(res)
        # pr.Build(self.build)
        # ds.Add(pr)


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
                    latitude = 0
                    longitude = 0
                    try:
                        latitude = float(self._get_ts_value(ret, 2))
                    except Exception as e:
                        pass
                    try:
                        longitude = float(self._get_ts_value(ret, 1))
                    except Exception as e:
                        pass
                    if latitude != 0 or longitude != 0:
                        feed.location_latitude = latitude
                        feed.location_longitude = longitude
                        try:
                            address1 = self._get_ts_value(ret, 3) or ""
                            address2 = self._get_ts_value(ret, 5) or ""
                            address3 = self._get_ts_value(ret, 15) or ""
                            feed.location_address = " ".join((address1, address2, address3))
                        except Exception as e:
                            feed.location_address = None
                        feed.location_type = model_wechat.LOCATION_TYPE_GOOGLE

    def get_likes(self, feed):
        feed.like_count = 0
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
                        try:
                            fl.timestamp = int(self._get_ts_value(like, 6))
                        except Exception as e:
                            pass
                        feed.like_count += 1
        if feed.like_count == 0:
            feed.like_id = 0

    def get_comments(self, feed):
        feed.comment_count = 0
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
                        try:
                            fm.timestamp = self._get_ts_value(comment, 6)
                        except Exception as e:
                            pass
                        feed.comment_count += 1
        if feed.comment_count == 0:
            feed.comment_id = 0

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
            if Decryptor.is_valid_decrypted_db(dst_db_path):
                return True
            else:
                try:
                    os.remove(dst_db_path)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error,
                                       "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
                    return False

        size = src_node.Size
        src_node.Data.seek(0)
        first_page = src_node.read(1024)

        if len(first_page) < 1024:
            TraceService.Trace(TraceLevel.Error,
                               "android_wechat.py Error: Decryptor decrypt() first_page size less than 1024!")
            return False

        salt = first_page[0:16]
        final_key = hashlib.pbkdf2_hmac('sha1', key.encode(encoding='utf-8'), salt, 4000, 32)
        final_key = Convert.FromBase64String(base64.b64encode(final_key))

        iv = first_page[1008: 1024]
        content = Decryptor.aes_decrypt(final_key,
                                        Convert.FromBase64String(base64.b64encode(iv)),
                                        Convert.FromBase64String(base64.b64encode(first_page[16:1008])))
        if not Decryptor.is_valid_decrypted_header(content):
            TraceService.Trace(TraceLevel.Error,
                               "android_wechat.py Error: Decryptor decrypt() error: db({0}) and key({1}) is not valid".format(
                                   src_node.AbsolutePath, key))
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
        Decryptor.db_insert_flag(dst_db_path)
        return True

    @staticmethod
    def is_valid_decrypted_db(db_path):
        if not os.path.exists(db_path):
            return False
        db = None
        cursor = None
        ret = False
        try:
            db = sqlite3.connect(db_path)
            cursor = db.cursor()
            sql = "select count(*) from sqlite_master where type='table' and name='PNFA' "
            cursor.execute(sql)
            row = cursor.fetchone()
            ret = row is not None and row[0] > 0
        except Exception as e:
            pass
        finally:
            if cursor is not None:
                cursor.close()
            if db is not None:
                db.close()
        return ret

    @staticmethod
    def db_insert_flag(db_path):
        if not os.path.exists(db_path):
            return
        db = None
        ret = False
        try:
            db = sqlite3.connect(db_path)
            sql = "create table if not exists PNFA(ID INTEGER PRIMARY KEY AUTOINCREMENT)"
            db.execute(sql)
        except Exception as e:
            pass
        finally:
            if db is not None:
                db.close()

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
            TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))
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

    @staticmethod
    def _judge_sex(flag):
        if flag == 0x01:
            return model_wechat.GENDER_MALE
        elif flag == 0x02:
            return model_wechat.GENDER_FEMALE
        else:
            return model_wechat.GENDER_NONE

    @staticmethod
    def parse_lvbuff(data):
        signature = None
        region = None
        gender = None

        if not data:
            return signature, region, gender

        try:
            if data[0] != 0x7B:
                return

            length = 1
            gender = Decryptor._judge_sex(data[8])
            # 这里是其他数据开始的位置
            start = 0x30
            # 个性签名
            signature_length, = struct.unpack('B', data[start:(start + 0x01)])
            start = start + length
            if data[start:start + signature_length]:
                signature = data[start:start + signature_length].decode(encoding="utf-8")
            # 国家
            start = start + signature_length
            country_length, = struct.unpack('>H', data[start:(start + 2)])
            start = start + 0x02
            country = str(data[start:start + country_length]).decode(encoding="utf-8")
            # 城市
            start += country_length
            city_length, = struct.unpack('>H', data[start:(start + 2)])
            start = start + 0x02
            city = data[start:start + city_length].decode(encoding="utf-8")
            if city or country:
                region = '{} {}'.format(country, city)
        except Exception as e:
            pass
        finally:
            return signature, region, gender
