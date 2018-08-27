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
            if True: # Decryptor.decrypt(node, self._get_db_key(self.imei, self.uin), mm_db_path):
                self._parse_mm_db(mm_db_path)

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

    def _parse_mm_db(self, mm_db_path):
        mm_db = sqlite3.connect(mm_db_path)
        mm_cursor = mm_db.cursor()

        self._parse_mm_db_user_info(mm_cursor)

        mm_cursor.close()
        mm_db.close()

    def _parse_mm_db_user_info(self, cursor):
        self.user_account.account_id = self._parse_mm_db_get_user_info_from_userinfo(cursor, 2)
        self.user_account.nickname = self._parse_mm_db_get_user_info_from_userinfo(cursor, 4)
        self.user_account.email = self._parse_mm_db_get_user_info_from_userinfo(cursor, 5)
        self.user_account.telephone = self._parse_mm_db_get_user_info_from_userinfo(cursor, 6)
        self.user_account.signature = self._parse_mm_db_get_user_info_from_userinfo(cursor, 12291)
        self.user_account.username = self._parse_mm_db_get_user_info_from_userinfo2(cursor, 'USERINFO_LAST_LOGIN_USERNAME_STRING')
        self.user_account.photo = self._parse_mm_db_get_user_info_from_userinfo2(cursor, 'USERINFO_SELFINFO_SMALLIMGURL_STRING')

    def _parse_mm_db_get_user_info_from_userinfo(self, cursor, id):
        sql = 'select value from userinfo where id = {}'.format(id)
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            pass
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
            pass
        if row is not None:
            return row[0]
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
