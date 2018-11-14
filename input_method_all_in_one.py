#coding:utf-8
import clr
try:
    clr.AddReference('System.Data.SQLite')
    clr.AddReference('unity_c37r')
except:
    pass
del clr
#
# unity_c37r contains unities funcitons such as create C# sqlite connection
# execute sqlite commands and read sqlite command results
# there are also some useful functions while generating models
#
import sys
sys.setdefaultencoding('utf-8')
reload(sys)

import System.Data.SQLite as sql
from PA_runtime import *
from System.Text import *
from System.IO import *
from System.Security.Cryptography import *
from System import Convert
from PA.InfraLib.Utils import PList
from PA.InfraLib.Extensions import PlistHelper
from PA.InfraLib.Models import InputMethod
from PA.InfraLib.Models import Common
import unity_c37r
import os
import codecs
import re

EN_DEBUG = True

TBL_CREATE_IME_ACCOUNT = '''
    create table if not exists WA_MFORENSICS_120100(COLLECT_TARGET_ID text,
                                                    NETWORK_APP text,
                                                    ACCOUNT_ID text,
                                                    ACCOUNT text,
                                                    REGIS_NICKNAME text,
                                                    PASSWORD text,
                                                    INSTALL_TIME int,
                                                    AREA text,
                                                    CITY_CODE text,
                                                    FIXED_PHONE text,
                                                    MSISDN text,
                                                    EMAIL_ACCOUNT text,
                                                    CERTIFICATE_TYPE text,
                                                    CERTIFICATE_CODE text,
                                                    SEXCODE text,
                                                    AGE int,
                                                    POSTAL_ADDRESS text,
                                                    POSTAL_CODE text,
                                                    OCCUPATION_NAME text,
                                                    BLOOD_TYPE text,
                                                    NAME text,
                                                    SIGN_NAME text,
                                                    PERSONAL_DESC text,
                                                    REG_CITY text,
                                                    GRADUATESCHOOL text,
                                                    ZODIAC text,
                                                    CONSTALLATION text,
                                                    BIRTHDAY text,
                                                    HASH_TYPE text,
                                                    USER_PHOTO text,
                                                    ACCOUNT_REG_DATE int,
                                                    LAST_LOGIN_TIME int,
                                                    LATEST_MOD_TIME int,
                                                    DELETE_STATUS int,
                                                    DELETE_TIME int
)
'''

TBL_INSERT_IME_ACCOUNT = '''
    insert into WA_MFORENSICS_120100 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''

class IMEAccount(unity_c37r.C37RBasic):
    def __init__(self):
        super(IMEAccount, self).__init__()
        self.cid = 0
        self.app_type = 1
        self.account_id = 2
        self.account = 3
        self.nick = 4
        self.password = 5
        self.install_time = 6
        self.county_code = 7
        self.city_code = 8
        self.telephone = 9
        self.mobile = 10
        self.email = 11
        self.certificate_type = 12
        self.certificate_code = 13
        self.gender_code = 14
        self.age = 15
        self.address = 16
        self.post_code = 17
        self.occupation = 18
        self.blood_code =19
        self.real_name = 20
        self.signature = 21
        self.description = 22
        self.city = 23
        self.school = 24
        self.zodiac = 25
        self.constallation = 26
        self.birthday = 27
        self.hash_type = 28
        self.photo = 29
        self.reg_date = 30
        self.login_time = 31
        self.update_time = 32
        self.delete_status = 33
        self.delete_time = 34
        self.res = [None] * 35
    
TBL_CREATE_IME_DICT = '''
    create table if not exists WA_MFORENSICS_120200(COLLECT_TARGET_ID text,
                                                    NETWORK_APP text,
                                                    ACCOUNT_ID text,
                                                    ACCOUNT text,
                                                    KEYWORD text,
                                                    TIMES int,
                                                    DELETE_STATUS int,
                                                    DELETE_TIME int
)
'''
        
TBL_INSERT_IME_DICT = '''
    insert into WA_MFORENSICS_120200 values(?,?,?,?,?,?,?,?)
'''

class IMEKeyword(unity_c37r.C37RBasic):
    def __init__(self):
        super(IMEKeyword, self).__init__()
        self.cid = 0
        self.app_type = 1
        self.account_id = 2
        self.account = 3
        self.key_word = 4
        self.times = 5
        self.delete_status = 6
        self.delete_time = 7
        self.res = [None] * 8

IME_DB_KEY = 'DB_KEY'
IME_DB_VAL = 1
IME_APP_KEY = 'APP_KEY'

class IME(object):
    def __init__(self, cache_db, app_version):
        self.cache_db = cache_db
        self.conn = None
        self.cmd = None
        self.transaction = None
        res = 0x0
        res |= 0x1 if unity_c37r.CheckVersion(self.cache_db, IME_DB_KEY, IME_DB_VAL) else 0
        res |= 0x2 if unity_c37r.CheckVersion(self.cache_db, IME_APP_KEY, app_version) else 0
        self.need_parse = True
        if res != 0x3:
            self.need_parse = True
        else:
            self.need_parse = False
            return
        if os.path.exists(self.cache_db):
            os.remove(self.cache_db)
        self.conn = unity_c37r.create_connection(self.cache_db, False)
        self.cmd = sql.SQLiteCommand(self.conn)
        self.create_tables()
    
    def create_tables(self):
        self.cmd.CommandText = TBL_CREATE_IME_ACCOUNT
        self.cmd.ExecuteNonQuery()
        self.cmd.CommandText = TBL_CREATE_IME_DICT
        self.cmd.ExecuteNonQuery()
        self.cmd.CommandText = unity_c37r.TBL_CREATE_VERSION
        self.cmd.ExecuteNonQuery()
        self.begin_event()

    def begin_event(self):
        self.transaction = self.conn.BeginTransaction()
    
    def db_commit(self):
        if self.transaction is not None:
            self.transaction.Commit()
        self.transaction = self.conn.BeginTransaction()
    
    def db_close(self):
        if self.cmd is not None:
            self.cmd.Dispose()
        if self.conn is not None:
            self.conn.Close()

    def db_insert_account(self, a):
        unity_c37r.execute_query(self.cmd, TBL_INSERT_IME_ACCOUNT, a.get_values())
    
    def db_insert_key(self, k):
        unity_c37r.execute_query(self.cmd, TBL_INSERT_IME_DICT, k.get_values())
    
    def db_insert_version(self, v_key, v_val):
        unity_c37r.execute_query(self.cmd, unity_c37r.TBL_INSERT_VERSION, [v_key, v_val])
    
    def generate_models(self):
        pass
    
    def generate_bcp(self):
        pass

#
# generate IMEModels
#
class IMEModel(object):
    def __init__(self, cache_db):
        self.cache_db = cache_db
    
    def get_models(self):
        models = list()
        conn = unity_c37r.create_connection(self.cache_db, True)
        cmd = sql.SQLiteCommand(conn)
        cmd.CommandText = '''
            select * from WA_MFORENSICS_120100
        '''
        reader = cmd.ExecuteReader()
        a = IMEAccount()
        while reader.Read():
            am = Common.User()
            am.ID.Value = unity_c37r.c_sharp_get_string(reader, a.account)
            am.Name.Value = unity_c37r.c_sharp_get_string(reader, a.nick)
            photo = unity_c37r.c_sharp_get_string(reader, a.photo)
            photo = unity_c37r.get_c_sharp_uri(photo)
            am.PhotoUris.Add(photo)
            # reserved gender
            sex = unity_c37r.c_sharp_get_string(reader, a.gender_code)
            models.append(am)
        reader.Close()
        cmd.CommandText = '''
            select * from WA_MFORENSICS_120200
        '''
        reader = cmd.ExecuteReader()
        b = IMEKeyword()
        while reader.Read():
            kw = InputMethod.WordFrequency()
            kw.KeyWord.Value = unity_c37r.c_sharp_get_string(reader, b.key_word)
            kw.Count.Value = unity_c37r.c_sharp_get_long(reader, b.times)
            models.append(kw)
        reader.Close()
        cmd.Dispose()
        conn.Close()
        return models
#
# all_in_one 因为聚合的设计，我们不在是适配单个app的脚本，因此在匹配时，可能和存在多个节点命中的情况，因此我们，要保证每个节点匹配到正确节点
# 另外，在节点返回数据时，要注意节点所属的ID，因此我们将Generate Model 安排在每个类中进行，而不是之前的单独在parse function 中产生
# 第三点，就是尝试多线程加载的方式，并行分析，这个要加以尝试
#
class IMEBase(object):
    def __init__(self, node, extract_source, extract_deleted, cache_path):
        self.extract_source = extract_source
        self.extract_deleted = extract_deleted
        self.cache = ds.OpenCachePath(cache_path)
        self.node = node
        if not os.path.exists(self.cache):
            os.mkdir(self.cache) 

    def judge(self):
        return False
    
    def generate_model_container(self):
        return None
    
    def go(self):
        pass

class SougouIME(IMEBase):
    def __init__(self, node, extract_source, extract_deleted):
        super(SougouIME, self).__init__(node, extract_source, extract_deleted, 'sougou_ime')
        self.grp_node = None
        self.hitted = self.judge()
        if not self.hitted:
            return
        self.acquire_shared_node()
        self.ime = IME(self.cache + '/C37R', 1)
        self.need_parse = self.ime.need_parse
        if not self.ime.need_parse:
            return

    def judge(self):
        global EN_DEBUG
        if EN_DEBUG:
            return True
        plist_node = self.node.GetByPath('Library/Preferences/com.sogou.sogouinput.plist')
        if plist_node is None:
            return False
        else:
            return True

    def generate_model_container(self):
        pr = ParserResults()
        pr.Categories = DescripCategories.SougouIme
        pr.Build(u'搜狗输入法')
        return pr
    
    def acquire_shared_node(self):
        global EN_DEBUG
        grp = ds.GroupContainers.ToArray()
        if EN_DEBUG:
            self.grp_node = self.node
            return
        for g in grp:
            sub_node = g.GetByPath('dict/usr/uud_base.txt')
            if sub_node is None:
                continue
            else:
                self.grp_node = g
                break
        if self.grp_node is None:
            print("can't get group nodes!")
            return
    
    def go(self):
        txt_node = None
        if self.grp_node is not None:
            txt_node = self.grp_node.GetByPath('dict/usr/uud_base.txt')
        if txt_node is not None:
            f = open(txt_node.PathWithMountPoint, 'rb')
            content = bytearray(f.read())
            f.close()
            #decoder = codecs.getdecoder('utf_16_le')
            #r_content = decoder(content)[0]
            r_content = content.decode('utf_16_le')
            rl = r_content.split('\n')
            for r in rl:
                # 跳过前两行
                if  not r.__contains__('['):
                    continue
                #[dian][hua]	电话	1
                kl = r.split('\t')
                kw = IMEKeyword()
                kw.set_value_with_idx(kw.key_word, kl[1])
                kw.set_value_with_idx(kw.times, int(kl[2]))
                self.ime.db_insert_key(kw)
        self.ime.db_commit()
        pl_node = None
        if self.grp_node is not None:
            pl_node = self.grp_node.GetByPath('dict/dictSync/userinfo.plist')
        if pl_node is not None:
            b = PlistHelper.ReadPlist(pl_node)
            a = IMEAccount()
            a.set_value_with_idx(a.account, b['userId'].ToString())
            a.set_value_with_idx(a.account_id, b['userId'].ToString())
            a.set_value_with_idx(a.nick, b['uniqName'].ToString())
            a.set_value_with_idx(a.photo, b['largeAvatar'].ToString())
            a.set_value_with_idx(a.gender_code, b['sex'].ToString())
            self.ime.db_insert_account(a)
        self.ime.db_commit()
        self.ime.db_insert_version(IME_DB_KEY, IME_DB_VAL)
        self.ime.db_insert_version(IME_APP_KEY, 1)
        self.ime.db_commit()
        self.ime.db_close()
#
# 系统输入法
# 系统输入法存在新老版本的区别，这里只支持新版本，老版本等以后有空在研究
# 新版本：keyboard cache 存在数据库中，代码很简单，具体请看代码
# update:=>ios 10 以上也支持zh-hans.db
# 英文的词库可以用linux的string命令进行文本分割，早期的词库string命令可能会失效
#
class SystemIME(IMEBase):
    def __init__(self, node, extract_source, extract_deleted):
        super(SystemIME, self).__init__(node, extract_source, extract_deleted, 'SystemIme')
        self.hitted = False
        self.version = -1 # =>通过version 来区分不同的版本的情况，这里从11.4.1开始 到 12.01
        self.ime = IME(self.cache + "/C37R", 1)
        self.need_parse = self.ime.need_parse
        
    def judge(self):
        res = 0x0
        key_node = self.node.GetByPath('DynamicPhraseLexicon_zh_Hans.db')
        if key_node is not None:
            self.version = 1141
            self.hitted = True
            res |= 0x1
        dm_node = self.node.GetByPath('en-dynamic.lm/dynamic-lexicon.dat')
        if dm_node is not None:
            print 'detect ios-10 upper system input method'
            res |= 0x2
        self.version = res
    
    def parse_sys_ver_1141(self):
        #if the sys ime does not contain zh.db
        if self.version & 0x1 == 0:
            return
        sql_node = self.node.GetByPath('DynamicPhraseLexicon_zh_Hans.db')
        db = unity_c37r.create_connection_tentatively(sql_node.PathWithMountPoint, True)
        cmd = sql.SQLiteCommand(db)
        cmd.CommandText = '''
            select Reading, Surface, Seed from Words
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            pharse = unity_c37r.c_sharp_get_blob(reader, 0)
            pharse = pharse.decode('utf_16_le', 'ignore') # => TODO:add pharse to sqlite
            bts = unity_c37r.c_sharp_get_blob(reade, 1)
            idx = 0
            words = ''
            while idx < len(bts):
                m_string = r'\u%x%x' %(bts[idx + 1], bts[idx]) # => note little ending
                m_string = m_string.decode('unicode_escape')
                words += ''
                idx += 2
            kw = IMEKeyword()
            kw.set_value_with_idx(kw.key_word, words)
            times = unity_c37r.c_sharp_get_long(reader, 2)
            kw.set_value_with_idx(kw.times, times)
            self.ime.db_insert_key(kw)
        self.ime.db_commit()

    def parse_dynamic_file(self):
        if self.version & 0x2 == 0:
            return
        dynamic_node = self.node.GetByPath("en-dynamic.lm/dynamic-lexicon.dat")
        if dynamic_node is None:
            return None
        strings = unity_c37r.py_strings(dynamic_node.PathWithMountPoint)
        for s in strings:
            kw = IMEKeyword()
            kw.set_value_with_idx(kw.key_word, s)
            kw.set_value_with_idx(kw.times , 1)
            self.ime.db_insert_key(kw)
        self.ime.db_commit()
    
    def go(self):
        self.parse_sys_ver_1141()
        self.parse_dynamic_file()

    def generate_model_container(self):
        pr = ParserResults()
        pr.Categories = DescripCategories.IOSSysIme
        pr.Build(u'IOS系统输入法')
        return pr

SUPPORTTED_APP_LIST = [SougouIME, SystemIME]
#
# 烈酒入肠，千忧何妨!
#
def go(root, extract_source, extract_deleted):
    root = FileSystem.FromLocalDir(r'D:\Cases\iPhone 7 plus_12.0.1_201811072014_EXP\Image\private\var\mobile\Containers\Shared\AppGroup\51629415-AB93-43C9-8165-D1104D6AA6AA')
    pr = ParserResults()
    for apps in SUPPORTTED_APP_LIST:
        a = apps(root, extract_source, extract_deleted)
        if not a.hitted:
            continue
        else: # 每次app命中的节点只会传入一个进来，因此我们单次只返回一个结果。
            if a.need_parse:
                a.go()
            mlm = ModelListMerger()
            models = IMEModel(a.cache + '/C37R').get_models()
            container = a.generate_model_container()
            pr.Models.AddRange(list(mlm.GetUnique(models)))
            return pr
