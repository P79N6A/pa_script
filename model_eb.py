#coding:utf-8

__author__ = "chenfeiyang"

import PA_runtime
import clr
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference('model_im')
    clr.AddReference('unity_c37r')
except:
    pass
del clr

import System.Data.SQLite as sql
import os
import unity_c37r
import model_im
import traceback
from PA.InfraLib.Models import EC 
import random


TRADE_STATUS_UNKWON     = 0
TRADE_STATUS_PROCESSING = 1
TRADE_STATUS_FINISHED   = 2
TRADE_STATUS_CLOSE      = 3

class Column(object):
    def __init__(self):
        self.account_id = 0
        self.res = list()
    
    def get_value(self):
        return self.res

    def set_value_with_idx(self, idx, val):
        if idx >= len(self.res):
            return
        self.res[idx] = val

    def get_value_with_idx(self, idx):
        if idx >= len(self.res):
            return None
        return self.res[idx]

TBL_PRODUCT_CREATE = '''
    create table if not exists tb_product(aid text, source text, pid text, pname text, price text, ts int, description text,
    url text, shop_id text, deleted int default 0, source_file text)
'''

TBL_PRODUCT_INSERT = '''
    insert into tb_product values(?,?,?,?,?,?,?,?,?,?,?)
'''
#00未知、01收藏夹、02购物车、03已购买、04普通浏览、99其他
EB_PRODUCT_UNKWON = "0"
EB_PRODUCT_FAVORITE = "1"
EB_PRODUCT_SHOPCART = "2"
EB_PRODCUT_BUIED = "3"
EB_PRODUCT_BROWSE = "4"
EB_PRODUCT_OTHER = "99"
class EBProduct(Column):
    def __init__(self):
        super(EBProduct, self).__init__()
        self.source = 1
        self.product_id = 2
        self.product_name = 3
        self.price = 4
        self.time = 5
        self.description = 6
        self.url = 7
        self.shop_id = 8
        self.deleted = 9
        self.source_file = 10
        self.res = [None] * 11

TBL_SHOP_CREATE = '''
    create table if not exists tb_shop(aid text, sid text, sname text, boss_id text, boss_account text, boss_nick text, boss_full text, deleted int default 0, source_file text)
'''

TBL_SHOP_INSERT = '''
    insert into tb_shop values(?,?,?,?,?,?,?,?,?)
'''
class EBShop(Column):
    def __init__(self):
        super(EBShop, self).__init__()
        self.shop_id = 1
        self.shop_name = 2
        self.boss_id = 3
        self.boss_account = 4
        self.boss_nick = 5
        self.boss_real_name = 6
        self.deleted = 7
        self.source_file = 8
        self.res = [None] * 9

#Note: type limited...
TBL_DEAL_CREATE = '''
    create table if not exists tb_deal(aid text, target text, deal_type int, money real, status int, begin_time int, end_time int, deleted int default 0,
    desc text, content text, source_file text)
'''

TBL_DEAL_INSERT = '''
    insert into tb_deal values(?,?,?,?,?,?,?,?,?,?,?)
'''

EBDEAL_TYPE_REC = 0
EBDEAL_TYPE_SEND = 1
EBDEAL_TYPE_OTHER = 99


class EBDeal(Column):
    def __init__(self):
        super(EBDeal, self).__init__()
        self.target = 1
        self.deal_type = 2
        self.money = 3
        self.status = 4
        self.begin_time = 5
        self.end_time = 6
        self.deleted = 7
        self.desc = 8
        self.content = 9
        self.source_file = 10
        self.res = [None] * 11

TBL_LOG_CREATE = '''
    create table if not exists tb_log(aid text, log_description text, log_content text, log_res int, log_time int, deleted int default 0, source_file text)
'''

TBL_LOG_INSERT = '''
    insert into tb_log values(?,?,?,?,?,?,?)
'''
class EBLog(Column):
    def __init__(self):
        super(EBLog, self).__init__()
        self.description = 1
        self.content = 2
        self.result = 3
        self.time = 4
        self.deleted = 5
        self.source_file = 6
        self.res = [None] * 7

TBL_VERSION_CREATE = '''
    create table if not exists tb_version(version_key text, version_value)
'''

TBL_VERSION_INSERT = '''
    insert into tb_version values(?,?)
'''

EB_VERSION_KEY = 'EB_SYS_V'
EB_APP_VERSION_KEY = 'EB_APP_V'
EB_VERSION_VALUE = 1

class EB(object):
    def __init__(self, cache_db, app_v, id):
        self.cache = cache_db
        self.version = 1
        self.conn = None
        self.cmd = None
        self.need_parse = False
        self.im = model_im.IM()
        parent, tail = os.path.split(self.cache)
        self.log = unity_c37r.SimpleLogger(parent + 'C37R.log', True, id)
        self.db_check(app_v)
        if self.need_parse:
            if os.path.exists(self.cache):
                os.remove(self.cache)
            if os.path.exists(self.cache + '.im'):
                os.remove(self.cache + '.im')

    def db_check(self, app_v):
        if not os.path.exists(self.cache) or not os.path.exists(self.cache + '.im'):
            self.need_parse = True
            return
        conns = unity_c37r.create_connection(self.cache)
        cmd = sql.SQLiteCommand(conns)
        try:
            eb_checked = False
            eb_app_checked = False
            cmd.CommandText = '''
                select version_value from tb_version where version_key = '{}' 
            '''.format(EB_VERSION_KEY)
            reader = None
            reader = cmd.ExecuteReader()
            if reader.Read():
                res = unity_c37r.c_sharp_get_long(reader, 0)
                if res == EB_VERSION_VALUE:
                    eb_checked = True
            cmd.Dispose()
            cmd.CommandText = '''
                select version_value from tb_version where version_key = '{}'
            '''.format(EB_APP_VERSION_KEY)
            reader = cmd.ExecuteReader()
            if reader.Read():
                res = unity_c37r.c_sharp_get_long(reader, 0)
                if res == app_v:
                    eb_app_checked = True
            cmd.Dispose()
            reader.Close()
            conns.Close()
            conns = None
            reader = None
            if not ( eb_checked and eb_app_checked):
                self.need_parse = True
            n_parse = self.im.need_parse(self.cache + '.im', EB_VERSION_VALUE)
            if n_parse:
                self.need_parse = True
                return
            self.need_parse = False
        except Exception as e:
            traceback.print_exc()
            if reader is not None:
                reader.Close()
            if conns is not None:
                cmd.Dispose()
                conns.Close()
            self.log.m_err('check db failed, please try again!')
            self.log.m_err(e.message)
            self.need_parse = True
            return
        

    def db_create(self):
        self.conn = sql.SQLiteConnection("DataSource = {}".format(self.cache))
        self.conn.Open()
        self.cmd = sql.SQLiteCommand(self.conn)
        self.event = None
        self.im.db_create(self.cache + '.im')
        self.db_create_table()
        self.db_begin_transaction()


    def db_create_table(self):
        self.cmd.CommandText = TBL_DEAL_CREATE
        self.cmd.ExecuteNonQuery()
        self.cmd.CommandText = TBL_LOG_CREATE
        self.cmd.ExecuteNonQuery()
        self.cmd.CommandText = TBL_PRODUCT_CREATE
        self.cmd.ExecuteNonQuery()
        self.cmd.CommandText = TBL_SHOP_CREATE
        self.cmd.ExecuteNonQuery()
        self.cmd.CommandText = TBL_VERSION_CREATE
        self.cmd.ExecuteNonQuery()

    def db_insert_table_version(self, v_key, v_value):
        self.db_insert_table(TBL_VERSION_INSERT,[v_key, v_value])

    def sync_im_version(self):
        self.im.db_insert_table_version(model_im.VERSION_KEY_DB, model_im.VERSION_VALUE_DB)
        self.im.db_insert_table_version(model_im.VERSION_KEY_APP, EB_VERSION_VALUE)
        self.im.db_commit()

    def db_begin_transaction(self):
        self.event = self.conn.BeginTransaction()

    def db_commit(self):
        if self.event is not None:
            self.event.Commit()
        self.event = self.conn.BeginTransaction()
    
    def db_close(self):
        if self.event is not None:
            self.event.Commit()
        self.cmd.Dispose()
        self.conn.Close()
    
    def db_insert_table(self, command, val):
        self.cmd.CommandText = command
        self.cmd.Parameters.Clear()
        for v in val:
            param = self.cmd.CreateParameter()
            param.Value = v
            self.cmd.Parameters.Add(param)
        self.cmd.ExecuteNonQuery()
    
    def db_insert_table_deal(self, val):
        self.db_insert_table(TBL_DEAL_INSERT, val)
    
    def db_insert_table_log(self, val):
        self.db_insert_table(TBL_LOG_INSERT, val)
    
    def db_insert_table_shop(self, val):
        self.db_insert_table(TBL_SHOP_INSERT, val)
    
    def db_insert_table_product(self, val):
        self.db_insert_table(TBL_PRODUCT_INSERT, val)

#
# 暂时没什么思路从JSON中来构造，然而C#貌似有个什么JSON可以直接从类创造JSON String，有时间可以看看他是怎么实现的（时间在哪里？妈的）
# 如果能从JSON中来构造对象的话，能够省掉很多事情，稳定性也相对会变高。
#
class O(object):
    def __init__(self):
        self.uid = None
        self.nick = None
        self.photo = None
    
class GenerateModel(object):
    def __init__(self, cache_name):
        self.cache = cache_name
        self.im_cache = cache_name + '.IM'
        if not (os.path.exists(self.cache) and os.path.exists(self.im_cache)):
            raise IOError("File Losted!")
        self.ebc = unity_c37r.create_connection(self.cache)
        self.ccmd = sql.SQLiteCommand (self.ebc)
        self.contact = dict()

    def get_models(self):
        models = []
        models.extend(self.__get_product_models())
        models.extend(self.__get_log_models())
        models.extend(self.__get_shop_models())
        models.extend(self.__get_trading_models())
        models.extend(model_im.GenerateModel(self.im_cache).get_models())
        self.ccmd.Dispose()
        self.ebc.Close() #====>释放对数据库的占用
        return models
    #test pass
    def __get_log_models(self):
        self.ccmd.CommandText = '''
            select * from tb_log
        '''
        reader = self.ccmd.ExecuteReader()
        models = []
        while reader.Read():
            log = EC.ECLog()
            log.ID.Value = random.randint(0, 0xfffffff)
            if unity_c37r.c_sharp_get_string(reader, 1) is not '':
                log.Description.Value = unity_c37r.c_sharp_get_string(reader, 1)
            if unity_c37r.c_sharp_get_string(reader, 2) is not '':
                log.Content.Value = unity_c37r.c_sharp_get_string(reader, 2)
            if unity_c37r.c_sharp_get_long(reader, 3) is not None:
                log.Result.Value = unity_c37r.c_sharp_get_long(reader, 3)
            if unity_c37r.c_sharp_get_long(reader, 4) is not 0:
                log.Timestamp.Value = unity_c37r.get_c_sharp_ts(unity_c37r.c_sharp_get_long(reader, 4))
            models.append(log)
        reader.Close()
        #self.ccmd.Dispose()
        return models
    
    #真是神烦，一个屌SQLITE都能把过程设计的这么蛋疼。。。。
    #乖乖的用上下文设计会死？
    def __get_shop_models(self):
        self.ccmd.CommandText = '''
            select * from tb_shop
        '''
        reader = self.ccmd.ExecuteReader()
        models = []
        while reader.Read():
            sp = EC.Shop()
            if unity_c37r.c_sharp_get_string(reader, 0) is not '':
                sp.OwnerUserID.Value = unity_c37r.c_sharp_get_string(reader, 0)
            if unity_c37r.c_sharp_get_string(reader, 1) is not '':
                sp.ShopId.Value = unity_c37r.c_sharp_get_string(reader, 1)
            if unity_c37r.c_sharp_get_string(reader, 2) is not '':
                sp.ShopName.Value = unity_c37r.c_sharp_get_string(reader, 2)
            if unity_c37r.c_sharp_get_string(reader, 3) is not '':
                sp.BossId.Value = unity_c37r.c_sharp_get_string(reader, 3)
            if unity_c37r.c_sharp_get_string(reader, 4) is not '':
                sp.BossUserNmae.Value = unity_c37r.c_sharp_get_string(reader, 4)
            if unity_c37r.c_sharp_get_string(reader, 5) is not '':
                sp.BossNickName.Value = unity_c37r.c_sharp_get_string(reader, 5)
            if unity_c37r.c_sharp_get_string(reader, 6):
                sp.BossFullName.Value = unity_c37r.c_sharp_get_string(reader, 6)
            models.append(sp)
        reader.Close()
        #self.ccmd.Dispose()
        return models
    
    # 时间没测试
    def __get_product_models(self):
        self.ccmd.CommandText = '''
            select * from tb_product
        '''
        models = []    
        reader = self.ccmd.ExecuteReader()
        while reader.Read():
            p = EC.Product()
            if unity_c37r.c_sharp_get_string(reader, 0) is not '':
                p.OwnerUserID.Value = unity_c37r.c_sharp_get_string(reader, 0)
            if unity_c37r.c_sharp_get_string(reader, 1) is not "":
                tp = unity_c37r.c_sharp_get_string(reader, 1)
                if tp == EB_PRODUCT_UNKWON:
                    p.From.Value = u'未知来源'
                elif tp == EB_PRODCUT_BUIED:
                    p.From.Value = u'已购买的商品'
                elif tp == EB_PRODUCT_BROWSE:
                    p.From.Value = u'浏览或系统推荐的商品'
                elif tp == EB_PRODUCT_FAVORITE:
                    p.From.Value = u'收藏夹'
                elif tp == EB_PRODUCT_SHOPCART:
                    p.From.Value = u'购物车'
                elif tp == EB_PRODUCT_OTHER:
                    p.From.Value = u'未知来源'
            if unity_c37r.c_sharp_get_string(reader, 2) is not '':
                p.ProductId.Value = unity_c37r.c_sharp_get_string(reader, 2)
            if unity_c37r.c_sharp_get_string(reader, 3):
                p.ProductName.Value = unity_c37r.c_sharp_get_string(reader, 3)
            if unity_c37r.c_sharp_get_string(reader, 4) is not "":
                p.Price.Value = str(unity_c37r.c_sharp_get_string(reader, 4))
            else:
                p.Price.Value = u"价格未知"
            if unity_c37r.c_sharp_get_long(reader, 5) is not 0:
                p.Timestamp.Value = unity_c37r.get_c_sharp_ts(unity_c37r.c_sharp_get_long(reader, 5))
            if unity_c37r.c_sharp_get_string(reader, 6):
                p.Description.Value = unity_c37r.c_sharp_get_string(reader, 6)
            if unity_c37r.c_sharp_get_string(reader, 7):
                p.Url.Value = unity_c37r.get_c_sharp_uri(unity_c37r.c_sharp_get_string(reader, 7))
            if unity_c37r.c_sharp_get_string(reader, 8):
                p.ShopId.Value = unity_c37r.c_sharp_get_string(reader, 8)
            models.append(p)
        reader.Close()
        #self.ccmd.Dispose()
        return models
    
    def __prepare_contact(self):
        im_conn = unity_c37r.create_connection(self.cache + '.IM')
        cmd = sql.SQLiteCommand(im_conn)
        cmd.CommandText = '''
            select account_id, nickname, photo from account
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            o = O()
            o.uid = unity_c37r.c_sharp_get_string(reader, 0)
            o.nick = unity_c37r.c_sharp_get_string(reader, 1)
            o.photo = unity_c37r.c_sharp_get_string(reader, 2)
            self.contact[o.uid] = o
        reader.Close()
        #cmd.Dispose()
        cmd.CommandText = '''
            select friend_id, nickname, photo from friend
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            o = O()
            o.uid = unity_c37r.c_sharp_get_string(reader, 0)
            o.nick = unity_c37r.c_sharp_get_string(reader, 1)
            o.photo = unity_c37r.c_sharp_get_string(reader, 2)
            if self.contact.__contains__(o.uid):
                continue
            self.contact[o.uid] = o
        reader.Close()
        cmd.Dispose()
        im_conn.Close()

    def __get_trading_models(self):
        self.__prepare_contact()
        self.ccmd.CommandText = '''
            select * from tb_deal
        '''
        reader = self.ccmd.ExecuteReader()
        models = []
        while reader.Read():
            t = EC.Trading()
            if unity_c37r.c_sharp_get_string(reader, 0) is not '':
                t.OwnerUserID.Value = unity_c37r.c_sharp_get_string(reader, 0)
            if unity_c37r.c_sharp_get_string(reader, 1) is not '':
                uid = unity_c37r.c_sharp_get_string(reader, 1)
                usr = Common.UserIntro()
                usr.ID.Value = uid
                if not self.contact.__contains__(uid):
                    usr.Name.Value = u'未知联系人'
                else:
                    usr.Name.Value = self.contact[uid].nick
                    usr.Photo.Value = unity_c37r.get_c_sharp_uri(self.contact[uid].photo)
                t.Party.Value = usr
            if unity_c37r.c_sharp_get_long(reader, 2) is not None:
                tp = unity_c37r.c_sharp_get_long(reader, 2)
                if tp == EBDEAL_TYPE_REC:
                    t.Type.Value = EC.TradingType.Conllection
                elif tp == EBDEAL_TYPE_SEND:
                    t.Type.Value = EC.TradingType.Payment
                elif tp == EBDEAL_TYPE_OTHER:
                    t.Type.Value = EC.TradingType.None
            if unity_c37r.c_sharp_get_real(reader, 3) is not 0.0:
                t.Money.Value = str(unity_c37r.c_sharp_get_real(reader, 3))
            if unity_c37r.c_sharp_get_long(reader, 4) is not None:
                tp = unity_c37r.c_sharp_get_long(reader, 4)
                if tp == TRADE_STATUS_CLOSE:
                    t.Status.Value = EC.TradingStatus.Close
                elif tp == TRADE_STATUS_FINISHED:
                    t.Status.Value = EC.TradingStatus.Finish
                elif tp == TRADE_STATUS_PROCESSING:
                    t.Status.Value = EC.TradingStatus.Unfinish
                elif tp == TRADE_STATUS_UNKWON:
                    t.Status.Value = EC.TradingStatus.None
            if unity_c37r.c_sharp_get_long(reader, 5) is not 0:
                t.StartTime.Value = unity_c37r.get_c_sharp_ts(unity_c37r.c_sharp_get_long(reader, 5))
            if unity_c37r.c_sharp_get_long(reader, 6) is not 0:
                t.EndTime.Value = unity_c37r.get_c_sharp_ts(unity_c37r.c_sharp_get_long(reader, 6))
            if unity_c37r.c_sharp_get_string(reader, 8) is not '':
                t.Description.Value = unity_c37r.c_sharp_get_string(reader, 8)
            if unity_c37r.c_sharp_get_string(reader, 9):
                t.Content.Value = unity_c37r.c_sharp_get_string(reader, 9)
            models.append(t)
        reader.Close()
        #self.ccmd.Dispose()
        return models
#
# 拷文件。。。
#
# WACODE_0010_24
WA_CODE_DANGDANG = 1220001
WA_CODE_TMAIL = 1220002
WA_CODE_JINGDONG = 1220005
WA_CODE_SUNING = 1220006
WA_CODE_TAOBAO = 1220007
WA_CODE_NB1 = 1220008
WA_CODE_PAIPAI = 1220009
WA_CODE_JUMEI = 1220013
WA_CODE_NUOMI = 1220014
WA_CODE_MEITUAN = 1220040
WA_CODE_DAZHONG = 1220050
WA_CODE_XIANYU = 1220069
WA_CODE_WANHUI = 1220077


TBL_BCP_CREATE_ACCOUNT = '''
    create table if not exists WA_MFORENSICS_070100(COLLECT_TARGET_ID text,
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
                                                    DELETE_STATUS text,
                                                    DELETE_TIME int,
                                                    REG_ACCOUNT_TYPE text,
                                                    HASH_TYPE text,
                                                    USER_PHOTO text,
                                                    ACCOUNT_REG_DATE int,
                                                    LAST_LOGIN_TIME int,
                                                    LATEST_MOD_TIME int,
                                                    REMAIN_SUM int
)
'''

TBL_BCP_INSERT_ACCOUNT = '''
    insert into WA_MFORENSICS_070100 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''

TBL_BCP_CREATE_SHOP = '''
    create table if not exists WA_MFORENSICS_070300(COLLECT_TARGET_ID text,
                                                    NETWORK_APP text,
                                                    ACCOUNT_ID text,
                                                    ACCOUNT text,
                                                    SHOP_ID text,
                                                    SHOP_NAME text,
                                                    FRIEND_ID text,
                                                    FRIEND_ACCOUNT text,
                                                    FRIEND_NICKNAME text,
                                                    DELETE_STATUS text,
                                                    DELETE_TIME int,
                                                    REG_ACCOUNT_TYPE text
)
'''

TBL_BCP_INSERT_SHOP = '''
    insert into WA_MFORENSICS_070300 values(?,?,?,?,?,?,?,?,?,?,?,?)
'''

TBL_BCP_CREATE_PRODUCT = '''
    create table if not exists WA_MFORENSICS_070200(COLLECT_TARGET_ID text,
                                                    NETWORK_APP text,
                                                    ACCOUNT_ID text,
                                                    ACCOUNT text,
                                                    MATERIALS_SOURCE text,
                                                    OS_PRODUCT_ID text,
                                                    MATERIALS_NAME text,
                                                    CREATE_TIME int,
                                                    MONEY int,
                                                    URL text,
                                                    EXTRACT_DESC text,
                                                    BUYCOUNT int,
                                                    DELETE_STATUS text,
                                                    DELETE_TIME int,
                                                    REG_ACCOUNT_TYPE text,
                                                    SHOP_ID text
)
'''

TBL_BCP_INSERT_PRODUCT = '''
    insert into WA_MFORENSICS_070200 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''

TBL_BCP_CREATE_DEAL = '''
    create table if not exists WA_MFORENSICS_070400(COLLECT_TARGET_ID text,
                                                    NETWORK_APP text,
                                                    ACCOUNT_ID text,
                                                    ACCOUNT text,
                                                    REGIS_NICKNAME text,
                                                    FRIEND_ID text,
                                                    FRIEND_ACCOUNT text,
                                                    FRIEND_NICKNAME text,
                                                    LOCAL_ACTION text,
                                                    BUSINESS_TIME int,
                                                    MONEY int,
                                                    DEAL_STATUS text,
                                                    RELAFULLDESC text,
                                                    TALK_ID text,
                                                    DELETE_STATUS text,
                                                    DELETE_TIME int,
                                                    REG_ACCOUNT_TYPE text,
                                                    CURRENCY_TYPE text,
                                                    CONTACT_ACCOUNT_TYPE text,
                                                    PAY_ACCOUNT_ID text,
                                                    PAY_ACCOUNT text,
                                                    NAME text,
                                                    CITY_CODE text,
                                                    COMPANY_ADDRESS text,
                                                    LONGITUDE text,
                                                    LATITUDE text,
                                                    ABOVE_SEALEVEL text,
                                                    RECEIVER_NAME text,
                                                    RECEIVER_PHONE text
)
'''

TBL_BCP_INSERT_DEAL = '''
    insert into WA_MFORENSICS_070400(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''

TBL_BCP_CREATE_VERSION = '''
    create table if not exists tb_version(vkey text, vvalue int)
'''

TBL_BCP_INSERT_VERSION = '''
    insert into tb_version values(?,?)
'''

TBL_BCP_CREATE_SEARCH = '''
    create table if not exists WA_MFORENSICS_070500(COLLECT_TARGET_ID text,
                                                    NETWORK_APP text,
                                                    ACCOUNT_ID text,
                                                    ACCOUNT text,
                                                    CREATE_TIME text,
                                                    KEYWORD text,
                                                    DELETE_STATUS text,
                                                    DELETE_TIME int,
                                                    REG_ACCOUNT_TYPE text
)
'''

TBL_BCP_INSERT_SEARCH = '''
    insert into WA_MFORENSICS_070500 values(?,?,?,?,?,?,?,?,?,?)
'''

TBL_BCP_CREATE_CARD = '''
    create table if not exists WA_MFORENSICS_070600(COLLECT_TARGET_ID text,
                                                    REG_ACCOUNT_TYPE text,
                                                    ACCOUNT_ID text,
                                                    ACCOUNT text,
                                                    BANK_ACCOUNT_NUM text,
                                                    NAME text,
                                                    DELETE_STATUS text,
                                                    DELETE_TIME int
)
'''

TBL_BCP_INSERT_CARD = '''
    insert into WA_MFORENSICS_070600 values(?,?,?,?,?,?,?,?,?)
'''

class BasicCol(object):
    def __init__(self):
        self.colltection_target_id = 0
        self.app_code = 1
        self.account_id = 2
        self.account = 3
        self.idx = 3
        self.res = list()
    
    def get_values(self):
        return self.res
    
    def set_value_with_idx(self, idx, val):
        if idx >= len(self.res):
            return
        self.res[idx] = val

class AccountInfo(BasicCol):
    def __init__(self):
        super(AccountInfo, self).__init__()
        #self.account = 3
        self.nick = 4
        self.password = 5
        self.install_time = 6
        self.country_code = 7
        self.city_cide = 8
        self.telephone = 9
        self.mobile = 10
        self.email = 11
        self.certification_type = 12
        self.certification_code = 13
        self.sex_code = 14
        self.age = 15
        self.address = 16
        self.post_code = 17
        self.career = 18
        self.blood_type = 19
        self.real_name = 20
        self.signature = 21
        self.description = 22
        self.city = 23
        self.zodiac = 24
        self.constallation = 25
        self.graduation = 26
        self.birth = 27
        self.delete_status = 28
        self.delete_time = 29
        self.account_type = 30
        self.hash_type = 31
        self.photo = 32
        self.reg_time = 33
        self.login_time = 34
        self.last_update = 35
        self.remain_sum = 36
        self.res = [None] * 37

class ProductInfo(BasicCol):
    def __init__(self):
        super(ProductInfo, self).__init__()
        self.source = 4
        self.product_id = 5
        self.product_name = 6
        self.create_time = 7
        self.price = 8
        self.url = 9
        self.description = 10
        self.sell_count = 11
        self.delete_status = 12
        self.delete_time = 13
        self.account_type = 14
        self.shop_id = 15
        self.res = [None] * 16

class ShopInfo(BasicCol):
    def __init__(self):
        super(ShopInfo, self).__init__()
        idx = self.idx
        idx += 1
        self.shop_id = idx
        idx += 1
        self.shop_name = idx
        idx += 1
        self.boss_id = idx
        idx += 1
        self.boss_account = idx
        idx += 1
        self.boss_nick = idx
        idx += 1
        self.delete_status = idx
        idx += 1
        self.delete_time = idx
        idx += 1
        self.account_type = idx
        idx += 1
        if idx != 12:
            raise IOError('you failed!')
        self.idx = idx
        self.res = [None] * idx

class DealInfo(BasicCol):
    def __init__(self):
        super(DealInfo, self).__init__()
        idx = self.idx
        idx += 1
        self.account_nick = idx
        idx += 1
        self.friend_id = idx
        idx += 1
        self.friend_account = idx
        idx += 1
        self.friend_nick = idx
        idx += 1
        self.is_sender = idx
        idx += 1
        self.time = idx
        idx += 1
        self.money = idx
        idx += 1
        self.deal_status = idx
        idx += 1
        self.description = idx
        idx += 1
        self.deal_id = idx
        idx += 1
        self.delete_status = idx
        idx += 1
        self.delete_time = idx
        idx += 1
        self.account_type = idx
        idx += 1
        self.coin_type = idx
        idx += 1
        self.pay_tool = idx
        idx += 1
        self.pay_account_id = idx
        idx += 1
        self.pay_account = idx
        idx += 1
        self.pay_real_name = idx
        idx += 1
        self.address_name = idx
        idx += 1
        self.address = idx
        idx += 1
        self.longtitude = idx 
        idx += 1
        self.latitude = idx
        idx += 1
        self.attitude = idx
        idx += 1
        self.reciever_name = idx
        idx += 1
        self.reciever_phone = idx
        idx += 1
        if idx != 29:
            raise IOError('you failed')
        self.idx = idx
        self.res = [None] * self.idx
    
class SearchInfo(BasicCol):
    def __init__(self):
        super(SearchInfo, self).__init__()
        self.create_time = 4
        self.key_word = 5
        self.delete_status = 6
        self.delete_time = 7
        self.account_type = 8
        self.res = [None] * 9

class CardInfo(BasicCol):
    def __init__(self):
        super(CardInfo, self).__init__()
        self.card_number = 4
        self.real_name = 5
        self.res = [None] * 6
# the same as im
EB_BCP_VERION = 1
EB_BCP_VERSION_KEY = 'bcp_version'

class EBBCP(object):
    def __init__(self, bcp_path, mount_path, cache_db, bcp_db, collect_target_id, contact_account_type):
        res = EBBCP.before_check(bcp_path, bcp_db)
        self.need_generate = True
        if res is True:
            if os.path.exists(os.path.join(bcp_path, bcp_db)):
                os.remove(os.path.join(bcp_path, bcp_db))
        else:
            self.need_generate = False
        self.bcp_path = bcp_path
        self.cache_db = cache_db
        self.db = unity_c37r.create_connection(os.path.join(bcp_path, bcp_db), False)
        self.cache_path = os.path.join(bcp_path, contact_account_type)
        if not os.path.exists(self.cache_path):
            os.mkdir(self.cache_path)
        self.cmd = sql.SQLiteCommand(self.db)
        self.event = None
        self.colltection_target_id = collect_target_id
        self.app_code = contact_account_type
        self.mnt = mount_path
        if self.need_generate:
            self.create_tables()
            self.begin_event()

    @staticmethod
    def before_check(bcp_path, bcp_db):
        r_bcp_db = os.path.join(bcp_path, bcp_db)
        if not os.path.exists(r_bcp_db):
            return True
        try:
            conn = unity_c37r.create_connection(r_bcp_db)
            cmd = sql.SQLiteCommand(conn)
            cmd.CommandText = '''
                select * from tb_version where vkey = '{}'
            '''.format(EB_VERSION_KEY)
            reader = cmd.ExecuteReader()
            if reader.Read():
                value = unity_c37r.c_sharp_get_long(reader, 0)
                reader.Close()
                cmd.Dispose()
                conn.Close()
                if value != EB_VERSION_VALUE:
                    return True
                else:
                    return False
            else:
                reader.Close()
                cmd.Dispose()
                conn.Close()
                return True
        except:
            traceback.print_exc()
            reader.Close()
            cmd.Dispose()
            conn.Close()
            return False

    def create_tables(self):
        self.cmd.CommandText = TBL_BCP_CREATE_ACCOUNT
        self.cmd.ExecuteNonQuery()
        self.cmd.CommandText = TBL_BCP_CREATE_DEAL
        self.cmd.ExecuteNonQuery()
        self.cmd.CommandText = TBL_BCP_CREATE_PRODUCT
        self.cmd.ExecuteNonQuery()
        self.cmd.CommandText = TBL_BCP_CREATE_SHOP
        self.cmd.ExecuteNonQuery()
        self.cmd.CommandText = TBL_BCP_CREATE_VERSION
        self.cmd.ExecuteNonQuery()
        self.cmd.CommandText = TBL_BCP_CREATE_CARD
        self.cmd.ExecuteNonQuery()
        self.cmd.CommandText = TBL_BCP_CREATE_SEARCH
        self.cmd.ExecuteNonQuery()
    
    def begin_event(self):
        if self.event is not None:
            self.event = self.db.BeginTransaction()
    
    
    def db_commit(self):
        if not self.event is None:
            self.event.Commit()
        self.begin_event()
    
    def db_close(self):
        self.cmd.Dispose()
        self.db.Close()

    def generate_bcp(self):
        if not self.need_generate:
            return
        self.begin_event()
        self.generate_bcp_account()
        self.generate_product_bcp()
        self.generate_shop_bcp()
        self.generate_deal_bcp()
        self.db_commit()

    def generate_bcp_account(self):
        conn = unity_c37r.create_connection(self.cache_db + '.IM')
        cmds = sql.SQLiteCommand(conn)
        cmds.CommandText = '''
            select * from account
        '''
        reader = cmds.ExecuteReader()
        while reader.Read():
            a = AccountInfo()
            #a.colltection_target_id = self.colltection_target_id
            a.set_value_with_idx(a.colltection_target_id, self.colltection_target_id)
            a.set_value_with_idx(a.app_code, self.app_code)
            a.set_value_with_idx(a.account_id, unity_c37r.c_sharp_get_string(reader, 0))
            a.set_value_with_idx(a.nick, unity_c37r.c_sharp_get_string(reader, 1))
            a.set_value_with_idx(a.account, unity_c37r.c_sharp_get_string(reader, 2))
            a.set_value_with_idx(a.password, unity_c37r.c_sharp_get_string(reader, 3))
            pic = unity_c37r.c_sharp_get_string(reader, 4)
            pic = os.path.join(self.mnt, pic)
            if os.path.exists(pic):
                #a.set_value_with_idx(os.path.join(self.app_code, ))
                ppath,pname = os.path.split(pic)
                a.set_value_with_idx(self.app_code + '/' + pname)
                pass # copy file...
            a.set_value_with_idx(a.telephone, unity_c37r.c_sharp_get_string(reader, 5))
            gender = unity_c37r.c_sharp_get_long(reader, 6)
            rg = '0'
            if gender == 0:
                rg = '0'
            elif gender == 1:
                rg = '1'
            elif gender == 2:
                rg = '2'
            elif gender == 9:
                rg = '9'
            a.set_value_with_idx(a.sex_code, rg)
            a.set_value_with_idx(a.age, unity_c37r.c_sharp_get_long(reader, 7))
            #a.set_value_with_idx(a.country_code, unity_c37r.c_sharp_get_string(reader, 8)) # NOT SUPPORT RIGHT NOW!!!!
            a.set_value_with_idx(a.city, unity_c37r.c_sharp_get_string(reader, 9))
            #a.set_value_with_idx(a.signature, unity_c37r.c_sharp_get_string(reader, ))
            a.set_value_with_idx(a.address, unity_c37r.c_sharp_get_string(reader, 12))
            a.set_value_with_idx(a.birth, unity_c37r.c_sharp_get_string(reader, 13))
            a.set_value_with_idx(a.signature, unity_c37r.c_sharp_get_string(reader, 14))
            d = unity_c37r.c_sharp_get_string(reader, 15)
            a.set_value_with_idx(a.signature, d)
            # not support delete time
            unity_c37r.execute_query(self.cmd, TBL_BCP_INSERT_ACCOUNT, a.get_values())
        self.db_commit()
        reader.Close()
        cmds.Dispose()
        conn.Close()
    
    #
    # BCP DOES NOT CONTAIN THIS TABLE
    #
    def generate_friend_bcp(self):
        pass

    #
    # BCP DOES NOT CONTAIN THIS TABLE
    #
    def generate_message_bcp(self):
        pass
    
    def generate_deal_bcp(self):
        conn = unity_c37r.create_connection(self.cache_db + '.IM')
        cmds  = sql.SQLiteCommand(conn)
        cmds.CommandText = '''
            select * from deal
        '''
        reader = cmds.ExecuteReader()
        res = dict()
        while reader.Read():
            deal = DealInfo()
            deal.set_value_with_idx(deal.deal_id, unity_c37r.c_sharp_get_string(reader, 0))
            deal.set_value_with_idx(deal.money, unity_c37r.c_sharp_get_long(reader, 2))
            deal.set_value_with_idx(deal.description, unity_c37r.c_sharp_get_string(reader, 3))
            t = unity_c37r.c_sharp_get_long(reader, 6)
            if t != 0:
                deal.set_value_with_idx(deal.time, t)
            pass
            unity_c37r.execute_query(self.cmd, TBL_DEAL_INSERT, deal.get_values())
            # status... not supported right now
            # type ... not suppoted right now
        reader.Close()
        cmds.Dispose()
        conn.Close()
    
    def generate_product_bcp(self):
        conn = unity_c37r.create_connection(self.cache_db)
        cmds = sql.SQLiteCommand(conn)
        cmds.CommandText = '''
            select * from tb_product
        '''
        reader = cmds.ExecuteReader()
        while reader.Read():
            pdt = ProductInfo()
            pdt.set_value_with_idx(pdt.app_code, self.app_code)
            pdt.set_value_with_idx(pdt.colltection_target_id, self.colltection_target_id)
            pdt.set_value_with_idx(pdt.account_id, unity_c37r.c_sharp_get_string(reader, 0))
            tp = unity_c37r.c_sharp_get_string(reader, 1)
            pdt.set_value_with_idx(pdt.source, '0%s' %tp)
            pdt.set_value_with_idx(pdt.product_id, unity_c37r.c_sharp_get_string(reader, 2))
            pdt.set_value_with_idx(pdt.product_name, unity_c37r.c_sharp_get_string(reader, 3))
            pdt.set_value_with_idx(pdt.price, unity_c37r.c_sharp_get_string(reader, 4))
            t = unity_c37r.c_sharp_get_long(reader, 5)
            if t is not 0:
                pdt.set_value_with_idx(pdt.create_time, t)
            pdt.set_value_with_idx(pdt.description, unity_c37r.c_sharp_get_string(reader, 6))
            pdt.set_value_with_idx(pdt.url, unity_c37r.c_sharp_get_string(reader, 7))
            pdt.set_value_with_idx(pdt.shop_id, unity_c37r.c_sharp_get_string(reader, 8))
            pdt.set_value_with_idx(pdt.delete_status, unity_c37r.c_sharp_get_long(reader, 9))
            unity_c37r.execute_query(self.cmd, TBL_BCP_INSERT_PRODUCT, pdt.get_values())
        reader.Close()
        cmds.Dispose()
        conn.Close()
    
    def generate_shop_bcp(self):
        conn = unity_c37r.create_connection(self.cache_db)
        cmds = sql.SQLiteCommand(conn)
        cmds.CommandText = '''
            select * from tb_shop
        '''
        reader = cmds.ExecuteReader()
        while reader.Read():
            s = ShopInfo()
            s.set_value_with_idx(s.colltection_target_id, self.colltection_target_id)
            s.set_value_with_idx(s.app_code, self.app_code)
            s.set_value_with_idx(s.account_id, unity_c37r.c_sharp_get_string(reader, 0))
            s.set_value_with_idx(s.shop_id, unity_c37r.c_sharp_get_string(reader, 1))
            s.set_value_with_idx(s.shop_name, unity_c37r.c_sharp_get_string(reader, 2))
            s.set_value_with_idx(s.boss_id, unity_c37r.c_sharp_get_string(reader, 3))
            s.set_value_with_idx(s.boss_account, unity_c37r.c_sharp_get_string(reader, 4))
            s.set_value_with_idx(s.boss_nick, unity_c37r.c_sharp_get_string(reader, 5))
            s.set_value_with_idx(s.delete_status, unity_c37r.c_sharp_get_long(reader, 6))
            unity_c37r.execute_query(self.cmd, TBL_BCP_INSERT_SHOP, s.get_values())
        reader.Close()
        cmds.Dispose()
        conn.Close()