#coding:utf-8
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
    insert into tb_deal values(?,?,?,?,?,?,?,?)
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
                t.OwnerUserID = unity_c37r.c_sharp_get_string(reader, 0)
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
                    t.TradingType.Value = TradingType.Conllection
                elif tp == EBDEAL_TYPE_SEND:
                    t.TradingType.Value = TradingType.Payment
                elif tp == EBDEAL_TYPE_OTHER:
                    t.TradingType.Value = TradingType.None
            if unity_c37r.c_sharp_get_real(reader, 3) is not 0.0:
                t.Money = str(unity_c37r.c_sharp_get_real(reader, 3))
            if unity_c37r.c_sharp_get_long(reader, 4) is not None:
                tp = unity_c37r.c_sharp_get_long(reader, 4)
                if tp == TRADE_STATUS_CLOSE:
                    t.TradingStatus.Value = TradingStatus.Close
                elif tp == TRADE_STATUS_FINISHED:
                    t.TradingStatus.Value = TradingStatus.Finish
                elif tp == TRADE_STATUS_PROCESSING:
                    t.TradingStatus.Value = TradingStatus.Unfinish
                elif tp == TRADE_STATUS_UNKWON:
                    t.TradingStatus.Value = TradingStatus.None
            if unity_c37r.c_sharp_get_long(reader, 5) is not 0:
                t.StartTime.Value = unity_c37r.get_c_sharp_ts(unity_c37r.c_sharp_get_long(reader, 5))
            if unity_c37r.c_sharp_get_long(reader, 6) is not 0:
                t.EndTime.Value = unity_c37r.get_c_sharp_ts(unity_c37r.c_sharp_get_long(reader, 6))
            if unity_c37r.c_sharp_get_string(reader, 7) is not '':
                t.Description.Value = unity_c37r.c_sharp_get_string(reader, 7)
            if unity_c37r.c_sharp_get_string(reader, 8):
                t.Content.Value = unity_c37r.c_sharp_get_string(reader, 8)
            models.append(t)
        reader.Close()
        #self.ccmd.Dispose()
        return models