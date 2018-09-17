#coding:utf-8
#   Author:C37R
#   脚本分析了淘宝app的账号、聊天、app日志、搜索和APP推荐内容
#   具体分析详见分析说明
#   
import clr
clr.AddReference('System.Data.SQLite')
clr.AddReference('Base3264-UrlEncoder')
try:
    clr.AddReference('model_im')
    clr.AddReference('unity_c37r')
except:
    pass
del clr

import System.Data.SQLite as sql
from PA_runtime import *
from System.Text import *
from System.IO import *
from System.Security.Cryptography import *
from System import Convert

import datetime
import model_im
import os
import logging
import re
import unity_c37r
import json
import biplist
import random
#
# 淘宝多账号并不会删除账号的cache数据，因此，我们要建对象，而不是解析单个脚本
#
class TbAccount(object):
    def __init__(self):
        self.uid = None   # 淘宝数字账号
        self.tb_id = None # 淘宝字符串账号（用户可见）
        self.tb_nick = None # 淘宝昵称

class Taobao(object):
    def __init__(self, node, extract_source, extract_deleted):
        self.node = node
        self.extract_source = extract_source
        self.extract_deleted = extract_deleted
        self.cache = ds.OpenCachePath('taobao')
        self.im = model_im.IM()
        self.im.db_create(self.cache + '/C37R')
        self.account = list()
        self.log = unity_c37r.SimpleLogger(self.cache + '/C37R.log', True, 'Taobao')
        self.message_dict = dict() # 放关于message对应的表
        self.log.set_level(1) # 开启双重print
   
    def search(self):
        cache_node = self.node.GetByPath('Library/Caches')
        pl = os.listdir(cache_node.PathWithMountPoint)
        al = list()
        for p in pl:
            res = re.search(r'amps3_(.*)\.db$', p, re.I | re.M)
            if res is not None:
                if not al.__contains__(res.group(1)):
                    al.append(res.group(1))
        for a in al:
            ac = TbAccount()
            ac.uid = a
            self.account.append(ac)
        self.log.m_print('total find %d account(s)' % len(self.account))
        try:
            self.prepare_message_dict()
        except Exception as e:
            self.log.m_err('get message_dict failed!')


    def prepare_message_dict(self):
        db_node = self.node.GetByPath('Library/Caches/YWDB')
        p_l = os.listdir(db_node.PathWithMountPoint)
        db_dirs = list()
        for p in p_l:
            res = re.search('WXOPENIMSDKDB(.*)', p, re.I | re.M)
            if res is not None:
                db_dirs.append(p)
        for d in db_dirs:
            message_node = self.node.GetByPath('Library/Caches/YWDB/%s/message.db' %d)
            if message_node is not None:
                conn = unity_c37r.create_connection(message_node.PathWithMountPoint)
                cmd = sql.SQLiteCommand(conn)
                cmd.CommandText = '''
                    select ZUSER_ID from ZUSERINFO
                '''
                reader = cmd.ExecuteReader() 
                if reader.Read():
                    tb_id = unity_c37r.c_sharp_get_blob(reader, 0).decode('utf-8') #cntaobaotb5057305_11
                    tb_id = re.search('cntaobao(.*)', tb_id, re.I | re.M).group(1)
                    self.message_dict[tb_id] = message_node.PathWithMountPoint
                cmd.Dispose()
                conn.Close()

    def parse_search(self, ac):
        db_node = self.node.GetByPath('Library/edge_compute.db')
        conn = unity_c37r.create_connection(db_node.PathWithMountPoint)
        cmd = sql.SQLiteCommand(conn)
        cmd.CommandText = '''
            select args,create_time from usertrack where owner_id = {} and page_name = 'Page_SearchItemList'
        '''.format(ac.uid)
        reader = cmd.ExecuteReader()
        while reader.Read():
            s = model_im.Search()
            s.account_id = ac.uid
            m_str = unity_c37r.c_sharp_get_string(reader, 0)
            try:
                s.key = re.search('keyword=(.*?),', m_str, re.I | re.M).group(1)
            except:
                self.log.m_err('error string:{}'.format(m_str))
                continue
            s.create_time = unity_c37r.c_sharp_get_long(reader, 1) / 1000
            self.im.db_insert_table_search(s)
        self.im.db_commit()
        cmd.Dispose()
        conn.Close()
 
    def create_fake_friend(self, ac, name):
        f = model_im.Friend()
        f.account_id = ac.uid
        f.nickname = name
        f.remark = name
        f.friend_id = random.randint(0, 0xffffffff)# 产生假ID
        return f
        

    def parse(self, ac):
        #ac = TbAccount()
        db_node = self.node.GetByPath('Library/Caches/amps3_{}.db'.format(ac.uid))
        if db_node is None:
            self.log.m_print('get db node failed, parse exists!')
            return
        path = db_node.PathWithMountPoint
        conn = unity_c37r.create_connection(path)
        cmd = sql.SQLiteCommand(conn)
        cmd.CommandText = '''
            select ZDISPLAYNAME, ZHEADPIC, ZMOBILEPHONE, ZNICK from ZAMPUSER where ZTAOBAOID = {}
        '''.format(ac.uid)
        reader = cmd.ExecuteReader()
        image_cache_path = os.path.join(self.node.PathWithMountPoint, 'Library/Caches/YWDiskCache/ImageCache')
        if reader.Read():
            a = model_im.Account()
            a.account_id = ac.uid
            a.nickname = unity_c37r.c_sharp_get_string(reader, 3)
            a.username = unity_c37r.c_sharp_get_string(reader, 0)
            ac.tb_id = a.username
            a.telephone = unity_c37r.c_sharp_get_string(reader, 2)
            pic = unity_c37r.c_sharp_get_string(reader, 1)
            hash_code = unity_c37r.md5(pic)
            if os.path.exists(os.path.join(image_cache_path, hash_code)):
                a.photo = 'Library/Cache/YWDiskCache/YWDiskCache/ImageCache/{}'.format(hash_code)
            else:
                a.photo = pic
            self.im.db_insert_table_account(a)
        cmd.Dispose()
        cmd.CommandText = '''
           select ZDISPLAYNAME, ZHEADPIC, ZMOBILEPHONE, ZNICK, ZTAOBAOID, ZSIGNATURE from ZAMPUSER where ZTAOBAOID != {}
        '''.format(ac.uid)
        reader = cmd.ExecuteReader()
        f_dict = dict()
        while reader.Read():
            f = model_im.Friend()
            f.account_id = self.account
            try:
                f.friend_id = int(unity_c37r.c_sharp_get_string(reader, 4))
            except Exception as e:
                self.log.m_err('error account id: %s' % unity_c37r.c_sharp_get_string(reader, 4))
                f.friend_id = random.randint(0, 0xffffffff)# 产生假ID
            f.nickname = unity_c37r.c_sharp_get_string(reader, 3)
            f.remark = unity_c37r.c_sharp_get_string(reader, 0)
            f.telephone = unity_c37r.c_sharp_get_string(reader, 2)
            f.signature = unity_c37r.c_sharp_get_string(reader, 5)
            pic = unity_c37r.c_sharp_get_string(reader, 1)
            hash_code = unity_c37r.md5(pic)
            if os.path.exists(os.path.join(image_cache_path, hash_code)):
                f.photo = 'Library/Cache/YWDiskCache/ImageCache/{}'.format(hash_code)
            else:
                f.photo = pic
            f_dict[f.remark] = f
        cmd.Dispose()
        conn.Close()
        conn = None
        cmd = None     
        # 用户行为分析 可以说很详细了
        if ac.tb_id is None:
            reader = None
        else:
            log_node = self.node.GetByPath('Library/Caches/StructuredLogs/cntaobao{}.db'.format(ac.tb_id))
            if log_node is None:
                reader = None
            else:
                conn = unity_c37r.create_connection(log_node.PathWithMountPoint)
                cmd = sql.SQLiteCommand(conn)
                cmd.CommandText = '''
                    select id, operation_id, record, logtime, result from Record 
                '''
                reader = cmd.ExecuteReader()
        while reader is not None and reader.Read():
            try:
                logs = model_im.APPLog()
                logs.log_id = unity_c37r.c_sharp_get_long(reader, 0)
                logs.log_description = ""
                m_str = unity_c37r.c_sharp_get_string(reader, 1)
                m_sl = m_str.split('|')
                sender = m_sl[1]
                reciever = m_sl[2]
                if reciever == 'wwLogin':
                    sender = re.search(r'\(null\)(.*)', sender, re.I | re.M).group(1)
                    logs.log_description = '''{} try to login'''.format(sender)
                else:
                    sender = re.search('cnhhupan(.*)', sender, re.I | re.M).group(1)
                    reciever = re.search('cnhhupan(.*)', reciever, re.I | re.M).group(1)
                    logs.log_description = '''{} try to send message to {}'''.format(sender, reciever)
                m_str = unity_c37r.c_sharp_get_string(reader, 2)
                js = json.loads(m_str)
                logs.log_content = js.get('title')
                logs.log_time = unity_c37r.c_sharp_get_long(reader, 3) / 1000
                logs.log_result = unity_c37r.c_sharp_get_long(reader, 4)
                self.im.db_insert_table_log(logs)
            except Exception as e:
                self.log.m_print(e)
                self.log.m_err('detect wrong string format: {}'.format(m_str))
        if conn is not None:
            cmd.Dispose()
            conn.Close()
            cmd = None
            conn = None
        #l = unity_c37r.search_for_certain(self.node, 'Library/Caches/YWDB/WXOPENIMSDKDB(.*)/message.db$')
        u = self.message_dict.get(ac.tb_id)
        if u is None:
            self.log.m_print('no %s chat info!' %ac.tb_id)
            return

        conn = unity_c37r.create_connection(u)
        cmd = sql.SQLiteCommand(conn)
        cmd.CommandText = '''
            select ZDISPLAYNAME, ZEMAIL, ZGENDER, ZTBNICK, ZPHONE_NUM from ZWWPERSON
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            user_name = unity_c37r.c_sharp_get_string(reader, 0)
            if f_dict.__contains__(user_name):
                pass
            else:
                f = model_im.Friend()
                f.account_id = ac.uid
                f.nickname = unity_c37r.c_sharp_get_string(reader, 3)
                f.remark = unity_c37r.c_sharp_get_string(reader, 0)
                f.friend_id = random.randint(0, 0xffffffff)# 产生假ID
                f_dict[f.remark] = f
        
        for k in f_dict:
            #self.log.m_print(k)
            self.im.db_insert_table_friend(f_dict[k])

        cmd.Dispose()
        cmd.CommandText = '''
            select ZMESSAGEID, ZTYPE, ZTIME, ZCONTENT, ZRECEIVERID, ZSENDERID from ZWWMESSAGE
        '''
        reader = cmd.ExecuteReader()
        # 最好将talker_name之类的设置全面，因为淘宝的id管理比较混乱。
        # 原则上淘宝账号、支付宝账号、闲鱼账号、天猫账号、阿里巴巴账号等通用，但是实际上他们在进行管理时
        # 各自采用不同的ID管理方式。导致这边筛选很尴尬
        idx = 0
        while reader.Read():
            m = model_im.Message()
            sender = unity_c37r.c_sharp_get_blob(reader, 5).decode('utf-8') #struct.unpack('i', unity_c37r.c_sharp_get_blob(reader, 5))
            reciever = unity_c37r.c_sharp_get_blob(reader, 4).decode('utf-8') #struct.unpack('i', unity_c37r.c_sharp_get_blob(reader, 4))
            try:
                sender = re.search("cnhhupan(.*)", sender, re.I | re.M).group(1)
                reciever = re.search("cnhhupan(.*)", reciever, re.I | re.M).group(1)
            except:
                try:
                    if sender.__contains__('alichn'):
                        sender = re.search('cnalichn(.*)', sender, re.I | re.M).group(1)
                    else:
                        reciever = re.search('cnalichn(.*)', reciever, re.I | re.M).group(1)
                except:
                    self.log.m_print("sender:{} rec:{}".format(sender, reciever))
            if sender.__contains__(':'):
                sender = sender.split(':')[0]
            if reciever.__contains__(':'):
                reciever = reciever.split(':')[0]
            if sender == ac.tb_id:
                m.is_sender = 1
                if not f_dict.__contains__(reciever):
                    self.log.m_print("no such reciever friend:%s" %reciever)
                    continue
                m.talker_id = f_dict[reciever].friend_id
                m.sender_id = ac.uid
            elif f_dict.__contains__(sender):
                m.is_sender = 0
                m.talker_id = f_dict[sender].friend_id
                m.sender_id = f_dict[sender].friend_id
            elif f_dict.__contains__(sender): #类似于hqxuelang:服务助手 这种有时候就不在表中
                m.is_sender = 0
                m.talker_id = f_dict[sender].friend_id
                m.sender_id = f_dict[sender].friend_id
            else:
                self.log.m_print('find unchecked friend:%s' %sender)
                f = self.create_fake_friend(ac, sender)
                f_dict[sender] = f
                m.talker_id = f_dict[sender].friend_id
                m.sender_id = f_dict[sender].friend_id
            m.msg_id = unity_c37r.c_sharp_get_long(reader, 0)
            tp = unity_c37r.c_sharp_get_long(reader, 1)
            tm = unity_c37r.c_sharp_try_get_time(reader, 2)
            m.send_time = unity_c37r.format_mac_timestamp(tm)
            m_str = unity_c37r.c_sharp_get_blob(reader, 3)
            if m_str is not None:
                m_str = m_str.decode('utf-8')
            else:
                self.log.m_print('find useless data, skipped!')
                m_str = ''
            if tp == 0 or tp == 5:
                m.content = m_str
                m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
            elif tp == 1:
                m.content = m_str
                m.media_path = m_str
                m.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
            elif tp == 2:
                m.content = m_str
                m.media_path = m_str
                m.type = model_im.MESSAGE_CONTENT_TYPE_VOICE
            elif tp == 8:
                m.content = 'empty message'
                m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
            elif tp == 65:
                try:
                    js = json.loads(m_str)
                    # 这里有子类型号
                    # 实际上是属于千牛发送的自动排列的消息内容
                    sub_tp = js.get('template').get('id')
                    if sub_tp == 20002:
                        m.type = model_im.MESSAGE_CONTENT_TYPE_ATTACHMENT
                        deal = model_im.Deal()
                        deal.deal_id = js.get('header').get('degrade').get('alternative').split(':')[1]
                        m.extra_id = deal.deal_id
                        deal.description = '''title:{}\ncontent:{}'''.format(js.get('header').get('title'), js.get('header').get('degrade').get('alternative'))
                        deal.type = model_im.DEAL_TYPE_RECEIPT # may fix it later
                        self.im.db_insert_table_deal(deal)
                    elif sub_tp == 20013: # 快速入口
                        m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                        label_list = js.get('template').get('data').get('alist')
                        content = ''
                        for l in label_list:
                            content += '%s\n' % l.get('label')
                        m.content = content
                    elif sub_tp is None:
                        m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                        m.content = '''title:{}\ncontent:{}'''.format(js.get('header').get('title'), js.get('degrade').get('alternative'))
                except Exception as e:
                    self.log.m_err('detect wrong message content:{}\nidx:{}'.format(m_str, idx))
            elif tp == 112:
                    js = json.loads(m_str)
                    m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    m.content = js.get('title')
            elif tp == 113:
                    js = json.loads(m_str)
                    m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    m.content = '''file message name:{}, size:{}'''.format(js.get('nodeName'), js.get('nodeSize'))
            elif tp == 241:
                    js = json.loads(m_str)
                    js = json.loads(js.get('datas'))
                    m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    m.content = '''[product info]name:{}\nprice:{}\nsales:{}\npic:{}'''.format(js.get('name'), js.get('priceAsString'), js.get('salesCount'), js.get('picUrl'))
            else:
                self.log.m_print('detect unspport type:{}, index:{}'.format(tp, idx))
                self.log.m_print('raw string: %s' %m_str)
                m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                m.content = '''[unsupport type message] raw string:%s''' %m_str
            self.im.db_insert_table_message(m)
            idx += 1
        self.im.db_commit()

def parse_tb(root, extract_deleted, extract_source):
    #root = FileSystem.FromLocalDir(r"D:\ios_case\taobao\C0B97359-E334-4838-93F1-A40BC2A5DF0B")
    t = Taobao(root, extract_source, extract_deleted)
    t.search()
    for a in t.account:
        t.parse(a)
        t.parse_search(a)
    models = model_im.GenerateModel(t.cache + '/C37R').get_models()
    mlm = ModelListMerger()
    pr = ParserResults()
    pr.Categories = DescripCategories.QQ
    pr.Models.AddRange(list(mlm.GetUnique(models)))
    pr.Build("taobao")
    return pr


