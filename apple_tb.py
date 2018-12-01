#coding:utf-8
#   Author:C37R
#   脚本分析了淘宝app的账号、聊天、app日志、搜索和APP推荐内容
#   具体分析详见分析说明
#   
__author__ = "chenfeiyang"
import clr
clr.AddReference('System.Data.SQLite')
clr.AddReference('Base3264-UrlEncoder')
try:
    clr.AddReference('model_im')
    clr.AddReference('unity_c37r')
    clr.AddReference('model_eb')
except:
    pass
del clr

import System.Data.SQLite as sql
from PA_runtime import *
from System.Text import *
from System.IO import *
from System.Security.Cryptography import *
from System import Convert
from PA.InfraLib.Utils import PList
from PA.InfraLib.Extensions import PlistHelper

import datetime
import model_im
import os
import logging
import re
import unity_c37r
import json
import random
import model_eb
import traceback
#
# 淘宝多账号并不会删除账号的cache数据，因此，我们要建对象，而不是解析单个脚本
#
TB_VERSION = 1

class TbAccount(object):
    def __init__(self):
        self.uid = None   # 淘宝数字账号
        self.tb_id = None # 淘宝字符串账号（用户可见）
        self.tb_nick = None # 淘宝昵称
        self.photo = None
        self.is_from_avfs = False # 表明是否是从avfs中获取到的 这是为了防止重复插入account

class Taobao(object):
    def __init__(self, node, extract_source, extract_deleted):
        self.node = node
        self.extract_source = extract_source
        self.extract_deleted = extract_deleted
        self.cache = ds.OpenCachePath('taobao')
        self.hash = unity_c37r.md5(node.PathWithMountPoint)
        self.eb = model_eb.EB(self.cache + '/{}'.format(self.hash), TB_VERSION, u'淘宝')
        self.im = self.eb.im
        self.need_parse = self.eb.need_parse
        self.master_account = None # 从Documents/../TBSettingsUserInfo.json(xml) 中获取        
        if self.need_parse:
            self.eb.db_create()
            self.get_master_account()
        self.account = list()
        self.log = self.eb.log
        self.message_dict = dict() # 放关于message对应的表
        self.avfs_message_dict = dict()
        self.log.set_level(1) # 开启双重print

    def get_master_account(self):
        node = self.node.GetByPath('Documents/TBSUserInfo/TBSettingsUserInfo.json')
        if node is None:
            return
        p = PlistHelper.ReadPlist(node)
        if p is not None:
            try:
                self.master_account = p['userId'].ToString()
            except:
                traceback.print_exc()
                return

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
        if len(self.account) == 0:
            print('try to load from filesystem')
            node = self.node.GetByPath('Documents/TBDocuments/userInfo')
            if node is None:
                print('failed')
                return
            p = PlistHelper.ReadPlist(node)
            if p is not None:
                try:
                    ac = TbAccount()
                    ac.uid = p['userId'].ToString()
                    ac.tb_nick = p['nick'].ToString()
                    ac.is_from_avfs = True
                    self.account.append(ac)
                except:
                    traceback.print_exc()
                    return
        self.log.m_print('total find %d account(s)' % len(self.account))
        try:
            self.prepare_message_dict()
            self.try_get_avfs_message_dict()
        except Exception as e:
            traceback.print_exc()
            self.log.m_err('get message_dict failed!')

    def try_get_avfs_message_dict(self):
        avfs_dir_node = self.node.GetByPath('Documents/AVFSStorage')
        if avfs_dir_node is None:
            return
        avfs_path = avfs_dir_node.PathWithMountPoint
        pl = os.listdir(avfs_path)
        for f in pl:
            avfs_sqlite_node = self.node.GetByPath('Documents/AVFSStorage/{}/avfs.sqlite'.format(f))
            if avfs_sqlite_node is None:
                continue
            conn = unity_c37r.create_connection_tentatively(avfs_sqlite_node)
            cmd = sql.SQLiteCommand(conn)
            cmd.CommandText = '''
                select tbl_name from sqlite_master where tbl_name = 'MPMProfileim_cc' and type = 'table'
            '''
            reader = cmd.ExecuteReader()
            if not reader.Read():
                reader.Close()
                cmd.Dispose()
                conn.Close()
                continue
            reader.Close()
            cmd.CommandText = '''
                select targetId from MPMProfileim_cc where name is not null
            '''
            reader = cmd.ExecuteReader()
            if reader.Read():
                uid = unity_c37r.c_sharp_get_string(reader, 0)
                self.avfs_message_dict[uid] = avfs_sqlite_node
            reader.Close()
            cmd.Dispose()
            conn.Close()


    def prepare_message_dict(self):
        db_node = self.node.GetByPath('Library/Caches/YWDB')
        if db_node is None:
            return
        p_l = os.listdir(db_node.PathWithMountPoint)
        db_dirs = list()
        for p in p_l:
            res = re.search('WXOPENIMSDKDB(.*)', p, re.I | re.M)
            if res is not None:
                db_dirs.append(p)
        for d in db_dirs:
            message_node = self.node.GetByPath('Library/Caches/YWDB/%s/message.db' %d)
            if message_node is not None:
                path = unity_c37r.check_sqlite_maturity(message_node, self.cache)
                conn = unity_c37r.create_connection(path)
                cmd = sql.SQLiteCommand(conn)
                cmd.CommandText = '''
                    select ZUSER_ID from ZUSERINFO
                '''
                reader = cmd.ExecuteReader() 
                if reader.Read():
                    tb_id = unity_c37r.c_sharp_get_blob(reader, 0).decode('utf-8') #cntaobaotb5057305_11
                    tb_id = re.search('cntaobao(.*)', tb_id, re.I | re.M).group(1)
                    self.message_dict[tb_id] = unity_c37r.check_sqlite_maturity(message_node, self.cache)
                cmd.Dispose()
                conn.Close()

    def parse_prefer_file_cache(self):
        #这些cache均是master account的
        dir_node = self.node.GetByPath('Documents/wxstorage')
        if dir_node is None:
            return
        abs_path = dir_node.PathWithMountPoint
        relative_path = dir_node.AbsolutePath
        fl = os.listdir(abs_path)
        md5_fl = list()
        for f in fl:
            try:
                if f.__contains__('.'):
                    continue
                else:
                    md5_fl.append(f)
            except:
                pass
        fl = list()
        for ft in md5_fl:
            f = open(os.path.join(abs_path, ft), 'r')
            try:
                js = json.loads(f.read())
                f.close()
                #现在暂时只分析这两种文件其余的暂不支持（主要是类似于微信公众号的东西，意义不太大。）
                if js.__contains__('recommedResult'):
                    self.log.m_print('got file:%s' % ft)
                    il = js.get('recommedResult')
                    for it in il:
                        itmsl = it.get('itemList')
                        for iter in itmsl:
                            try:
                                p = model_eb.EBProduct()
                                p.set_value_with_idx(p.product_id, iter.get('itemId'))
                                p.set_value_with_idx(p.url, iter.get('targetUrl'))
                                p.set_value_with_idx(p.product_name, iter.get('title'))
                                p.set_value_with_idx(p.price, iter.get('marketPrice'))
                                p.set_value_with_idx(p.source, model_eb.EB_PRODUCT_BROWSE)
                                p.set_value_with_idx(p.source_file, os.path.join(relative_path, ft))
                                self.eb.db_insert_table_product(p.get_value())
                            except:
                                traceback.print_exc()
                                continue
                    self.eb.db_commit()
                    continue
                if js.__contains__('storageLists'):
                    self.log.m_print('got shop file %s' %ft)
                    il = js.get('storageLists')[0]
                    il = il.get('goodsList')
                    for it in il:
                        p = model_eb.EBProduct()
                        p.set_value_with_idx(p.shop_id, it.get('shid'))
                        p.set_value_with_idx(p.product_name, it.get('name'))
                        p.set_value_with_idx(p.url, it.get('url'))
                        p.set_value_with_idx(p.price, it.get('price'))
                        p.set_value_with_idx(p.source, model_eb.EB_PRODUCT_BROWSE)
                        p.set_value_with_idx(p.source_file, os.path.join(relative_path, ft))
                        self.eb.db_insert_table_product(p.get_value())
                    self.eb.db_commit()
                    continue      
            except:
                traceback.print_exc()
                continue
                               
    def parse_search(self, ac):
        db_node = self.node.GetByPath('Library/edge_compute.db')
        if db_node is None:
            return
        path = unity_c37r.check_sqlite_maturity(db_node, self.cache)
        conn = unity_c37r.create_connection(path)
        cmd = sql.SQLiteCommand(conn)
        cmd.CommandText = '''
            select args,create_time from usertrack where owner_id = {} and page_name = 'Page_SearchItemList'
        '''.format(ac.uid)
        reader = cmd.ExecuteReader()
        while reader.Read():
            if canceller.IsCancellationRequested:
                cmd.Dispose()
                conn.Close()
                self.im.db_close()
                raise IOError('E')
            s = model_im.Search()
            s.account_id = ac.uid
            m_str = unity_c37r.c_sharp_get_string(reader, 0)
            try:
                #s.key = re.search('keyword=(.*?),', m_str, re.I | re.M).group(1)
                match =  re.search('keyword=(.*?),', m_str, re.I | re.M)
                if match is None:
                    continue
                s.key = match.group(1)
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
        
    def parse_avfs(self, ac):
        #ac = TbAccount()
        db_node = self.avfs_message_dict.get(str(ac.uid))
        if db_node is None:
            return
        path = unity_c37r.check_sqlite_maturity(db_node, self.cache)
        conn = unity_c37r.create_connection_tentatively(path)
        cmd = sql.SQLiteCommand(conn)
        if ac.is_from_avfs:
            cmd.CommandText = '''
                select name, extInfo, deleteStatus, signature, avatarURL, displayName, targetId from MPMProfileim_cc where targetId = {}
            '''.format(ac.uid)
            reader = cmd.ExecuteReader()
            if reader.Read():
                a = model_im.Account()
                a.account_id = unity_c37r.c_sharp_get_string(reader, 6)
                a.nickname = unity_c37r.c_sharp_get_string(reader, 0)
                a.username = a.nickname
                a.signature = unity_c37r.c_sharp_get_string(reader, 3)
                a.photo = unity_c37r.c_sharp_get_string(reader, 4)
                self.im.db_insert_table_account(a)
            reader.Close()
        cmd.CommandText = '''
            select name, extInfo, deleteStatus, signature, avatarURL, displayName, targetId from MPMProfileim_cc where targetId != {}
        '''.format(ac.uid)
        reader = cmd.ExecuteReader()
        f_dict = dict()
        while reader.Read():
            f = model_im.Friend()
            f.nickname = unity_c37r.c_sharp_get_string(reader, 0)
            f.account_id = ac.uid
            f.friend_id = unity_c37r.c_sharp_get_string(reader, 6)
            f.signature = unity_c37r.c_sharp_get_string(reader, 3)
            f.remark = f.nickname
            f.deleted = unity_c37r.c_sharp_get_long(reader, 2)
            f.photo = unity_c37r.c_sharp_get_string(reader, 4)
            f_dict[f.friend_id] = f
            self.eb.im.db_insert_table_friend(f)
        reader.Close()
        cmd.CommandText = '''
            select name, displayName, targetId, avatarURL from MPMProfileim_bc where name != '{}'
        '''.format(ac.tb_id)
        reader = cmd.ExecuteReader()
        #TODO add pic/tb_id when parse avfs message dict
        while reader.Read():
            f = model_im.Friend()
            f.account_id = ac.uid
            f.nickname = unity_c37r.c_sharp_get_string(reader, 0)
            f.friend_id = unity_c37r.c_sharp_get_string(reader, 2)
            f.remark = unity_c37r.c_sharp_get_string(reader, 1)
            f.photo = unity_c37r.c_sharp_get_string(reader, 3)
            f_dict[f.friend_id] = f
            self.eb.im.db_insert_table_friend(f)
        reader.Close()
        #c2c messages
        cmd.CommandText = '''
            select  convTargetId, messageID, summary, senderId, receiverId, messageTime from MPMessageim_cc
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            m = model_im.Message()
            m.account_id = ac.uid
            m.sender_id = unity_c37r.c_sharp_get_string(reader, 3)
            m.is_sender = 1 if unity_c37r.c_sharp_get_string(reader, 4) == m.sender_id else 0
            m.msg_id = unity_c37r.c_sharp_get_string(reader, 1)
            m.talker_id = unity_c37r.c_sharp_get_string(reader, 0)
            m.content = unity_c37r.c_sharp_get_string(reader, 2)
            m.send_time = unity_c37r.c_sharp_get_long(reader, 5) / 1000
            m.talker_name = f_dict[m.talker_id].nickname
            m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
            self.im.db_insert_table_message(m)
        reader.Close()
        #b2c messages
        cmd.CommandText = '''
            select convTargetId, messageID, summary, senderId, receiverId, messageTime from MPMessageim_bc
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            m = model_im.Message()
            m.account_id = ac.uid
            m.sender_id = unity_c37r.c_sharp_get_string(reader, 3)
            m.talker_id = unity_c37r.c_sharp_get_string(reader, 0)
            m.is_sender = 0 if m.sender_id.__contains__(m.talker_id) else 1
            if m.is_sender == 1:
                m.sender_id = ac.uid # 如果是自己发的，则替换对应的ID
            m.send_time = unity_c37r.c_sharp_get_long(reader, 5) / 1000
            m.talker_name = f_dict[m.talker_id].nickname
            m.content = unity_c37r.c_sharp_get_string(reader, 2)
            m.msg_id = unity_c37r.c_sharp_get_string(reader, 1)
            m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
            self.im.db_insert_table_message(m)
        reader.Close()
        cmd.Dispose()
        conn.Close()
        self.im.db_commit()

    def parse(self, ac):
        #ac = TbAccount()
        self.parse_avfs(ac)
        db_node = self.node.GetByPath('Library/Caches/amps3_{}.db'.format(ac.uid))
        if db_node is None:
            self.log.m_print('get db node failed, parse exists!')
            return
        #path = db_node.PathWithMountPoint
        #update sqlite-checking.
        path = unity_c37r.check_sqlite_maturity(db_node, self.cache)
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
            if canceller.IsCancellationRequested:
                cmd.Dispose()
                conn.Close()
                self.im.db_close()
                raise IOError('E')
            f = model_im.Friend()
            f.account_id = ac.uid # Fix error
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
                #update sqlite connection
                pth = unity_c37r.check_sqlite_maturity(log_node, self.cache)
                conn = unity_c37r.create_connection(pth)
                cmd = sql.SQLiteCommand(conn)
                cmd.CommandText = '''
                    select id, operation_id, record, logtime, result from Record
                '''
                reader = cmd.ExecuteReader()
        while reader is not None and reader.Read():
            if canceller.IsCancellationRequested:
                cmd.Dispose()
                conn.Close()
                raise IOError('E')
            try:
                logs = model_eb.EBLog()
                m_str = unity_c37r.c_sharp_get_string(reader, 1)
                m_sl = m_str.split('|')
                sender = m_sl[1]
                reciever = m_sl[2]
                if reciever == 'wwLogin':
                    sender = re.search(r'\(null\)(.*)', sender, re.I | re.M).group(1)
                    desc = '''{} try to login'''.format(sender)
                    logs.set_value_with_idx(logs.description, desc)
                else:
                    sender = re.search('cnhhupan(.*)', sender, re.I | re.M).group(1)
                    reciever = re.search('cnhhupan(.*)', reciever, re.I | re.M).group(1)
                    desc = '''{} try to send message to {}'''.format(sender, reciever)
                    logs.set_value_with_idx(logs.description, desc)
                m_str = unity_c37r.c_sharp_get_string(reader, 2)
                js = json.loads(m_str)
                content = js.get('title')
                logs.set_value_with_idx(logs.content, content)
                log_time = unity_c37r.c_sharp_get_long(reader, 3) / 1000
                logs.set_value_with_idx(logs.time, log_time)
                result = unity_c37r.c_sharp_get_long(reader, 4)
                logs.set_value_with_idx(logs.result, result)
                self.eb.db_insert_table_log(logs.get_value())
            except Exception as e:
                traceback.print_exc()
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
            if canceller.IsCancellationRequested:
                cmd.Dispose()
                conn.Close()
                self.im.db_close()
                raise IOError('e')
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
            if canceller.IsCancellationRequested:
                cmd.Dispose()
                conn.Close()
                self.im.db_close()
                raise IOError('e')
            m = model_im.Message()
            m.account_id = ac.uid
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
                    sub_string = js.get('template').get('data').get('text')
                    # f = open('D:/webs/{}.xml'.format(random.randint(0, 0xffffffff)), 'w+')
                    # if sub_string is not None:
                    #     f.write(sub_string)
                    # f.close()
                    if sub_tp == 20002:
                        m.type = model_im.MESSAGE_CONTENT_TYPE_ATTACHMENT
                        deal = model_im.Deal()
                        trade = model_eb.EBDeal()
                        alter_string = js.get('header').get('degrade').get('alternative')
                        if alter_string.__contains__(':'):
                            deal.deal_id = alter_string.split(':')[1]
                        m.extra_id = deal.deal_id
                        deal.description = '''title:{}\ncontent:{}'''.format(js.get('header').get('title'), js.get('header').get('degrade').get('alternative'))
                        trade.set_value_with_idx(trade.desc, deal.description)
                        deal.type = model_im.DEAL_TYPE_RECEIPT # may fix it later
                        trade.set_value_with_idx(trade.deal_type, model_eb.TRADE_PAY)
                        #trade.set_value_with_idx(trade.status, model_eb.EBDEAL)
                        self.im.db_insert_table_deal(deal)
                        self.eb.db_insert_table_deal(trade.get_value())
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
                    m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    m.content = u'解析失败,原始内容:' + m_str
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
    #
    #   购物车
    #
    def parse_shop_cart(self):
        node = self.node.GetByPath('Documents/AVFSStorage/carts/avfs.sqlite')
        if node is None:
            self.log.m_print('no shop carts item cached...')
            return
        path = unity_c37r.check_sqlite_maturity(node, self.cache)
        conn = unity_c37r.create_connection(path)
        cmd = sql.SQLiteCommand(conn)
        cmd.CommandText = '''
            select filename from AVFS_FIlE_INDEX_TABLE
        '''
        reader = cmd.ExecuteReader()
        r = list()
        while reader.Read():
            r.append(unity_c37r.c_sharp_get_string(reader, 0))
        cmd.Dispose()
        reader.Close()
        conn.Close()
        for f in r:
            f_node = self.node.GetByPath('Documents/AVFSStorage/carts/files/%s' % f)
            abs_path = f_node.AbsolutePath
            if f_node is None:
                continue
            try:
                m_node = BPReader.GetTree(f_node)
                m_str = unity_c37r.get_btree_node_str(m_node, "body", '')
                js = json.loads(m_str)
                obj = js.get('data').get('data')
                for k in obj.keys():
                    grp = re.search('itemv2_', k, re.I | re.M)
                    if grp is not None:
                        itm = obj.get(k)
                        p = model_eb.EBProduct()
                        itm = itm.get('fields')
                        p.set_value_with_idx(p.account_id, self.master_account)
                        p.set_value_with_idx(p.url, itm.get('pic'))
                        p.set_value_with_idx(p.product_id, itm.get('itemId'))
                        p.set_value_with_idx(p.product_name, itm.get('title'))
                        p.set_value_with_idx(p.shop_id, itm.get('shopId'))
                        p.set_value_with_idx(p.source, model_eb.EB_PRODUCT_SHOPCART)
                        p.set_value_with_idx(p.source_file, abs_path)
                        its = itm.get('pay')
                        if its is None:
                            continue
                        money_str = itm.get('pay').get('nowTitle')
                        grp = re.search(u'￥(.*)', money_str, re.I | re.M)
                        p.set_value_with_idx(p.price, float(grp.group(1)))
                        self.eb.db_insert_table_product(p.get_value())
                        continue
                    grp = re.search('shopv2_', k, re.I | re.M)
                    if grp is not None:
                        shop = model_eb.EBShop()
                        itm = obj.get(k)
                        shop.set_value_with_idx(shop.account_id, self.master_account)
                        shop.set_value_with_idx(shop.shop_id, itm.get('id'))
                        shop.set_value_with_idx(shop.shop_name, itm.get('fields').get('title'))
                        shop.set_value_with_idx(shop.boss_id, itm.get('fields').get('sellerId'))
                        shop.set_value_with_idx(shop.boss_nick, itm.get('fields').get('seller'))
                        shop.set_value_with_idx(shop.source_file, abs_path)
                        self.eb.db_insert_table_shop(shop.get_value())
                        continue
            except:
                # f = open('D:/webs/{}.txt'.format(random.randint(0, 0xffffffff)),'w+')
                # f.write(m_str)
                # f.close()
                traceback.print_exc()
                continue
        self.eb.db_commit()

def judge_node(node):
    root = node.Parent.Parent.Parent
    sub_node = root.GetByPath('Documents')
    #防止命中到group
    if sub_node is None:
        return None
    else:
        return root

def parse_tb(root, extract_source, extract_deleted):
    try:
        root = judge_node(root)
        if root is None:
            raise IOError('E')
        t = Taobao(root, extract_source, extract_deleted)
        if t.need_parse:
            t.search()
            for a in t.account:
                t.parse(a)
                t.parse_search(a)
                t.parse_prefer_file_cache()
                t.parse_shop_cart()
            t.eb.db_insert_table_version(model_eb.EB_VERSION_KEY, model_eb.EB_VERSION_VALUE)
            t.eb.db_insert_table_version(model_eb.EB_APP_VERSION_KEY, TB_VERSION)
            t.eb.db_commit()
            t.eb.sync_im_version()
        #models = model_im.GenerateModel(t.cache + '/C37R').get_models()
        #models = model_eb.GenerateModel(t.cache + '/C37R').get_models()
        models = model_eb.GenerateModel(t.cache + '/{}'.format(t.hash)).get_models()
        mlm = ModelListMerger()
        pr = ParserResults()
        pr.Categories = DescripCategories.Taobao
        pr.Models.AddRange(list(mlm.GetUnique(models)))
        pr.Build("淘宝")
    except:
        traceback.print_exc()
        pr = ParserResults()
    return pr


