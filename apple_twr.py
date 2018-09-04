#coding:utf-8
import clr
clr.AddReference('System.Data.SQLite')
del clr

import System.Data.SQLite as sql

import model_im
import os
import sys
from PA_runtime import *
import PA
from System.Text import *
from System.IO import *
import logging
import re
import unity_c37r
#####################Get Functions######################################
def GetString(reader, idx):
    return reader.GetString(idx) if not reader.IsDBNull(idx) else ""

def GetInt64(reader, idx):
    return reader.GetInt64(idx) if not reader.IsDBNull(idx) else 0

def GetBlob(reader, idx):
    return reader.GetValue(idx) if not reader.IsDBNull(idx) else None

def GetReal(reader, idx):
    return reader.GetDouble(idx) if not reader.IsDBNull(idx) else 0.0

def _db_record_get_value(record, column, default_value=None):
    if not record[column].IsDBNull:
        return record[column].Value
    return default_value

class TIphone(object):
    def __init__(self, root, extract_deleted, extract_source):
        self.node = root
        self.es = extract_source
        self.ed = extract_deleted
        self.account = list()
        self.friends = list()
        self.messages = list()
        self.blogs = list()
        self.search = list()
        cache = ds.OpenCachePath('Twitter')
        self.cache = cache
        if not os.path.exists(cache):
            os.mkdir(cache)
        #self.mb = microblog_base.BlogBase(env)
        self.im = model_im.IM()
        self.im.db_create(cache + '/C37R')
        
    @staticmethod
    def check_account_id(account_str):
        res = re.search("(.*)-(.*)", account_str, re.I | re.M)
        if res is None:
            return ""
        return res.group(1)
    
    @staticmethod
    def check_app_account_id(fname, account_id):
        '''
            something like :
            app.acct.ecxAkAKUaUgeimn-531809610917628.detail.11
            app.acct.()
        '''
        res = re.search('app\\.acct\\.{}-(.*)\\.detail\\.11'.format(account_id), fname, re.I | re.M)
        if res is not None:
            return True
        return False
        
    def parse_account(self):
        try:
            preference_node = self.node.GetByPath(r"/Library/Preferences/com.atebits.Tweetie2.plist")
            ana_res = BPReader(preference_node.Data).top
            m_d = ana_res['TFNTwitterHomeTimelineSerializedAccountCursorsKey']
            for key in m_d.Keys:
                print(key)
                r = self.check_account_id(key)
                if r is not "" and not self.account.__contains__(r):
                    self.account.append(r)
        except Exception as e:
            logging.error(e)
            print('check from file failed, try to load from folder info....')
        # add it later
        try:
            pass
        except Exception as e:
            logging.error(e)
            print('found account failed!!!')
            return list()
        self.parse()

    def deserialize_message(self, bp_arr, aid, src_file, account_id):
        try:
            sz = bp_arr.Length
            idx = 0
            ck = ''
            message_list = list()
            message_dict = dict()
            while idx < sz:
                v = bp_arr[idx]
                if type(v) is PA.Formats.Apple.BPListTypes.BPDict:
                    siter = v['local_last_read_event_id'].Value if v['local_last_read_event_id'] is not None else None
                    if siter is not None:
                        while type(bp_arr[idx + 1].Value) is not str:
                            idx += 1
                        idx += 1
                        tck = bp_arr[idx].Value
                        if ck is not None and ck != '':
                            if message_dict.__contains__(ck):
                                message_dict[ck].append(message_list)
                            else:
                                message_dict[ck] = message_list
                        message_list = list() # create new....
                        ck = tck

                    siter = v['userID'].Value if v['userID'] is not None else None
                    if siter is not None:
                        usr = dict()
                        usr['uid'] = siter
                        while type(bp_arr[idx + 1].Value) is not str:
                            idx += 1
                        idx += 1
                        usr['aid'] = bp_arr[idx].Value
                        usr['nick'] = bp_arr[idx + 1].Value
                        idx += 2
                        # passed...
                    #siter = None
                    #sub_dict = v['quickReplyRequest'].Value
                    siter = v['primarySortKey'].Value if v['primarySortKey'] is not None else None
                    if siter is not None:
                        chat = {'pkey': siter}
                        while type(bp_arr[idx + 1].Value) is not str:
                            #tp = type(bp_arr[idx + 1].Value)
                            idx += 1
                            if type(bp_arr[idx]) is PA.Formats.Apple.BPListTypes.BPDict:
                                smap = bp_arr[idx]
                                cid = smap['canonicalID'].Value if smap['canonicalID'] is not None else None
                                if cid is not None and cid is not '':
                                    chat['cid'] = cid
                                tm = smap['NS.time'].Value if smap['NS.time'] is not None else None
                                if tm is not None and tm is not '':
                                    chat['time'] = tm
                        idx += 1
                        chat['text'] = bp_arr[idx].Value
                        message_list.append(chat)
                idx += 1
            if ck is not None and ck != '':
                if message_dict.__contains__(ck):
                    message_dict[ck].append(message_list)
                else:
                    message_dict[ck] = message_list
            # to messages...
            for k in message_dict:
                l = message_dict[k]
                uids = k.split('-')
                objid = uids[0] if uids[0] != aid else uids[1]
                for m in l:
                    #msg = microblog_base.MicroBlogMessage()
                    msg = model_im.Message()
                    msg.account_id = account_id
                    msg.content = m['text']
                    msg.send_time = unity_c37r.format_mac_timestamp(m['time'])
                    msg.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    msg.sender_id = objid if m['pkey'] == m['cid'] else aid
                    msg.is_sender = 1 if msg.sender_id == aid else 0
                    msg.talker_id = k
                    msg.source = src_file
                    self.im.db_insert_table_message(msg)
        except Exception as e:
            logging.error(e)
            print('decode message plist failed, data is skipped...')
            pass
        pass

    def parse(self):
        for aid in self.account:
            m_dir = self.node.PathWithMountPoint
            m_dir = os.path.join(m_dir,"Library/Caches/databases")
            dl = os.listdir(m_dir)
            a_dir = ""
            for sub_dir in dl:
                if sub_dir.__contains__(aid):
                    a_dir = sub_dir
                    break
            if a_dir == "":
                continue
            db_id = os.listdir(os.path.join(m_dir, a_dir))[0] # not with full path....
            m_sql = "/Library/Caches/databases/{}/{}/twitter.db".format(a_dir, db_id)
            sub_node = self.node.GetByPath(m_sql)
            if sub_node is None:
                print('fatal error!')
                continue
            r_sql = sql.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(sub_node.PathWithMountPoint))
            r_sql.Open()
            cmd = sql.SQLiteCommand(r_sql)
            cmd.CommandText = '''
                select id, screenName, name, location, description from Users where screenName = '{}'
            '''.format(aid)
            reader = cmd.ExecuteReader()
            reader.Read()
            a = model_im.Account()
            a.account_id = GetInt64(reader, 0)
            a.username = aid
            current_id = GetInt64(reader, 0)
            a.photo = ""
            a.nickname = GetString(reader, 2)
            a.address = GetString(reader, 3)
            a.signature = GetString(reader, 4)
            self.im.db_insert_table_account(a)
            cmd.Dispose()
            cmd.CommandText = '''
                select id, screenName, name, location, description from Users where screenName != '{}'
            '''.format(aid)
            reader = cmd.ExecuteReader()
            while reader.Read():
                    f = model_im.Friend()
                    f.account_id = current_id
                    f.friend_id = GetInt64(reader, 0)
                    f.remark = GetString(reader, 1)
                    f.address = GetString(reader, 3)
                    f.nickname = GetString(reader, 2)
                    f.signature = GetString(reader, 4)
                    self.im.db_insert_table_friend(f)
            self.im.db_commit()
            cmd.Dispose()
            cmd.CommandText = '''
                select userId, text, date from Statuses
            '''
            reader = cmd.ExecuteReader()
            while reader.Read():
                blog = model_im.Feed()
                blog.account_id = current_id
                blog.content = GetString(reader, 1)
                #blog.send_time = unity_c37r.format_mac_timestamp(GetReal(reader, 2))
                blog.send_time = int(GetReal(reader, 2))
                blog.sender_id = GetInt64(reader, 0)
                blog.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                self.im.db_insert_table_feed(blog)
            self.im.db_commit()
            root_path = self.node.PathWithMountPoint
            m_path = os.path.join(root_path, 'Documents/com.atebits.tweetie.application-state')
            talk_file = None
            dirs = os.listdir(m_path)
            for d in dirs:
                res = self.check_app_account_id(d, aid)
                if not res:
                    continue
                talk_file = d
                talk_node = self.node.GetByPath('/Documents/com.atebits.tweetie.application-state/{}'.format(talk_file))
                talk_file = talk_node.PathWithMountPoint # absolute path
                print(talk_node.Size)
                bp = BPReader(talk_node.Data).top
                bp_arr = bp['$objects']
                self.deserialize_message(bp_arr, aid, talk_file, current_id)
            self.im.db_commit()

def analyse_twitter(root, extract_deleted, extract_source):
    #node = FileSystem.FromLocalDir(r'D:\ios_case\tweet\tw')
    node = root
    t = TIphone(node, extract_deleted, extract_source)
    t.parse_account()
    t.im.db_close()
    models = model_im.GenerateModel(t.cache + "/C37R", root.PathWithMountPoint).get_models()
    mlm = ModelListMerger()
    pr = ParserResults()
    pr.Categories = DescripCategories.QQ
    pr.Models.AddRange(list(mlm.GetUnique(models)))
    pr.Build('Twitter')
    return pr
    