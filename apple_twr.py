#coding:utf-8
import microblog_base
import os
import sys
from PA_runtime import *
import PA
from System.Text import *
from System.IO import *
import logging
import re


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
        if not os.path.exists(cache):
            os.mkdir(cache)
        version = 1
        env = dict()
        env['cache'] = cache
        env['version'] = version
        self.mb = microblog_base.BlogBase(env)
        
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

    def deserialize_message(self, bp_arr, aid, src_file):
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
                    msg = microblog_base.MicroBlogMessage()
                    msg.account_id = aid
                    msg.content = m['text']
                    msg.timestamp = int(m['time'])
                    msg.media = 1
                    msg.sender_id = objid if m['pkey'] == m['cid'] else aid
                    msg.is_sender = 1 if msg.sender_id == aid else 0
                    msg.talk_id = k
                    msg.source_file = src_file
                    t = msg.generate_sqlite_turple()
                    self.mb.insert_messages(t)
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
            
            #
            # sqlite ---> sqlite_parser
            #
            m_sql = "/Library/Caches/databases/{}/{}/twitter.db".format(a_dir, db_id)
            sub_node = self.node.GetByPath(m_sql)
            # create sqlite
            r_sql = SQLiteParser.Database.FromNode(sub_node)
            ts = SQLiteParser.TableSignature("Users")
            result = r_sql.ReadTableRecords(ts, False)
            friend_dict = dict()
            for rec in result:
                if aid == rec['screenName'].Value:
                    reg_account = microblog_base.MicroBlogAccount()
                    reg_account.user_id = str(rec['id'].Value)
                    reg_account.account_id = aid
                    reg_account.icon = "" # we need to analysing photo data, check save one for deserialization... keep in record...
                    reg_account.nick_name = _db_record_get_value(rec, "name")
                    reg_account.area_string = _db_record_get_value(rec, "location")
                    reg_account.description = _db_record_get_value(rec, 'description')
                    reg_account.noticed_quantity = _db_record_get_value(rec, 'followingCount')
                    reg_account.blog_quantity = _db_record_get_value(rec, 'statusesCount')
                    reg_account.fans_quantity = _db_record_get_value(rec, 'followersCount')
                    reg_account.source_file = sub_node.PathWithMountPoint
                    self.mb.insert_account(reg_account.generate_sqlite_turple())
                else:
                    friend = microblog_base.MricoBlogFriends()
                    friend.account = aid
                    friend.account_id = _db_record_get_value(rec,'screenName')
                    friend.user_id = str(rec['id'].Value) # note this is string this must not be null!
                    friend.nick_name = _db_record_get_value(rec, 'name')
                    friend.area_string = _db_record_get_value(rec, 'location')
                    friend.description = _db_record_get_value(rec, 'description')
                    friend.reg_time = _db_record_get_value(rec, 'createdDate')
                    friend.update_time = int(_db_record_get_value(rec, 'updatedAt', 0) * 100000) / 100000 # as it may be float data
                    friend.fan_quantity = _db_record_get_value(rec, 'followersCount')
                    friend.follow_quantity = _db_record_get_value(rec, 'followingCount')
                    friend.blog_quantity = _db_record_get_value(rec, 'statusesCount')
                    friend.home_page = _db_record_get_value(rec, 'url')
                    friend.source_file = sub_node.PathWithMountPoint
                    self.mb.insert_friends(friend.generate_sqlite_turple())
                    friend_dict[friend.account_id] = friend # insert into dict, we serialize it later....
            
            ts = SQLiteParser.TableSignature("Statuses")
            record = r_sql.ReadTableRecords(ts, False)
            for r in record:
                blog = microblog_base.MircorBlogBlogs()
                blog.account = aid
                blog.sender = _db_record_get_value(r, "userId")
                blog.content = _db_record_get_value(r, "text")
                blog.blog_id = _db_record_get_value(r, "id")
                blog.send_time = _db_record_get_value(r, "date")
                blog.comment_quantity = _db_record_get_value(r, "replyCount")
                blog.retweet_quantity = _db_record_get_value(r, "retweetCount")
                blog.like_quantity = _db_record_get_value(r, "favoriteCount")
                if friend_dict.__contains__(blog.sender):
                    blog.sender_name = friend_dict[blog.sender].nick_name
                blog.source_file = sub_node.PathWithMountPoint
                self.mb.insert_blogs(blog.generate_sqlite_turple())
                # url may be important, but.... suit your self.
                pass
            self.mb.on_commit() 
            # messages decription...
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
                #bp = BPReader(talk_node.Data).top
                print(talk_node.Size)
                bp = BPReader(talk_node.Data).top
                bp_arr = bp['$objects']
                self.deserialize_message(bp_arr, aid, talk_file)
                self.mb.on_commit()
            #self.models.append(self.decode_account(bp['$objects'], acc_ind.Value))
            # this code can not run with real-data
            pass

class TAndroid(object):
    def __init__(self, root, extract_deleted, extract_source):
        self.node = root
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        cache = ds.OpenCache('TwitterAndroid')
        if not os.path.exists(cache):
            os.mkdir(cache)
        env = dict()
        version = 1
        env['version'] = version
        env['cache'] = cache
        self.mb = microblog_base.BlogBase(env)
        self.account = list()

    def searh_account(self):
        pref_node = self.node.GetByPath('/databases')
        dir_path = pref_node.PathWithMountPoint
        dirs = os.listdir(dir_path)
        for d in dirs:
            try:
                ret_v = int(d)
                # 0 暂时不分析
                if ret_v == 0:
                    continue
                if self.account.__contains__(ret_v):
                    continue
                self.account.append(d)
            except ValueError as e:
                continue
        # db_node = self.node.GetByPath('/databases')
        # dir_path = db_node.PathWithMountPoint
        # some file...
        for aid in self.account:

            pass
    pass

def analyse_twitter(root, extract_deleted, extract_source):
    t = TIphone(root, extract_deleted, extract_source)
    t.parse_account()
    pass
    