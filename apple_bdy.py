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
    clr.AddReference('model_nd')
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

import model_im
import os
import re
import unity_c37r
import json
import random
import traceback
import model_nd
BDY_APP_VERSION = 1
class BDNetDisk(object):
    def __init__(self, node, extract_deleted, extract_source):
        self.cache = ds.OpenCachePath('baidunetdisk')
        if not os.path.exists(self.cache):
            os.mkdir(self.cache)
        self.hash = unity_c37r.md5(node.PathWithMountPoint)
        self.nd = model_nd.NetDisk(self.cache + '/{}'.format(self.hash), BDY_APP_VERSION)
        self.need_parse = self.nd.need_parse
        if not self.need_parse:
            return
        self.node = node
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.account_folders = list()

    def search(self):
        doc_node = self.node.GetByPath('Documents')
        fl = os.listdir(doc_node.PathWithMountPoint)
        for f in fl:
            r = unity_c37r.is_md5(f)
            if r:
                self.account_folders.append(f)
    
    def parse(self, account_folder):
        f_node = self.node.GetByPath('Documents/{}/netdisk.sqlite'.format(account_folder))
        if f_node is None:
            return
        conn = unity_c37r.create_connection_tentatively(f_node)
        cmd = sql.SQLiteCommand(conn)
        cmd.CommandText = '''
            select uk, user_name, avatar_url from feed_userlist
        '''
        reader = cmd.ExecuteReader()
        current_account = None
        if reader.Read():
            a = model_im.Account()
            a.account_id = unity_c37r.c_sharp_get_string(reader, 0)
            current_account = a.account_id
            a.username = unity_c37r.c_sharp_get_string(reader, 1)
            a.nickname = a.username
            a.photo = unity_c37r.c_sharp_get_string(reader, 2)
            self.nd.im.db_insert_table_account(a)
        else:
            raise IOError('[BAIDU NETDISK] E: NO ACCOUNT INFORMATION!')
        reader.Close()
        cmd.CommandText = '''
            select fid, server_full_path, file_name, file_size, file_md5, ctime, mtime, atime from cachefilelist
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            f = model_nd.NDFileList()
            f.set_value_with_idx(f.account, current_account)
            f.set_value_with_idx(f.file_name, unity_c37r.c_sharp_get_string(reader, 2))
            f.set_value_with_idx(f.server_path, unity_c37r.c_sharp_get_string(reader, 1))
            f.set_value_with_idx(f.create_time, unity_c37r.c_sharp_try_get_time(reader, 5))
            f.set_value_with_idx(f.update_time, unity_c37r.c_sharp_try_get_time(reader, 6))
            f.set_value_with_idx(f.cache_time, unity_c37r.c_sharp_try_get_time(reader, 7))
            f.set_value_with_idx(f.file_hash, unity_c37r.c_sharp_get_string(reader, 4))
            f.set_value_with_idx(f.file_size, unity_c37r.c_sharp_get_long(reader, 3))
            self.nd.db_insert_filelist(f.get_values())
        reader.Close()
        self.nd.db_commit()
        cmd.CommandText = '''
            select file_name, server_full_path, file_size, blocklistmd5, trans_type, trans_status, ctime, mtime, atime, downloadlink, thumburl from transfilelist
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            f = model_nd.NDFileTransfer()
            f.set_value_with_idx(f.account, current_account)
            f.set_value_with_idx(f.file_name, unity_c37r.c_sharp_get_string(reader, 0))
            f.set_value_with_idx(f.server_path, unity_c37r.c_sharp_get_string(reader, 1))
            f.set_value_with_idx(f.file_size, unity_c37r.c_sharp_get_long(reader, 2))
            f.set_value_with_idx(f.hash_code, unity_c37r.c_sharp_get_string(reader, 3))
            f.set_value_with_idx(f.is_download, 1)
            st = unity_c37r.c_sharp_get_long(reader, 5)
            if st == 1:
                f.set_value_with_idx(f.status, model_nd.NDFileDone)
            else:
                f.set_value_with_idx(f.status, model_nd.NDFileProcessing)
            f.set_value_with_idx(f.url, unity_c37r.c_sharp_get_string(reader, 9))
            f.set_value_with_idx(f.begin_time, unity_c37r.c_sharp_try_get_time(reader, 8))
            f_name = f.get_value_with_idx(f.file_name)
            paddern = os.path.splitext(f_name)
            if len(paddern) > 1:
                paddern = paddern[1]
            else:
                paddern = ""
            r_fname = 'Documents/{0}/Cache/{1}{2}'.format(account_folder, f.get_value_with_idx(f.hash_code), paddern)
            s_node = self.node.GetByPath(r_fname)
            if s_node is not None:
                f.set_value_with_idx(f.local_path, s_node.AbsolutePath)
            self.nd.db_insert_transfer(f.get_values())
        reader.Close()
        cmd.CommandText = '''
            select server_full_path, file_md5, file_size, ctime, mtime from image_filelist
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            f = model_nd.NDFileList()
            f.set_value_with_idx(f.account, current_account)
            f.set_value_with_idx(f.file_name, unity_c37r.c_sharp_get_string(reader, 0))
            f.set_value_with_idx(f.server_path, f.get_value_with_idx(f.file_name))
            f.set_value_with_idx(f.create_time, unity_c37r.c_sharp_try_get_time(reader, 3))
            f.set_value_with_idx(f.update_time, unity_c37r.c_sharp_try_get_time(reader, 4))
            f.set_value_with_idx(f.file_hash, unity_c37r.c_sharp_get_string(reader, 1))
            self.nd.db_insert_filelist(f.get_values())
        self.nd.db_commit()
        reader.Close()
        cmd.CommandText = '''
            select from_uk, server_fullpath, server_filename, size, server_mtime, server_ctime, dlink, uname, msg_time from Mbox_groupfile_share
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            s = model_nd.NDFileShared()
            s.set_value_with_idx(s.account, current_account)
            s.set_value_with_idx(s.file_name, unity_c37r.c_sharp_get_string(reader, 2))
            s.set_value_with_idx(s.server_path, unity_c37r.c_sharp_get_string(reader, 1))
            s.set_value_with_idx(s.file_size, unity_c37r.c_sharp_get_long(reader, 3) )
            s.set_value_with_idx(s.sender_id, unity_c37r.c_sharp_get_string(reader, 0))
            s.set_value_with_idx(s.update_time, unity_c37r.c_sharp_try_get_time(reader, 4))
            s.set_value_with_idx(s.create_time, unity_c37r.c_sharp_try_get_time(reader, 5))
            s.set_value_with_idx(s.url, unity_c37r.c_sharp_get_string(reader, 6))
            s.set_value_with_idx(s.sender_name, unity_c37r.c_sharp_get_string(reader, 7))
            s.set_value_with_idx(s.send_time, unity_c37r.c_sharp_try_get_time(reader, 8))
            self.nd.db_insert_shared(s.get_values())
        reader.Close()
        self.nd.db_commit()
        cmd.CommandText = '''
            select gid, name, uname, uk, avatarurl, ctime from Mbox_group
        '''
        reader = cmd.ExecuteReader()
        grp_dict = dict()
        while reader.Read():
            grp = model_im.Chatroom()
            grp.account_id = current_account
            grp.chatroom_id = unity_c37r.c_sharp_get_string(reader, 0)
            grp.name = unity_c37r.c_sharp_get_string(reader, 1)
            grp.owner_id = unity_c37r.c_sharp_get_string(reader, 3)
            grp.photo = unity_c37r.c_sharp_get_string(reader, 4)
            grp.create_time = unity_c37r.c_sharp_try_get_time(reader, 5)
            grp_dict[grp.chatroom_id] = grp
            self.nd.im.db_insert_table_chatroom(grp)
        self.nd.im.db_commit()
        reader.Close()
        cmd.CommandText = '''
            select gid, ctime, uk, uname, nick_name, avatar_url from Mbox_group_member
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            gm = model_im.ChatroomMember()
            gm.account_id = current_account
            gm.chatroom_id = unity_c37r.c_sharp_get_string(reader, 0)
            gm.member_id = unity_c37r.c_sharp_get_string(reader, 2)
            gm.display_name = unity_c37r.c_sharp_get_string(reader, 3)
            gm.photo = unity_c37r.c_sharp_get_string(reader, 5)
            self.nd.im.db_insert_table_chatroom_member(gm)
        # add system message
        gm = model_im.ChatroomMember()
        gm.account_id = current_account
        gm.member_id = 0
        gm.display_name = u'系统消息'
        self.nd.im.db_insert_table_chatroom_member(gm)
        self.nd.im.db_commit()
        reader.Close()
        # group messages
        cmd.CommandText = '''
            select gid, msgid, msguk, time, content, username from mbox_groupmsg
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            m = model_im.Message()
            m.talker_id = unity_c37r.c_sharp_get_string(reader, 0)
            if grp_dict.__contains__(m.talker_id):
                m.talker_name = grp_dict[m.talker_id].name
            m.msg_id = unity_c37r.c_sharp_get_string(reader, 1)
            m.sender_id = unity_c37r.c_sharp_get_string(reader, 2)
            m.send_time = unity_c37r.c_sharp_try_get_time(reader, 3)
            m.content = unity_c37r.c_sharp_get_string(reader, 4)
            m.is_sender = 1 if current_account == m.sender_id else 0
            m.account_id = current_account
            m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
            self.nd.im.db_insert_table_message(m)
        self.nd.im.db_commit()
        reader.Close()
        cmd.CommandText = '''
            select uk, uname, avatarurl from mbox_newfriendunreadlist
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            f = model_im.Friend()
            f.account_id = current_account
            f.friend_id = unity_c37r.c_sharp_get_string(reader, 0)
            f.photo = unity_c37r.c_sharp_get_string(reader, 2)
            f.nickname = unity_c37r.c_sharp_get_string(reader, 1)
            self.nd.im.db_insert_table_friend(f)
        self.nd.im.db_commit()
        reader.Close()
        cmd.CommandText = '''
            select msgid, msguk, is_receive, time, content, username from mbox_msg
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            m = model_im.Message()
            m.is_sender = unity_c37r.c_sharp_get_long(reader, 2)
            m.msg_id = unity_c37r.c_sharp_get_string(reader, 0)
            m.talker_id = unity_c37r.c_sharp_get_string(reader, 1)
            if m.is_sender == 0:
                m.sender_id = m.talker_id
            else:
                m.sender_id = current_account
            m.account_id = current_account
            m.content = unity_c37r.c_sharp_get_string(reader, 4)
            m.send_time = unity_c37r.c_sharp_try_get_time(reader, 3)
            m.talker_name = unity_c37r.c_sharp_get_string(reader, 5)
            m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
            self.nd.im.db_insert_table_message(m)
        self.nd.im.db_commit()
        reader.Close()


def judge_node(node):
    node = node.Parent.Parent.Parent
    sub = node.GetByPath('Documents')
    if sub is None:
        return None
    return node

def parse_bdy(node, extract_source, extract_deleted):
    node = judge_node(node)
    try:
        if node is None:
            raise IOError('e')
        b = BDNetDisk(node, extract_deleted, extract_source)
        if b.need_parse:
            b.search()
            for f in b.account_folders:
                b.parse(f)
            b.nd.db_insert_version(model_nd.NDDBVersionKey, model_nd.NDDBVersionValue)
            b.nd.db_insert_version(model_nd.NDDBApplicationVersionKey, BDY_APP_VERSION)
            b.nd.db_insert_im_version(BDY_APP_VERSION)
            b.nd.db_commit()
            b.nd.db_close()
        nd = model_nd.NDModel(b.cache + '/{}'.format(b.hash))
        models = nd.generate_models()
        mlm = ModelListMerger()
        pr = ParserResults()
        pr.Categories = DescripCategories.BDN
        pr.Models.AddRange(list(mlm.GetUnique(models)))
        pr.Build("百度云")
    except:
        traceback.print_exc()
        pr = ParserResults()
    return pr