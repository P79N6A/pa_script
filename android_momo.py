#coding:utf-8
#
# 要知前世因 今生受的是， 要知后世果 今生做的是
# 罪由心生 还从心灭，只在表面上下功夫，徒然浪费时间
#
import clr
try:
    clr.AddReference('System.Data.SQLite')
    clr.AddReference('PA_runtime')
    clr.AddReference('model_im')
    clr.AddReference('unity_c37r')
except:
    pass
del clr
from PA_runtime import *
import System.Data.SQLite as sql
import model_im
import unity_c37r
import os
import json
import traceback
MM_PARSER_VERSION = 1

def string2Bytes(string):
    byte_s = bytes(string,encoding='utf-8')
    arr = []
    i = 0
    while i < 256:
        arr.append(i)
        i += 1
    if len(byte_s) is 0:
        return
    i = 0
    k = 0
    j = 0
    while j < 256:
        i = ((i + ord(byte_s[k])&0xff) + (arr[j] & 0xff)) & 0xff
        b = arr[j]
        arr[j] = arr[i]
        arr[i] = b
        k = (k+1) % len(byte_s)
        j += 1
    return arr
# 处理字符串字节
def processBytesAndString(bts, string):
    i = 0
    j = 0
    k = 0
    s_bts = string2Bytes(string)
    arr = []
    while i < len(bts):
        arr.append(0)
        i += 1
    i = 0
    while i < len(bts):
        k = (k + 1) & 0xff
        j = (j + s_bts[k]) & 0xff
        b = s_bts[k]
        s_bts[k] = s_bts[j]
        s_bts[j] = b
        l = (s_bts[k] + s_bts[j]) & 0xff
        arr[i] = s_bts[l] ^ bts[i]
        i = i+1
    #return arr # ==>转成字符串
    result = ''
    for a in arr:
        u = chr(a & 0xff)
        result += u
    return result

def processHexBytes(hex_bytes):
    values = []
    i = 0
    while i < len(hex_bytes):
        vl = int(hex_bytes[i],base=16)<<4 | int(hex_bytes[i+1],base=16)
        values.append(vl)
        i+=2
    return values

class momo(object):
    def __init__(self, node, extract_deleted, extract_source):
        global MM_PARSER_VERSION
        self.node = node
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.cache = ds.OpenCachePath('momo')
        self.hash = unity_c37r.md5(node.PathWithMountPoint)
        self.cache_db = os.path.join(self.cache, self.hash)
        self.need_parse = False
        self.im = model_im.IM()
        if model_im.IM.need_parse(self.cache_db, MM_PARSER_VERSION):
            self.need_parse = True
            self.im.db_create(self.cache_db)
        self.accounts = list()
        self.f_dict = dict()

    def search(self):
        db_folder = self.node.GetByPath('databases')
        if db_folder is None:
            raise IOError('can not get db folder, parser exits!')
        fl = os.listdir(db_folder.PathWithMountPoint)
        for f in fl:
            try:
                if f == '.' or f == '..':
                    continue
                a = int(f)
                if not self.accounts.__contains__(a):
                    self.accounts.append(a)
            except:
                continue
    #
    #分析删除恢复的好友
    #
    def parse_relation_recovery(self, db_node, account_id):
        if db_node is None:
            return
        db = SQLiteParser.Database.FromNode(db_node)
        ts = SQLiteParser.TableSignature('user')
        sb = SQLiteParser.Tools.AddSignatureToTable(ts, "MOMOID", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)

        for rec in db.ReadTableDeletedRecords(ts, False):
            friend = model_im.Friend()
            friend.friend_id = str(unity_c37r.try_get_rec_value(rec, 'MOMOID', ''))
            friend.nickname = str(unity_c37r.try_get_rec_value(rec, 'NAME', ""))
            friend.signature = str(unity_c37r.try_get_rec_value(rec, 'SIGNATURE', ""))
            friend.telephone = str(unity_c37r.try_get_rec_value(rec, 'PHONENUMBER', ''))
            friend.photo = str(unity_c37r.try_get_rec_value(rec, 'PHOTOS', ''))
            friend.account_id = account_id
            friend.type = model_im.FRIEND_TYPE_FRIEND
            friend.deleted = 1
            self.im.db_insert_table_friend(friend)
        ts = SQLiteParser.TableSignature('group')
        SQLiteParser.Tools.AddSignatureToTable(ts, "GID", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
        for rec in db.ReadTableDeletedRecords(ts, False):
            cm = model_im.Chatroom()
            cm.account_id = account_id
            cm.chatroom_id = str(unity_c37r.try_get_rec_value(rec, 'GID', ""))
            cm.name = str(unity_c37r.try_get_rec_value(rec, 'NAME', ''))
            cm.owner_id = str(unity_c37r.try_get_rec_value(rec, 'NAME', ""))
            cm.description = str(unity_c37r.try_get_rec_value(rec, 'SIGN', ''))
            cm.photo = str(unity_c37r.try_get_rec_value(rec, 'PHOTOS', ''))
            cm.deleted = 1
            self.im.db_insert_table_chatroom(cm)
        self.im.db_commit()
    #
    # chat_dict :   保存有对应关系的容器
    # friend_dict:  好友对应关系的容器
    # grp_dict :    群组对应关系的容器
    # 
    def parse_chat_recovery(self, db_node, account_id, chat_dict, friend_dict, grp_dict):
        if db_node is None:
            return
        db = SQLiteParser.Database.FromNode(db_node)
        for c in chat_dict:
            is_grp = grp_dict.__contains__(c)
            print(chat_dict[c])
            ts = SQLiteParser.TableSignature(chat_dict[c])
            SQLiteParser.Tools.AddSignatureToTable(ts, "m_msginfo", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            #select m_msgid, m_msginfo, m_time, m_receive, m_type, m_remoteid from {}
            for rec in db.ReadTableDeletedRecords(ts, False):
                msg = model_im.Message()
                msg.msg_id = str(unity_c37r.try_get_rec_value(rec, 'm_msgid', ''))
                msg.content = str(unity_c37r.try_get_rec_value(rec, 'm_msginfo', ''))
                msg.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                is_rcv = int(unity_c37r.try_get_rec_value(rec, 'm_receive', 0))
                msg.is_sender = 0
                if is_rcv:
                    msg.sender_id = account_id
                    msg.is_sender = 1
                elif not is_grp:
                    msg.sender_id = c
                    msg.sender_name = friend_dict[c].nickname
                else:
                    msg.sender_id = int(unity_c37r.try_get_rec_value(rec, 'm_remoteid', 0))
                if not is_grp:
                    msg.talker_id = c
                    msg.talker_name = friend_dict[c].nickname
                else:
                    msg.talker_id = c
                    msg.talker_name = grp_dict[c].name
                msg.send_time = int(unity_c37r.try_get_rec_value(rec, 'm_time', 0)) / 1000
                msg.deleted = 1
                self.im.db_insert_table_message(msg)
        self.im.db_commit()

    #parse feed
    def parse_feed(self, account_id):
        db_node = self.node.GetByPath('feed60_{}'.format(account_id))
        if db_node is None:
            return
        conn = unity_c37r.create_connection_tentatively(db_node, True)
        cmd = SQLiteCommand(conn)
        # 以下列类型全部为numeric类型，但是插入的各种类型都有
        # fid, time, content, location(name), sender id
        # sqlite 在数据管理方面是动态类型的，使用了更普遍的动态类型系统，sqlite中，值的类型和值本身是有关系的，与容器无关
        # 大型数据库则使用更为刚性和静态的类型语言
        cmd.CommandText = '''
            select _id, field3, field55, field59, field10 from commonfeed
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            feed = model_im.Feed()
            feed.account_id = account_id
            feed.deleted = 0
            feed.source = db_node.AbsolutePath
            content = json.loads(unity_c37r.c_sharp_get_string(reader, 2))
            feed.content = content.get('text')
            feed.send_time = unity_c37r.c_sharp_try_get_time(reader, 1) / 1000
            feed.sender_id = unity_c37r.c_sharp_get_long(reader, 4)
            feed.deleted = 0
            feed.insert_db(self.im)
        self.im.db_commit()
        reader.Close()
        cmd.Dispose()
        conn.Close()
        #recovery
        db = SQLiteParser.Database.FromNode(db_node)
        ts = SQLiteParser.TableSignature('commonfeed')
        SQLiteParser.Tools.AddSignatureToTable(ts, "field55", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
        for rec in db.ReadTableDeletedRecords(ts, False):
            feed = model_im.Feed()
            feed.account_id = account_id
            feed.deleted = 1
            content = unity_c37r.try_get_rec_value(rec, "field55", "")
            try:
                js = json.loads(content)
                feed.content = js.get('text')
            except:
                feed.content = content
            feed.send_time = unity_c37r.try_get_rec_value(rec, "field3", 0) / 1000
            feed.sender_id = unity_c37r.try_get_rec_value(rec, "field10", 0)
            feed.insert_db(self.im)
        self.im.db_commit()
        
    def parse(self, account_id):
        db_node = self.node.GetByPath('databases/momo_{}'.format(account_id))
        if db_node is None:
            return
        conn = unity_c37r.create_connection_tentatively(db_node)
        cmd = sql.SQLiteCommand(conn)
        cmd.CommandText = '''
            select NAME, SIGNATURE, PHONE_NUMBER, PHOTOS from user where MOMOID = {}
        '''.format(account_id)
        reader = cmd.ExecuteReader()
        if reader.Read():
            a = model_im.Account()
            a.account_id = account_id
            a.nickname = unity_c37r.c_sharp_get_string(reader, 0)
            a.signature = unity_c37r.c_sharp_get_string(reader, 1)
            a.photo = unity_c37r.c_sharp_get_string(reader, 3)
            a.telephone = unity_c37r.c_sharp_get_string(reader, 2)
            self.im.db_insert_table_account(a)
        reader.Close()
        cmd.CommandText = '''
            select MOMOID, NAME, SIGNATURE, PHONE_NUMBER, PHOTOS from user where MOMOID != {}
        '''.format(account_id)
        reader = cmd.ExecuteReader()
        while reader.Read():
            f = model_im.Friend()
            f.account_id = account_id
            f.friend_id = unity_c37r.c_sharp_get_string(reader, 0)
            f.nickname = unity_c37r.c_sharp_get_string(reader, 1)
            f.signature = unity_c37r.c_sharp_get_string(reader, 2)
            f.telephone = unity_c37r.c_sharp_get_string(reader, 3)
            f.photo = unity_c37r.c_sharp_get_string(reader, 4)
            f.type = model_im.FRIEND_TYPE_FRIEND
            self.im.db_insert_table_friend(f)
            self.f_dict[f.friend_id] = f
        reader.Close()
        self.im.db_commit()
        cmd.CommandText = '''
            select GID, NAME, CREATE_TIME, OWNER, SIGN, LOC_LAT, LOC_LNG, SITE_NAME, PHOTOS from 'group'
        '''
        reader = cmd.ExecuteReader()
        grp_dict = dict()
        while reader.Read():
            grp = model_im.Chatroom()
            grp.account_id = account_id
            grp.chatroom_id = unity_c37r.c_sharp_get_string(reader, 0)
            grp.name = unity_c37r.c_sharp_get_string(reader, 1)
            grp.create_time = unity_c37r.c_sharp_try_get_time(reader, 2) / 1000
            grp.owner_id = unity_c37r.c_sharp_get_string(reader, 3)
            grp.description = unity_c37r.c_sharp_get_string(reader, 4)
            grp.photo = unity_c37r.c_sharp_get_string(reader, 8)
            self.im.db_insert_table_chatroom(grp)
            grp_dict[grp.chatroom_id] = grp
        self.im.db_commit()
        reader.Close()
        cmd.Dispose()
        conn.Close()
        chat_list = list()
        db_node = self.node.GetByPath('databases/{}'.format(account_id))
        if db_node is None:
            pass
        conn = unity_c37r.create_connection_tentatively(db_node)
        cmd = sql.SQLiteCommand(conn)
        cmd.CommandText = '''
            select tbl_name from sqlite_master where tbl_name like 'Chat_%' and type = 'table'
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            c = unity_c37r.c_sharp_get_string(reader, 0)
            chat_list.append(c)
        reader.Close()
        idx = 0
        chat_dict = dict()
        while idx < len(chat_list):
            v = chat_list[idx].split('_')[1]
            v = processHexBytes(v)
            v = processBytesAndString(v, "jarekTan")
            #say hi 消息，之后再解析
            if v.__contains__('momo'):
                idx += 1
                continue
            v = v.split('_')[1]
            chat_dict[v] = chat_list[idx]
            idx += 1
        for c in chat_dict:
            tbl_name = chat_dict[c]
            is_grp = grp_dict.__contains__(c) # 是否是GRP群组
            if not is_grp:
                cmd.CommandText = '''
                    select m_msgid, m_msginfo, m_time, m_receive, m_type from {}
                '''.format(tbl_name)
            else:
                cmd.CommandText = '''
                    select m_msgid, m_msginfo, m_time, m_receive, m_type, m_remoteid from {}
                '''.format(tbl_name)
            reader = cmd.ExecuteReader()
            while reader.Read():
                m = model_im.Message()
                m.account_id = account_id
                m.msg_id = unity_c37r.c_sharp_get_string(reader, 0)
                m.is_sender =  0 if unity_c37r.c_sharp_get_string(reader, 3) == '1' else 1
                m.talker_id = c
                tp = unity_c37r.c_sharp_get_long(reader, 4)
                if not is_grp:
                    m.talker_name = self.f_dict[c].nickname
                else:
                    m.talker_name = grp_dict[c].name
                m.talker_type = model_im.CHAT_TYPE_FRIEND if not is_grp else model_im.CHAT_TYPE_GROUP
                if m.is_sender:
                    m.sender_id = account_id
                elif not is_grp:
                    m.sender_id = c
                    m.sender_name = m.talker_name
                else: # 如果是群组
                    m.sender_id = unity_c37r.c_sharp_get_string(reader, 5)
                m.send_time = int(unity_c37r.c_sharp_get_string(reader, 2))
                content = unity_c37r.c_sharp_get_string(reader, 1)
                js = json.loads(content)
                #如果是群组
                if is_grp:
                    m.sender_name = js.get('usertitle')
                # 0 正常消息 5 撤回消息 7 入群通知
                if tp == 0 or tp == 5 or tp == 7:
                    m.content = js.get('content')
                    m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                #暂时不知道怎么解析
                elif tp == 1:
                    m.content = "暂不支持的内容"
                    m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                #???没内容
                elif tp == 2:
                    m.content = "暂不支持的内容"
                    m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                #暂未分析
                elif tp == 6:
                    m.content = "暂不支持的内容"
                    m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                #群成员的动态，没有feed连接，只有图片
                #拆分成两条消息
                elif tp == 20:
                    m.content = js.get('t17').get('title') + '\n' + js.get('t17').get('desc')
                    m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    self.im.db_insert_table_message(m)
                    m.media_path = js.get('t17').get('pic')
                    m.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                #分享
                elif tp == 12:
                    m.content = js.get('content')
                    m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                else:
                    m.content = content
                    m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                self.im.db_insert_table_message(m)
            reader.Close()   
        cmd.Dispose()
        conn.Close()
        self.im.db_commit()
        self.parse_relation_recovery(self.node.GetByPath('databases/momo_{}'.format(account_id)), account_id)
        self.parse_chat_recovery(self.node.GetByPath('databases/{}'.format(account_id)),account_id, chat_dict, self.f_dict, grp_dict)
        self.parse_feed(account_id)

def judge_node(root):
    node = root.Parent
    if node.GetByPath('shared_prefs/com.immomo.momo_prefs.xml') is not None:
        return node
    return None

def parse_mm(root, extract_source, extract_deleted):
    try:
        node = judge_node(root)
        if node is None:
            raise IOError('E')
        m = momo(node, extract_deleted, extract_source)
        if m.need_parse:
            m.search()
            for id in m.accounts:
                m.parse(id)
            m.im.db_insert_table_version(model_im.VERSION_KEY_DB, model_im.VERSION_VALUE_DB)
            m.im.db_insert_table_version(model_im.VERSION_KEY_APP, MM_PARSER_VERSION)
            m.im.db_commit()
            m.im.db_close()
        pr = ParserResults()
        models = model_im.GenerateModel(m.cache_db).get_models()
        mlm = ModelListMerger()
        pr = ParserResults()
        pr.Categories = DescripCategories.QQ
        pr.Models.AddRange(list(mlm.GetUnique(models)))
        pr.Build('陌陌')
    except:
        traceback.print_exc()
        pr = ParserResults()
    return pr