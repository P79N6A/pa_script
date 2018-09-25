#coding=utf-8
import PA_runtime
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference('model_im')
    clr.AddReference('unity_c37r')
except:
    pass
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions
from PA_runtime import *
import System.Data.SQLite as sql

import PA
import model_im
import os
import sqlite3
import hashlib
import json
import re
import unity_c37r
import traceback

def md5(string):
    return hashlib.md5(str(string)).hexdigest()

def _db_record_get_value(record, column, default_value=None):
    if not record[column].IsDBNull:
        return record[column].Value
    return default_value

class SimpleMessage(object):
    def __init__(self):
        self.content = ""
        self.media_type = 99
        self.media_path = ""
        pass

#######
#error level....
#######
Normal = 0
Warnings = 1
Errors = 2
Critical = 3

def module_print(msg, level = 0):
    if level == 0:
        print('[MiLiao Message]:{}'.format(msg))
    elif level == 1:
        print('[MiLiao Warning]:{}'.format(msg))
    elif level == 2:
        print('[MiLiao Error]:{}'.format(msg))
    elif level == 3:
        print('******\r\n[MiLiao Critical]:{}, Parse Exists!********\r\n')
VERSION_APP_VALUE = 1
class MiLiao(object):
    def __init__(self, root, extract_deleted, extract_source):
        self.root_node = root
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.im = model_im.IM()
        self.cache = ds.OpenCachePath('miliao')
        self.need_parse = False
        if not os.path.exists(self.cache):
            os.mkdir(self.cache)
        if self.im.need_parse(self.cache + "/C37R", VERSION_APP_VALUE):
            self.im.db_create(os.path.join(self.cache, 'C37R'))
            self.need_parse = True
        else:
            self.need_parse = False
        self.account = list()
        self.current_login = None
    

    def search_account(self):
        """
        find account...
        """
        perfer_node = self.root_node.GetByPath('/Library/Preferences/com.xiaomi.miliao.plist')
        res = BPReader(perfer_node.Data).top
        v_dict = res['local_client_setting_init_status']
        if v_dict is None:
            #print('find by property list failed, try to load from file_system...')
            module_print('find by property list failed, try to load from file_system...', 1)
        else:
            for k in v_dict.Keys:
                self.account.append(k)
                if v_dict[k].Value is True:
                    self.current_login = k
            return
        # find by fs
        dir_node = self.root_node.GetByPath('/Documents')
        dir_abs = dir_node.PathWithMountPoint
        dirs = os.listdir(dir_abs)
        for d in dirs:
            try:
                r = int(d)
                if not self.account.__contains__(d):
                    self.account.append(d)
            except ValueError as e:
                continue
        module_print('total find:%d accounts' %self.account.__len__())
    
    def analyse_account(self, aid):
        account_node = self.root_node.GetByPath('/Documents/{}/account'.format(aid))
        # for debug
        # print  os.listdir(account_node.PathWithMountPoint)
        ##########################Friends#####################################
        f_sql_node = account_node.GetByPath('/{}_Relation2.sqlite'.format(aid))
        #db = sqlite3.connect(f_sql_node.PathWithMountPoint)
        #cur = db.cursor()
        db = unity_c37r.create_connection(f_sql_node.PathWithMountPoint)
        cmd = sql.SQLiteCommand(db)
        friends = list()
        # cur.execute('''
        # select miliaoid, name, comments, icon, type, timestamp from MLRelation2Object 
        # ''')
        cmd.CommandText = '''
            select miliaoid, name, comments, icon, type, timestamp from MLRelation2Object 
        '''
        reader = cmd.ExecuteReader()
        #row = cur.fetchone()
        while reader.Read():
            if canceller.IsCancellationRequested:
                self.im.db_close()
                raise IOError("E")
            f = model_im.Friend()
            f.account_id = aid
            f.friend_id = unity_c37r.c_sharp_get_string(reader, 0)
            f.deleted = 0
            f.nickname = unity_c37r.c_sharp_get_string(reader, 1)
            f.remark = unity_c37r.c_sharp_get_string(reader, 2)
            pic = unity_c37r.c_sharp_get_string(reader, 3)
            f.icon = self.__get_media_path(pic, -1)
            # f.type check this later
            f.source = f_sql_node.AbsolutePath
            # update time ?
            friends.append(f)
        cmd.Dispose()
        db.Close()

        f_info_node = account_node.GetByPath('/{}_PersonalInfo2.sqlite'.format(aid))
        #db = sqlite3.connect(f_info_node.PathWithMountPoint)
        db = unity_c37r.create_connection(f_info_node.PathWithMountPoint)
        #cur = db.cursor()
        cmd = sql.SQLiteCommand(db)
        for f in friends:
            cmd.CommandText = '''
                select info from MLPersonalInfo2Object where miliaoid = '{}'
            '''.format(f.friend_id)
            reader = cmd.ExecuteReader()
            if not reader.Read():
                cmd.Dispose()
                continue
            jstring = unity_c37r.c_sharp_get_string(reader, 0)
            r = json.loads(jstring, encoding='utf-8')
            f.address = r.get('city')
            if r.get('sex') == u'男':
                f.gender = 0
            elif r.get('sex') == u'女':
                f.gender = 1
            else:
                f.gender = 9
            f.photo = self.__get_media_path(r.get('icon'), -1)
            f.signature = r.get('signature')
            self.im.db_insert_table_friend(f)
            cmd.Dispose()
        self.im.db_commit()
        # cur.execute('''
        # select info from MLPersonalInfo2Object where miliaoid = {} 
        # '''.format(aid))
        # row = cur.fetchone()
        cmd.CommandText = '''
            select info from MLPersonalInfo2Object where miliaoid = {} 
        '''.format(aid)
        reader = cmd.ExecuteReader()
        if not reader.Read():
            module_print('no account record in the db, skipped', 1)
        else:
            a = model_im.Account()
            a.account_id = aid
            jstring = unity_c37r.c_sharp_get_string(reader, 0)
            r = json.loads(jstring)
            a.nickname = r.get('nickname')
            a.address = r.get('city')
            a.birthdat = r.get('birthday')
            a.source = f_info_node.PathWithMountPoint
            a.signature = r.get('signature')
            icon_url = r.get('icon')
            a.photo = self.__get_media_path(icon_url, -1)
            a.source = f_info_node.AbsolutePath
            self.im.db_insert_table_account(a)
        ##########################Groups#####################################
        cmd.Dispose()
        db.Close()

        grp_sql_node = account_node.GetByPath('/{}_MUC2.sqlite'.format(aid))
        if grp_sql_node is not None:
            db = unity_c37r.create_connection(grp_sql_node.PathWithMountPoint)
            cmd = sql.SQLiteCommand(db)
            cmd.CommandText = '''
            select creator_id, creator_name, group_id, group_name, create_time, group_member_limit, group_icon
            from MLMUC2Object
            '''
            reader = cmd.ExecuteReader()
        else:
            reader = None
        grp = dict()
        while reader is not None and reader.Read():
            if canceller.IsCancellationRequested:
                self.im.db_close()
                raise IOError("E")
            g = model_im.Chatroom()
            g.account_id = aid
            g.chatroom_id = unity_c37r.c_sharp_get_string(reader, 2)
            g.creator_id = unity_c37r.c_sharp_get_string(reader, 0)
            g.create_time = unity_c37r.c_sharp_get_long(reader, 4)
            g.owner_id = g.creator_id
            g.name = unity_c37r.c_sharp_get_string(reader, 3)
            g.max_member_count = unity_c37r.c_sharp_get_long(reader, 5)
            g.deleted = 0
            g.source = grp_sql_node.AbsolutePath
            g.member_count = 0
            pic = unity_c37r.c_sharp_get_string(reader, 6)
            if pic is None:
                pic = ""
            res = re.search('\\[(.*)\\]', pic, re.I | re.M)
            if res is not None:
                g.photo = self.__get_media_path(json.loads(res.group(1)).get('url'), -1) if res.group(1) is not "" else None
            grp[g.chatroom_id] = g
        if reader is not None:
            cmd.Dispose()
            db.Close()
        grp_mem_node = account_node.GetByPath('/{}_mucMember2.sqlite'.format(aid))
        if grp_mem_node is not None:
            #db = sqlite3.connect(grp_mem_node.PathWithMountPoint)
            db = unity_c37r.create_connection(grp_mem_node.PathWithMountPoint)
            #cur = db.cursor() 
            cmd = sql.SQLiteCommand(db)
            cmd.CommandText = '''
                select group_id, member_gender, member_icon, member_id, member_nick, join_time, last_send_msg, member_uptodate
                from MLMUCMember2Object           
            '''
            reader = cmd.ExecuteReader()
        else:
            reader = None
        while reader is not None and reader.Read():
            if canceller.IsCancellationRequested:
                self.im.db_close()
                raise IOError("E")
            gid = unity_c37r.c_sharp_get_string(reader, 0)
            if grp.__contains__(gid):
                grp[gid].member_count += 1
            m = model_im.ChatroomMember()
            m.account_id = aid
            m.member_id = unity_c37r.c_sharp_get_string(reader, 3)
            m.display_name = unity_c37r.c_sharp_get_string(reader, 4)
            # fix it later
            #m.photo = md5()
            pic = unity_c37r.c_sharp_get_string(reader, 2)
            m.photo = self.__get_media_path(pic, -1) if pic is not "" else None
            m.source = grp_mem_node.AbsolutePath
            m.chatroom_id = unity_c37r.c_sharp_get_string(reader, 0)
            #m.gender =  # fix it later...
            m.gender = 9
            m.deleted = 0
            self.im.db_insert_table_chatroom_member(m)
        if reader is not None:
            cmd.Dispose()
            db.Close()
        for k in grp:
            self.im.db_insert_table_chatroom(grp[k])
        self.im.db_commit()
        # parse message
        # as it very complex, we move it to another function
        self.__parse_message(account_node, aid)

    def __parse_message(self, a_node, aid):
        # friend message
        f_message_node = a_node.GetByPath('/{}_Message.sqlite'.format(aid))
        #f_sql = sqlite3.connect(f_message_node.PathWithMountPoint)
        db = unity_c37r.create_connection(f_message_node.PathWithMountPoint)
        cmd = sql.SQLiteCommand(db)
        #cur = f_sql.cursor()
        # cur.execute('''
        #     select ZLOCAL_ID, ZBODY_TYPE, ZTIMESTAMP, ZBODY, ZEXT_ID, ZMSG_SENDER, ZMSG_TO, ZMSG_XML from ZMESSAGEV5OBJECT
        # ''')
        cmd.CommandText = '''
            select ZLOCAL_ID, ZBODY_TYPE, ZTIMESTAMP, ZBODY, ZEXT_ID, ZMSG_SENDER, ZMSG_TO, ZMSG_XML from ZMESSAGEV5OBJECT
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            if canceller.IsCancellationRequested:
                self.im.db_close()
                raise IOError("E")
            m = model_im.Message()
            m.account_id = aid
            target_id = unity_c37r.c_sharp_get_string(reader, 5)
            m.is_sender = 1 if str(target_id) == aid else 0
            m.deleted = 0
            m.msg_id = unity_c37r.c_sharp_get_long(reader, 0)
            m.send_time = int(unity_c37r.c_sharp_get_real(reader, 2) * 1000) / 1000
            m.source = f_message_node.AbsolutePath
            m.sender_id = unity_c37r.c_sharp_get_string(reader, 5)
            if m.is_sender == 1:
                tid = unity_c37r.c_sharp_get_string(reader, 6)
                m.talker_id = tid.split('@')[0]
            else:
                m.talker_id = unity_c37r.c_sharp_get_string(reader, 5)
            #def __process_message(self, content, xml, aid, m_type, ext_id):
            p1 = unity_c37r.c_sharp_get_string(reader, 3)
            p2 = unity_c37r.c_sharp_get_string(reader, 7)
            tp = unity_c37r.c_sharp_get_long(reader, 1)
            p3 = unity_c37r.c_sharp_get_string(reader, 4)
            s_msg = self.__process_message(p1, p2, aid, tp, p3)
            m.content = s_msg.content
            m.media_path = s_msg.media_path
            m.type = s_msg.media_type
            try:
                self.im.db_insert_table_message(m)
            except:
                print('fucked!')
        cmd.Dispose()
        db.Close()
        self.im.db_commit()
        g_message_node = self.root_node.GetByPath('Documents/{}/account/{}_MUC.sqlite'.format(aid, aid))
        if g_message_node is None:
            reader = None
        else:
            db = unity_c37r.create_connection(g_message_node.PathWithMountPoint)
            cmd = sql.SQLiteCommand(db)
            cmd.CommandText = '''
                select ZBODY_TYPE, ZMSG_ID, ZSEND_TIME, ZBODY, ZSENDER_ID, ZMSG_TO, ZMSG_XML, ZEXT_ID from ZMUCMESSAGEOBJECT
            '''
            reader = cmd.ExecuteReader()
        while reader is not None and reader.Read():
            if canceller.IsCancellationRequested:
                self.im.db_close()
                raise IOError("E")
            m = model_im.Message()
            m.msg_id = unity_c37r.c_sharp_get_string(reader, 1)
            m.account_id = aid
            sender_id = unity_c37r.c_sharp_get_string(reader, 4)
            m.sender_id = sender_id if sender_id is not 0 or unity_c37r.c_sharp_get_string(reader, 4) != '' else -1000 # -1000 means system...
            m.is_sender = 1 if str(m.sender_id) == aid else 0
            m.send_time = unity_c37r.c_sharp_get_long(reader, 2) / 1000
            m.source = g_message_node.AbsolutePath
            m.deleted = 0
            tp = unity_c37r.c_sharp_get_long(reader, 0)
            xml = unity_c37r.c_sharp_get_string(reader, 6)
            ext_id = unity_c37r.c_sharp_get_string(reader, 7)
            if tp == 0 or tp == 3:
                m.content = unity_c37r.c_sharp_get_string(reader, 3)
                m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT if m.sender_id != -1000 else model_im.MESSAGE_CONTENT_TYPE_SYSTEM
            elif xml is not None and ext_id is None:
                if tp == 1:
                    r = os.path.exists(os.path.join(self.root_node.PathWithMountPoint, 'Documents/image/%s' % ext_id))
                    if r:
                        m.media_path = os.path.join(self.root_node.AbsolutePath, 'Documents/image/%s' % ext_id)
                        m.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                    elif os.path.exists(os.path.join(self.root_node.PathWithMountPoint, 'Documents/image/th_%s' % ext_id)):
                        m.media_path = os.path.join(self.root_node.AbsolutePath, 'Documents/image/th_%s' % ext_id)
                        m.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                    else:
                        m.content = 'image message not cached'
                        m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                elif tp == 6:
                    r = self.__handle_location(aid, ext_id)
                    if r == '':
                        m.content = 'location message not cached'
                        m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    r = r.split(';')
                    m.location_name = r[0]
                    m.location_lat = r[1]
                    m.location_lng = r[2]
                    m.type = model_im.MESSAGE_CONTENT_TYPE_LOCATION
            else:
                p1 = unity_c37r.c_sharp_get_string(reader ,3)
                p2 = unity_c37r.c_sharp_get_string(reader, 6)
                ext_id = unity_c37r.c_sharp_get_string(reader, 7)
                s_msg = self.__process_message(p1, p2, aid, tp, ext_id)
                m.media_path = s_msg.media_path
                m.type = s_msg.media_type
            self.im.db_insert_table_message(m)
        if reader is not None:
            cmd.Dispose()
            db.Close()
        self.im.db_commit()

    def __process_message(self, content, xml, aid, m_type, ext_id):
        #define MIMSGTYPE_STR		0      [OK]
        #define MIMSGTYPE_IMG		1      [OK]
        #define MIMSGTYPE_AUDIO		5      [OK]
        #define MIMSGTYPE_VIDEO		23     [OK]
        #define MIMSGTYPE_FILE		29     [OK]
        #define MIMSGTYPE_CARD		16     [OK]
        #define MIMSGTYPE_LOCATION	6      [OK]
        #define MIMSGTYPE_SYSTEM	3      [OK]
        #define MIMSGTYPE_REDPACKET	26     [OK]
        xml_ns = XNamespace.Get('xm:chat_att')
        try:
            xele = XElement.Parse(xml)
        except Exception as e:
            traceback.print_exc()
            print(xml)
            msg = SimpleMessage()
            msg.content = content
            msg.media_type = model_im.MESSAGE_CONTENT_TYPE_TEXT
            msg.media_path = None
            return msg
        msg = SimpleMessage()
        if m_type == 0:
            msg.content = content
            msg.media_type = model_im.MESSAGE_CONTENT_TYPE_TEXT
            msg.media_path = None
            return msg
        elif m_type == 1:
            attachment = xele.Element(xml_ns + "attachment")
            resid = attachment.Attribute("resid")
            # fix it later...
            if resid is None or resid == "":
                msg.content = "image not cached"
                msg.media_type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                msg.media_path = None
            else:
                resid = resid.Value
                msg.media_path = self.__get_media_path(resid, m_type)
                msg.media_type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
            return msg
        elif m_type == 5:
            attachment = xele.Element(xml_ns + "attachment")
            resid = attachment.Attribute("resid")
            if resid is None or resid == "":
                msg.content = "audio not cached"
                msg.media_type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                msg.media_path = None
            else:
                msg.media_path = self.__get_media_path(resid, m_type)
                msg.media_type = model_im.MESSAGE_CONTENT_TYPE_VOICE
            return msg
        elif m_type == 23:
            attachment = xele.Element(xml_ns + "attachment")
            resid = attachment.Attribute("resid")
            if resid is None or resid == "":
                msg.content = "video not cached"
                msg.media_type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                msg.media_path = None
            else:
                msg.media_path = self.__get_media_path(resid, m_type)
                msg.media_type = model_im.MESSAGE_CONTENT_TYPE_VIDEO
            return msg
        elif m_type == 29:
            attachment = xele.Element(xml_ns + 'attachment')
            resid = attachment.Attribute('resid')
            r_k = re.search('/0.bucket.ks3.mi.com/(.*)', resid.Value, re.I | re.M)
            if r_k is not None:
                m_path = self.__find_file(aid, r_k.group(1))
                msg.media_path = m_path
                msg.media_type = model_im.MESSAGE_CONTENT_TYPE_ATTACHMENT
                return msg
            msg.content = 'FileMessage Not Cached..'
            msg.media_type = model_im.MESSAGE_CONTENT_TYPE_TEXT
            return msg
        elif m_type == 20:
            # 这种订阅消息一般不涉及特别敏感的信息，滥竽充数用
            msg.content = xml
            msg.media_type = model_im.MESSAGE_CONTENT_TYPE_TEXT
            return msg
        elif m_type == 16:
            ext_ele = xele.Element('ext')
            u_card = ext_ele.Element('u_card')
            uid = u_card.Attribute('jid')
            icon = self.__get_media_path(u_card.Attribute('icon'), -1)
            name = u_card.Attribute('name')
            gender = u_card.Attribute('sex')
            msg.content = '''name:{}\nuid:{}\ngender:{}
            '''.format(name, uid, gender)
            msg.media_path = icon
            msg.media_type = model_im.MESSAGE_CONTENT_TYPE_CONTACT_CARD
            return msg
        elif m_type == 26: # red packet
            ext = xele.Element('ext')
            rp = ext.Element('redpacket')
            if rp is not None:
                msg.content = rp.Value
            else:
                msg.content = 'unhandled redpacket message'
            msg.media_type = model_im.MESSAGE_CONTENT_TYPE_TEXT
            return msg
        # otherwise
        msg.content = "unsorted message %d" % m_type
        msg.media_type = model_im.MESSAGE_CONTENT_TYPE_TEXT
        return msg

    def __find_file(self, aid, f_k):
        f_sql_node = self.root_node.GetByPath('Documents/{}/account/{}_ks3Resource.sqlite'.format(aid, aid))
        if f_sql_node is None:
            return ''
        db = unity_c37r.create_connection(f_sql_node.PathWithMountPoint)
        cmd = sql.SQLiteCommand(db)
        cmd.CommandText = '''
            select localPath from MLKS3ResourceObject where objectKey = '{}' 
        '''.format(f_k)
        reader = cmd.ExecuteReader()
        res = ''
        if reader.Read():
            res = os.path.join(self.root_node.AbsolutePath, 'Documents/{}/{}'.format(aid, unity_c37r.c_sharp_get_string(reader, 0)))
        cmd.Dispose()
        db.Close()
        return res

    def __handle_location(self, aid, f_k):
        f_sql_node = self.root_node.GetByPath('Documents/{}/account/{}_Attachment.sqlite'.format(aid, aid))
        if f_sql_node is None:
            return None
        #db = sqlite3.connect(f_sql_node.PathWithMountPoint)
        db = unity_c37r.create_connection(f_sql_node.PathWithMountPoint)
        cmd = sql.SQLiteCommand(db)
        cmd.CommandText = '''
            select ZEXTENSION from ZATTACHMENTOBJECT where ZATT_ID = '{}'
        '''.format(f_k)
        reader = cmd.ExecuteReader()
        res = ''
        if reader.Read():
            res = unity_c37r.c_sharp_get_string(reader, 0)
        cmd.Dispose()
        db.Close()
        return res

    def __get_media_path(self, res_id, media_type, aid = None):      
        """
            why so hard to write this fucking code
        """
        # avatar...
        r_path = self.root_node.PathWithMountPoint
        abs_path = self.root_node.AbsolutePath
        if media_type == -1:
            hash_code = md5(res_id)
            #r_path = self.root_node.PathWithMountPoint
            m_path = os.path.join(r_path, "Documents/head/%s" % hash_code)
            if os.path.exists(m_path):
                return os.path.join(abs_path, "Documents/head/%s" % hash_code)
            return ""
        elif media_type == 1:
            hash_code = md5(res_id)
            #r_path = self.root_node.PathWithMountPoint
            m_path = os.path.join(r_path, "Documents/image/{}".format(hash_code))
            if os.path.exists(m_path):
                return os.path.join(abs_path, "Documents/image/{}".format(hash_code))
            m_path = os.path.join(r_path, "Documents/image/th_%s" % hash_code)
            if os.path.exists(m_path):
                return os.path.join(abs_path, "Documents/image/th_%s" % hash_code)
            return ""
        elif media_type == 23:
            hash_code = md5(res_id)
            r_path = self.root_node.PathWithMountPoint
            m_path = os.path.join(r_path, "Documents/video/%s.mp4" % hash_code)
            if os.path.exists(m_path):
                return os.path.join(abs_path, "Documents/video/%s.mp4" % hash_code)
            return ""
        elif media_type == 5:
            hash_code = md5(res_id)
            r_path = self.root_node.PathWithMountPoint
            m_path = os.path.join(r_path, "Documents/audio/%s.zip" % hash_code)
            if os.path.exists(m_path):
                return os.path.join(abs_path, "Documents/audio/%s.zip" % hash_code)
            return ""

def parse_miliao(root, extract_deleted, extract_source):
    #root = FileSystem.FromLocalDir(r"D:\BaiduNetdiskDownload\miliao6_0")
    try:
        ml = MiLiao(root, extract_deleted, extract_source)
        if ml.need_parse:
            ml.search_account()
            for aid in ml.account:
                ml.analyse_account(aid)
            ml.im.db_insert_table_version(model_im.VERSION_KEY_DB, model_im.VERSION_VALUE_DB)
            ml.im.db_insert_table_version(model_im.VERSION_KEY_APP, VERSION_APP_VALUE)
            ml.im.db_commit()
            ml.im.db_close()
        models = model_im.GenerateModel(ml.cache + '/C37R').get_models()
        mlm = ModelListMerger()
        pr = ParserResults()
        pr.Categories = DescripCategories.QQ
        pr.Models.AddRange(list(mlm.GetUnique(models)))
        pr.Build('米聊')
    except Exception as e:
        traceback.print_exc()
        pr = ParserResults()
    return pr
    