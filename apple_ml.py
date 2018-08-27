#coding=utf-8
import PA_runtime
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
# clr.AddReference('PA.InfraLib.Exts')
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions
from PA_runtime import *
# from PA.InfraLib.Exts import FileSystem
import PA
# python modules
import model_im
import os
import sqlite3
import hashlib
import json
import re

def md5(string):
    return hashlib.md5(string).hexdigest()

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

class MiLiao(object):
    def __init__(self, root, extract_deleted, extract_source):
        self.root_node = root
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.im = model_im.IM()
        cache = ds.OpenCachePath('miliao')
        if not os.path.exists(cache):
            os.mkdir(cache)
        self.im.db_create(os.path.join(cache, 'C37R'))
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
        print  os.listdir(account_node.PathWithMountPoint)
        ##########################Friends#####################################
        f_sql_node = account_node.GetByPath('/{}_Relation2.sqlite'.format(aid))
        db = sqlite3.connect(f_sql_node.PathWithMountPoint)
        cur = db.cursor()
        friends = list()
        cur.execute('''
        select miliaoid, name, comments, icon, type, timestamp from MLRelation2Object 
        ''')
        row = cur.fetchone()
        while row is not None:
            f = model_im.Friend()
            f.account_id = aid
            f.friend_id = row[0]
            f.deleted = 0
            f.nickname = row[1]
            f.remark = row[2]
            # note: give certain path!!!!!!!
            #f.icon = md5(row[3]) if row[3] is not None or row[3] is not '' else ''
            f.icon = self.__get_media_path(row[3], -1)
            # f.type check this later
            f.source = f_sql_node.PathWithMountPoint
            # update time ?
            friends.append(f)
            row = cur.fetchone()
        cur.close()
        db.close()
        f_info_node = account_node.GetByPath('/{}_PersonalInfo2.sqlite'.format(aid))
        db = sqlite3.connect(f_info_node.PathWithMountPoint)
        cur = db.cursor()
        for f in friends:
            cur.execute('''
            select info from MLPersonalInfo2Object where miliaoid = {}    
            '''.format(f.account_id))
            row = cur.fetchone()
            if row is None:
                continue
            r = json.loads(row[0], encoding='utf-8')
            f.address = r.get('city')
            if r.get('sex') == u'男':
                f.gender = 0
            elif r.get('sex') == u'女':
                f.gender = 1
            else:
                f.gender = 9
            # 1989-12-09 to timestamp...
            #f.birthday = r.get('birthday')
            f.photo = self.__get_media_path(r.get('icon'), -1)
            f.signature = r.get('signature')
            self.im.db_insert_table_friend(f)
        self.im.db.commit()
        cur.execute('''
        select info from MLPersonalInfo2Object where miliaoid = {} 
        '''.format(aid))
        row = cur.fetchone()
        if row is None:
            module_print('no account record in the db, skipped', 1)
        else:
            a = model_im.Account()
            a.account_id = aid
            r = json.loads(row[0])
            a.nickname = r.get('nickname')
            a.address = r.get('city')
            #a.birthday = r.get('birthday')
            a.birthdat = r.get('birthday')
            a.source = f_info_node.PathWithMountPoint
            a.signature = r.get('signature')
            icon_url = r.get('icon')
            # special process....
            #a.icon = md5(icon_url) if icon_url is not None and icon_url != '' else None
            a.photo = self.__get_media_path(icon_url, -1)
            a.source = f_info_node.PathWithMountPoint
            self.im.db_insert_table_account(a)
        ##########################Groups#####################################
        cur.close()
        db.close()
        grp_sql_node = account_node.GetByPath('/{}_MUC2.sqlite'.format(aid))
        if grp_sql_node is not None:
            db = sqlite3.connect(grp_sql_node.PathWithMountPoint)
            cur = db.cursor()
            cur.execute('''
            select creator_id, creator_name, group_id, group_name, create_time, group_member_limit, group_icon
            from MLMUC2Object
            ''')
            row = cur.fetchone()
        else:
            row = None
        grp = dict()
        while row is not None:
            g = model_im.Chatroom()
            #g.
            g.account_id = aid
            g.chatroom_id = row[2]
            g.creator_id = row[0]
            g.create_time = row[4]
            g.owner_id = row[0]
            g.name = row[3]
            g.max_member_count = row[5]
            g.deleted = 0
            g.source = grp_sql_node.PathWithMountPoint
            g.member_count = 0
            res = re.search('\\[(.*)\\]', row[6], re.I | re.M)
            if res is not None:
                g.photo = self.__get_media_path(json.loads(res.group(1)).get('url'), -1) if res.group(1) is not "" else None
            grp[g.chatroom_id] = g
            row = cur.fetchone()
        cur.close()
        db.close()
        grp_mem_node = account_node.GetByPath('/{}_mucMember2.sqlite'.format(aid))
        if grp_mem_node is not None:
            db = sqlite3.connect(grp_mem_node.PathWithMountPoint)
            cur = db.cursor()
            cur.execute('''
                select group_id, member_gender, member_icon, member_id, member_nick, join_time, last_send_msg, member_uptodate
                from MLMUCMember2Object
            ''')
            row = cur.fetchone()
        else:
            row = None
        while row is not None:
            if grp.__contains__(row[0]):
                grp[row[0]].member_count += 1
            m = model_im.ChatroomMember()
            m.account_id = aid
            m.member_id = row[3]
            m.display_name = row[4]
            # fix it later
            #m.photo = md5()
            m.photo = self.__get_media_path(row[2], -1) if row[2] is not None else None
            m.source = grp_mem_node.PathWithMountPoint
            m.chatroom_id = row[0]
            #m.gender =  # fix it later...
            m.gender = 9
            m.deleted = 0
            self.im.db_insert_table_chatroom_member(m)
            row = cur.fetchone()
        for k in grp:
            self.im.db_insert_table_chatroom(grp[k])
        self.im.db_commit()
        # parse message
        # as it very complex, we move it to another function
        self.__parse_message(account_node, aid)

    def __parse_message(self, a_node, aid):
        # friend message
        f_message_node = a_node.GetByPath('/{}_Message.sqlite'.format(aid))
        f_sql = sqlite3.connect(f_message_node.PathWithMountPoint)
        cur = f_sql.cursor()
        cur.execute('''
            select ZLOCAL_ID, ZBODY_TYPE, ZTIMESTAMP, ZBODY, ZEXT_ID, ZMSG_SENDER, ZMSG_TO, ZMSG_XML from ZMESSAGEV5OBJECT
        ''')
        row = cur.fetchone()
        while row is not None:
            m = model_im.Message()
            m.account_id = aid
            m.is_sender = 1 if str(row[5]) == aid else 0
            m.deleted = 0
            m.msg_id = row[0]
            m.send_time = int(row[2] * 1000) / 1000
            m.source = f_message_node.PathWithMountPoint
            m.sender_id = row[5]
            if m.is_sender == 1:
                m.talker_id = row[6].split('@')[0]
            else:
                m.talker_id = row[5]
            s_msg = self.__process_message(row[3], row[7], aid, row[1], row[4])
            m.content = s_msg.content
            m.media_path = s_msg.media_path
            m.type = s_msg.media_type
            try:
                self.im.db_insert_table_message(m)
            except:
                print('fucked!')
            row = cur.fetchone()
        self.im.db_commit()
        g_message_node = self.root_node.GetByPath('Documents/{}/account/{}_MUC.sqlite'.format(aid, aid))
        if g_message_node is None:
            row = None
        else:
            g_message_sql = sqlite3.connect(g_message_node.PathWithMountPoint)
            # note ZMSG_XML MAY BE NULL!
            cur = g_message_sql.cursor()
            cur.execute('''
                select ZBODY_TYPE, ZMSG_ID, ZSEND_TIME, ZBODY, ZSENDER_ID, ZMSG_TO, ZMSG_XML, ZEXT_ID from ZMUCMESSAGEOBJECT
            ''')
            row = cur.fetchone()
        while row is not None:
            m = model_im.Message()

            m.msg_id = row[1]
            m.account_id = aid
            m.sender_id = row[4] if row[4] is not None or row[4] != '' else -1000 # -1000 means system...
            m.is_sender = 1 if str(m.sender_id) == aid else 0
            m.send_time = int(row[2]) / 1000
            m.source = g_message_node.PathWithMountPoint
            m.deleted = 0
            
            # system only send text messages.
            if row[0] == 0 or row[0] == 3:
                m.content = row[3]
                m.type = model_im.MESSAGE_CONTENT_TYPE_TEXT if m.sender_id != -1000 else model_im.MESSAGE_CONTENT_TYPE_SYSTEM
            elif row[7] is not None and row[6] is None:
                ext_id = row[7]
                if row[0] == 1:
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
                elif row[0] == 6:
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
                s_msg = self.__process_message(row[3], row[6], aid, row[0], row[7])
                m.media_path = s_msg.media_path
                m.type = s_msg.media_type
            self.im.db_insert_table_message(m)
            row = cur.fetchone()
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
        xele = XElement.Parse(xml)
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
        db = sqlite3.connect(f_sql_node.PathWithMountPoint)
        cur = db.cursor()
        cur.execute('''select localPath from MLKS3ResourceObject where objectKey = '{}' '''.format(f_k))
        row = cur.fetchone()
        if row is not None:
            return os.path.join(self.root_node.PathWithMountPoint, 'Documents/{}/{}'.format(aid, row[0]))
        else:
            return ''
        cur.close()
        db.close()

    def __handle_location(self, aid, f_k):
        f_sql_node = self.root_node.GetByPath('Documents/{}/account/{}_Attachment.sqlite'.format(aid, aid))
        if f_sql_node is None:
            return None
        db = sqlite3.connect(f_sql_node.PathWithMountPoint)
        cur = db.cursor()
        cur.execute('''
        select ZEXTENSION from ZATTACHMENTOBJECT where ZATT_ID = '{}'
        '''.format(f_k))
        row = cur.fetchone()
        if row is not None:
            return row[0]
        else:
            return ""

    def __get_media_path(self, res_id, media_type, aid = None):      
        """
            why so hard to write this fucking code
        """
        # avatar...
        if media_type == -1:
            hash_code = md5(res_id)
            r_path = self.root_node.PathWithMountPoint
            m_path = os.path.join(r_path, "Documents/head/%s" % hash_code)
            if os.path.exists(m_path):
                return m_path
            return ""
        elif media_type == 1:
            hash_code = md5(res_id)
            r_path = self.root_node.PathWithMountPoint
            m_path = os.path.join(r_path, "Documents/image/{}".format(hash_code))
            if os.path.exists(m_path):
                return m_path
            m_path = os.path.join(r_path, "Documents/image/th_%s" % hash_code)
            if os.path.exists(m_path):
                return m_path
            return ""
        elif media_type == 23:
            hash_code = md5(res_id)
            r_path = self.root_node.PathWithMountPoint
            m_path = os.path.join(r_path, "Documents/video/%s.mp4" % hash_code)
            if os.path.exists(m_path):
                return m_path
            return ""
        elif media_type == 5:
            hash_code = md5(res_id)
            r_path = self.root_node.PathWithMountPoint
            m_path = os.path.join(r_path, "Documents/audio/%s.zip" % hash_code)
            if os.path.exists(m_path):
                return m_path
            return ""

def parse_miliao(root, extract_deleted, extract_source):
    ml = MiLiao(root, extract_deleted, extract_source)
    ml.search_account()
    '''
        mem_range = MemoryRange.CreateFromFile(rfs_path)
        r_node = Node(vfs_name, Node_T_Matrix['Emb'])
        r_node.Data = mem_range
        parent.Children.Add(r_node) # ^_^ must add this to virtual file system
        return r_node
    '''
    for aid in ml.account:
        ml.analyse_account(aid)
    