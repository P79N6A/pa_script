# coding=utf-8
import traceback
import hashlib
import re

from PA_runtime import *
from PA.Common.Utilities.Types import TimeStampFormats

import clr
try:
    clr.AddReference('model_im')
    clr.AddReference('bcp_im')
except:
    pass
del clr

import model_im
import bcp_im

DEBUG = True
DEBUG = False

VERSION_APP_VALUE = 1

# 朋友圈类型
MOMENT_TYPE_IMAGE     = 1  # 正常文字图片
MOMENT_TYPE_TEXT_ONLY = 2  # 纯文字
MOMENT_TYPE_SHARED    = 3  # 分享
MOMENT_TYPE_MUSIC     = 4  # 带音乐的（存的是封面）
MOMENT_TYPE_EMOJI     = 10  # 分享了表情包
MOMENT_TYPE_VIDEO     = 15  # 视频

def print_run_time(func): 
    ''' decorator ''' 
    def wrapper(*args, **kw):  
        local_time = time.time()  
        res = func(*args, **kw) 
        if DEBUG:
            msg = 'Current Function <{}> run time is {:.2} s'.format(func.__name__ , time.time() - local_time)  
            TraceService.Trace(TraceLevel.Warning, "{}".format(msg))
        if res:
            return res
    return wrapper

def exc(e=''):
    if DEBUG:
        TraceService.Trace(TraceLevel.Warning, "解析出错: LINE {}, {}".format(traceback.format_exc(), e))
        # traceback.print_exc()
    else:
        pass 

def exc_debug(*e):
    if DEBUG:
        TraceService.Trace(TraceLevel.Warning, "{}".format(e))
    else:
        pass

def analyze_line(node, extract_deleted, extract_source):
    """ ios LINE 

        jp.naver.line     
    """
    exc_debug('apple_line.py runing ...!')

    pr = ParserResults()
    res = []
    try:
        res = LineParser(node, extract_deleted, extract_source).parse()
    except:
        exc()
    if res:
        pr.Models.AddRange(res)
        pr.Build('LINE')
        exc_debug('apple_line.py completed!')
    return pr

class LineParser(object):

    def __init__(self, node, extract_deleted, extract_source):
        ''' boundId: 
            
            jp.naver.line    
                node: /private/var/mobile/Containers/Data/Application/5A249183-668C-4CC0-B983-C0A7EA2E657F
                Library\Preferences\jp.naver.line.plist

            group.com.linecorp.line
                Library\Preferences\group.com.linecorp.line.plist
                Library\\Application Support\\PrivateStore\\P_u423af962f1456db6cba8465cf82bb91b\\Messages\\Line.sqlite    
        '''
        # exc_debug('node.AbsolutePath:', node.AbsolutePath)
        self.root = node
        self.user_plist_node = self.root.GetByPath('Library/Preferences/jp.naver.line.plist')

        ################# group.com.linecorp.line ################
        # Library\Preferences\group.com.linecorp.line.plist
        self.group_plist_node = list(self.root.FileSystem.Search('group.com.linecorp.line.plist$'))[0]
        # exc_debug(type(self.group_plist_node)) 
        # exc_debug(self.group_plist_node.AbsolutePath)
        # Library\\Application Support\\PrivateStore
        self.group_root = self.group_plist_node.Parent.Parent.Parent
        self.group_db_root = self.group_root.GetByPath('Library/Application Support/PrivateStore')

        self.extract_deleted = extract_deleted
        self.extract_source  = extract_source
        self.im = model_im.IM()        
        self.cachepath = ds.OpenCachePath("LINE")

        hash_str = hashlib.md5(self.root.AbsolutePath).hexdigest()[8:-8]
        self.cache_db = self.cachepath + '\\i_line_{}.db'.format(hash_str)

        self.friend_list = {} # friend_pk: Friend()

    def parse(self):
        
        if DEBUG or self.im.need_parse(self.cache_db, VERSION_APP_VALUE):
            if not self.user_plist_node:
                return []
            self.im.db_create(self.cache_db) 
            self.parse_main()
            if not canceller.IsCancellationRequested:
                self.im.db_insert_table_version(model_im.VERSION_KEY_DB, model_im.VERSION_VALUE_DB)
                self.im.db_insert_table_version(model_im.VERSION_KEY_APP, VERSION_APP_VALUE)
                self.im.db_commit()
            self.im.db_close()
        tmp_dir = ds.OpenCachePath('tmp')
        save_cache_path(bcp_im.CONTACT_ACCOUNT_TYPE_IM_LINE, self.cache_db, tmp_dir)

        models = model_im.GenerateModel(self.cache_db).get_models()
        return models

    def parse_main(self):
        ''' 
            # self.CHAT_DICT              = {}   # table ZCHAT, keys: 'ZTYPE', 'ZMID', 'chat_name'
            # self.CHAT_PK_FRIEND_PKS     = {}   # chat pk: friend pk            Z_1MEMBERS
            # self.CHAT_PK_CHATROOM_MID   = {}   # chat pk: chatroom_mid
            # self.CHATROOM_MID_NAME      = {}   # 群 ZID: ZNAME
            # self.CHATROOM_PK_MID        = {}   # 群 pk: ZID
            # self.CHATROOM_MEMBER_COUNT  = {}   # 群 mid: member_count
            # self.MEMBER_PK_CHATROOM_PKS = {}   # 群成员pk: 群pk
            # self.FRIEND_MID_TEL         = {}   # friend ZMID: telphone
            # self.FRIEND_PK_NAME_MAP     = {}   # friend pk: friend name
            # self.FRIEND_PK_MID_MAP      = {}   # friend pk: mid        

            chatroom.max_member_count       CHATROOM_MEMBER_COUNT
            CHAT_DICT.chat_name             CHATROOM_MID_NAME[caht.zmid] or CHAT_PK_FRIEND_PKS[chat_pk]
            chatroommember.chatroom_id      CHAT_PK_CHATROOM_MID[
                                                    CHATROOM_PK_MID[ 
                                                        MEMBER_PK_CHATROOM_PKS[friend_pk] 
                                                    ]
                                            ]
            friend.telephone    FRIEND_MID_TEL
            message.sender_id   FRIEND_PK_MID_MAP
            message.sender_name FRIEND_PK_NAME_MAP
            message.talker_type self._convert_chat_type(CHAT_DICT.get(rec['ZCHAT'].Value, {}).get('ZTYPE', None))             
            message.talker_name FRIEND_PK_NAME_MAP, CHAT_PK_FRIEND_PKS

            关联 已退出群 与 群成员关系:
                1. ZMESSAGE     => friend.pk: chat.pk        # CHAT_PK_FRIEND_PKS
                2. ZUSER, ZCHAT => friend.mid: chatroom.mid  # CHAT_PK_CHATROOM_MID
            需要: FRIEND_MID_PK_MAP
        '''
        account_list = self.parse_Account(self.user_plist_node, self.group_plist_node)

        for account in account_list:
            self.cur_account_id = account.account_id
            account_file_name = 'P_' + account.account_id

            ######### Share 目录下 group.com.linecorp.line #########
            if self._read_db(self.group_db_root, account_file_name+'/Messages/Line.sqlite'):
                
                CHATROOM_MEMBER_COUNT, MEMBER_PK_CHATROOM_PKS = self.preparse_group_member('Z_4MEMBERS')   # 群 mid: member_count
                FRIEND_MID_TEL     = self.preparse_friend_tel('ZCONTACT')       # friend ZMID: telphone
                CHAT_PK_FRIEND_PKS = self.preparse_chat_friend_pk('Z_1MEMBERS') # chat pk: friend pk

                FRIEND_PK_NAME_MAP, FRIEND_PK_MID_MAP = self.parse_Friend('ZUSER', FRIEND_MID_TEL)
                CHATROOM_MID_NAME, CHATROOM_PK_MID  = self.parse_Chatroom('ZGROUP', CHATROOM_MEMBER_COUNT)
                CHAT_DICT, CHATROOM_MID_NAME, CHAT_PK_CHATROOM_MID = self.preparse_ZCHAT('ZCHAT', 
                                                                                         CHATROOM_MID_NAME, 
                                                                                         FRIEND_PK_NAME_MAP,
                                                                                         CHAT_PK_FRIEND_PKS)
                DEL_FRIEND_PK_CHATROOM_MID = self.parse_Message('ZMESSAGE', FRIEND_PK_MID_MAP, 
                                                                            FRIEND_PK_NAME_MAP, 
                                                                            CHAT_DICT, 
                                                                            CHAT_PK_FRIEND_PKS)
                self.parse_ChatroomMember(self.friend_list, 
                                          DEL_FRIEND_PK_CHATROOM_MID,
                                          MEMBER_PK_CHATROOM_PKS, 
                                          CHATROOM_PK_MID)
            ######### Data 目录下 jp.naver.line ##############
            search_db_path = '/Library/Application Support/PrivateStore/'+account_file_name+'/Search Data/SearchData.sqlite'
            if self._read_db(self.root, search_db_path):
                self.parse_Search('ZRECENTSEARCHKEYWORD')

            feed_path = 'Library/Caches/PrivateStore/P_'+self.cur_account_id+'/jp.naver.myhome.MBDataResults/timeline'
            self.parse_Feed(self.root.GetByPath(feed_path))

    def parse_Account(self, user_plist_node, group_plist_node):
        ''' 解析两个 .plist

            \Library\Preferences\jp.naver.line.plist
            keys:
                SimCardInfo              # base64
                mid                      # account_id   u423af962f1456db6cba8465cf82bb91b
                migrationDate           
                name                     # nickname     王明
                uid                      # username     chrisnaruto
                statusMessage            # signatue
                tel                      # telephone
            Library\Preferences\group.com.linecorp.line.plist
            keys:
                LineAccountType             
                LineProfilePicturePath   # photo        startwith('/')
                LineProfilePictureStatus # photo
                mid                      # account_id   u423af962f1456db6cba8465cf82bb91b
        '''       
        account_list = []
        user_plist_res  = self._read_plist(user_plist_node, 'mid', 'name', 'uid', 'statusMessage', 'tel') 
        group_plist_res = self._read_plist(group_plist_node, 'LineProfilePicturePath') 
        
        if not user_plist_res:
            return 
        account = model_im.Account()
        account.account_id = self.cur_account_id = user_plist_res['mid']
        account.nickname   = user_plist_res['name']
        account.username   = user_plist_res['uid']
        account.signature  = user_plist_res['statusMessage']
        account.telephone  = user_plist_res['tel']
        pic_url = group_plist_res['LineProfilePicturePath']
        if pic_url and pic_url.startswith('/'):
            account.photo = self._search_profile_img(pic_url)        
        account.source     = self.user_plist_node.AbsolutePath
        account_list.append(account)
        try:
            self.im.db_insert_table_account(account)
        except:
            exc()
        self.im.db_commit()      

        return account_list

    def preparse_group_member(self, table_name):
        ''' save member_chatroom_map

            :rtype CHATROOM_MEMBER_COUNT: dict
        
            'Z_4MEMBERS' Z_4GROUPS 
                        
            RecNo	FieldName	SQLType	Size
            1	Z_4GROUPS	    INTEGER
            2	Z_12MEMBERS1	    INTEGER
        '''
        CHATROOM_MEMBER_COUNT = {}
        MEMBER_PK_CHATROOM_PKS = {}
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return ()
            if self._is_empty(rec, 'Z_4GROUPS'):
                continue    
            member_id = rec['Z_12MEMBERS1'].Value
            group_id = rec['Z_4GROUPS'].Value

            if MEMBER_PK_CHATROOM_PKS.has_key(member_id):
                if group_id not in MEMBER_PK_CHATROOM_PKS[member_id]:
                    MEMBER_PK_CHATROOM_PKS[member_id].append(group_id)
            else:
                MEMBER_PK_CHATROOM_PKS[member_id] = [group_id]
        for _, chatroom_pk_list in  MEMBER_PK_CHATROOM_PKS.iteritems():
            for chatroom_pk in chatroom_pk_list:
                if CHATROOM_MEMBER_COUNT.has_key(chatroom_pk):
                    CHATROOM_MEMBER_COUNT[chatroom_pk] += 1    
                else:
                    CHATROOM_MEMBER_COUNT[chatroom_pk] = 1
        return CHATROOM_MEMBER_COUNT, MEMBER_PK_CHATROOM_PKS
        
    def preparse_friend_tel(self, table_name):
        ''' /Messages/Line.sqlite  ZCONTACT 
                        
            RecNo	FieldName	SQLType	
            1	Z_PK	            INTEGER
            2	Z_ENT	            INTEGER
            3	Z_OPT	            INTEGER
            4	ZISINVITEABLE	            INTEGER
            5	ZISINVITED	            INTEGER
            6	ZISREMOVED	            INTEGER
            7	ZSERVERSYNCED	            INTEGER
            8	ZTYPE	            INTEGER
            9	ZCREATEDAT	            TIMESTAMP
            10	ZKEY	            VARCHAR
            11	ZLUID	            VARCHAR
            12	ZMID	            VARCHAR
            13	ZNAME	            VARCHAR
            14	ZPHONENUMBER        VARCHAR
            15	ZPHONETICNAME       VARCHAR
            16	ZSORTABLENAME       VARCHAR
        '''
        FRIEND_MID_TEL = {}
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'ZMID', 'ZPHONENUMBER', 'ZNAME'):
                continue    
            friend_mid  = rec['ZMID'].Value
            friend_tel = rec['ZPHONENUMBER'].Value

            if FRIEND_MID_TEL.has_key(friend_mid):
                if friend_tel not in FRIEND_MID_TEL[friend_mid]:
                    FRIEND_MID_TEL[friend_mid].append(friend_tel)
            else:
                FRIEND_MID_TEL[friend_mid] = [friend_tel]

        return FRIEND_MID_TEL

    def preparse_chat_friend_pk(self, table_name):
        ''' chat PK, friend PK,  & friend name later
        
            /Messages/Line.sqlite  Z_1MEMBERS 
                        
            RecNo	FieldName	SQLType	Size
            1	Z_1CHATS	    INTEGER
            2	Z_12MEMBERS	    INTEGER
        '''
        CHAT_PK_FRIEND_PKS = {}
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'Z_1CHATS', 'Z_12MEMBERS'):
                continue    
            chat_pk    = rec['Z_1CHATS'].Value
            friend_pk  = rec['Z_12MEMBERS'].Value
            # 按理说 如果是好友聊天, 一个 chat PK 对应 一个user Pk
            if CHAT_PK_FRIEND_PKS.has_key(chat_pk):
                if friend_pk not in CHAT_PK_FRIEND_PKS[chat_pk]:
                    CHAT_PK_FRIEND_PKS[chat_pk].append(friend_pk)
            else:
                CHAT_PK_FRIEND_PKS[chat_pk] = [friend_pk]
        return CHAT_PK_FRIEND_PKS
        
    def preparse_ZCHAT(self, table_name, CHATROOM_MID_NAME, CHAT_PK_FRIEND_PKS, FRIEND_PK_NAME_MAP):
        ''' parse table ZCHAT index chat list 
        
            return  self.CHAT_DICT {chat_pk: {'ZTYPE': type, 'ZMID': id}, ...}

            ZCHAT
                RecNo	FieldName	SQLType	Size	
                1	Z_PK	            INTEGER
                2	Z_ENT	            INTEGER
                3	Z_OPT	            INTEGER
                4	ZALERT	            INTEGER
                5	ZE2EECONTENTTYPES	            INTEGER
                6	ZENABLE	            INTEGER
                7	ZLASTRECEIVEDMESSAGEID	        INTEGER
                8	ZLIVE	            INTEGER
                9	ZREADUPTOMESSAGEID	            INTEGER
                10	ZREADUPTOMESSAGEIDSYNCED	    INTEGER
                11	ZSESSIONID	            INTEGER
                12	ZSORTORDER	            INTEGER
                13	ZTYPE	            INTEGER
                14	ZUNREAD	            INTEGER
                15	ZMETADATA	            INTEGER
                16	ZEXPIREINTERVAL	        FLOAT
                17	ZLASTUPDATED	        TIMESTAMP
                18	ZINPUTTEXT	            VARCHAR
                19	ZINVITERMID	            VARCHAR
                20	ZLASTMESSAGE	        VARCHAR
                21	ZMID	            VARCHAR
                22	ZSKIN	            VARCHAR         
        '''
        CHAT_DICT = {}
        CHAT_PK_CHATROOM_MID = {}
        CHATROOM_QUIT_PK = 1
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return ()
            if self._is_empty(rec, 'Z_PK', 'ZMID'):
                continue
            chat_pk   = rec['Z_PK'].Value
            chat_type = rec['ZTYPE'].Value
            chat_zmid = rec['ZMID'].Value
            chat_name = None
            if chat_type == 2: # 群
                if chat_zmid.startswith('c') and not CHATROOM_MID_NAME.has_key(chat_zmid):
                    chat_name = '已退出群{}'.format(CHATROOM_QUIT_PK)
                    CHATROOM_MID_NAME[chat_zmid]  = chat_name
                    CHAT_PK_CHATROOM_MID[chat_pk] = chat_zmid
                    CHATROOM_QUIT_PK += 1
                    # 已经删除的群
                    chatroom = model_im.Chatroom()
                    chatroom.account_id  = self.cur_account_id
                    chatroom.chatroom_id = chat_zmid
                    chatroom.name        = chat_name
                    chatroom.source      = self.cur_db_source
                    chatroom.deleted     = 1
                    try:
                        self.im.db_insert_table_chatroom(chatroom)
                    except:
                        exc()
                chat_name = CHATROOM_MID_NAME.get(chat_zmid, '已退出该群')
            elif chat_type == 0: # 好友
                friend_pk = CHAT_PK_FRIEND_PKS.get(chat_pk, [None])[0]
                chat_name = FRIEND_PK_NAME_MAP.get(friend_pk, None)
            CHAT_DICT[chat_pk] = {
                'ZTYPE'    : chat_type,
                'ZMID'     : chat_zmid, # 会话对象 MID
                'chat_name': chat_name, # 会话对象的名称, 例如好友, 群...
            }
        self.im.db_commit()                      
        return CHAT_DICT, CHATROOM_MID_NAME, CHAT_PK_CHATROOM_MID

    @print_run_time
    def parse_Chatroom(self, table_name, CHATROOM_MEMBER_COUNT):
        ''' account_id+'/Messages/Line.sqlite', 'ZGROUP'
            
            :type  CHATROOM_MEMBER_COUNT:
            :rtype CHATROOM_MID_NAME:
            :rtype CHATROOM_PK_MID:

        '''
        '''
                            RecNo	FieldName	SQLType	Size
                            1	Z_PK	                INTEGER
                            2	Z_ENT	                INTEGER
                            3	Z_OPT	                INTEGER
                            4	ZALERT	                INTEGER
                            5	ZENABLE	                INTEGER
                            6	ZFAVORITEORDER	                INTEGER
                            7	ZISACCEPTED	                INTEGER
                            8	ZISPREVENTLINKINVITATION	INTEGER
                            9	ZISVIEWED	                INTEGER
            creator_id      10	ZCREATOR	                INTEGER
            create_time     11	ZCREATEDTIME	                TIMESTAMP
            mid             12	ZID	                VARCHAR
                            13	ZLINKINVITATION	                VARCHAR
            name            14	ZNAME	                VARCHAR
            photo           15	ZPICTURESTATUS	                VARCHAR

                account_id         # 账号ID[TEXT]
                chatroom_id        # 群ID[TEXT]
                name               # 群名称[TEXT]
                photo              # 群头像[TEXT]
                type               # 群类型[INT]
                notice             # 群声明[TEXT]
                description        # 群描述[TEXT]
                creator_id         # 创建者[TEXT]
                owner_id           # 管理员[TEXT]
                member_count       # 群成员数量[INT]
                max_member_count   # 群最大成员数量[INT
                create_time        # 创建时间[INT]
        '''
        CHATROOM_MID_NAME = {}
        CHATROOM_PK_MID = {}
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return ()
            if self._is_empty(rec, 'ZNAME', 'ZID') or rec['ZISACCEPTED'].Value != 1:
                continue        
            chatroom = model_im.Chatroom()
            chatroom.account_id  = self.cur_account_id
            chatroom.chatroom_id = rec['ZID'].Value 
            chatroom.name        = rec['ZNAME'].Value
            pic_url = rec['ZPICTURESTATUS'].Value
            chatroom.photo = self._search_profile_img(pic_url) if pic_url else None 
            chatroom.creator_id       = str(rec['ZCREATOR'].Value)
            chatroom.create_time      = self._convert_ios_time(rec['ZCREATEDTIME'].Value)
            chatroom.member_count = CHATROOM_MEMBER_COUNT.get(rec['Z_PK'].Value, None)
            chatroom.deleted = 1 if rec.IsDeleted else 0
            chatroom.source  = self.cur_db_source            
            try:
                CHATROOM_MID_NAME[chatroom.chatroom_id] = chatroom.name
                CHATROOM_PK_MID[rec['Z_PK'].Value] = chatroom.chatroom_id
            except:
                exc_debug('self.CHATROOM_MID_NAME', CHATROOM_MID_NAME)
                exc_debug('chatroom.chatroom_id', chatroom.chatroom_id)
                exc()
            try:
                self.im.db_insert_table_chatroom(chatroom)
            except:
                exc()
        self.im.db_commit()  
        return CHATROOM_MID_NAME, CHATROOM_PK_MID

    @print_run_time
    def parse_Friend(self, table_name, FRIEND_MID_TEL):
        ''' 'Line.sqlite', 'ZUSER'
        '''
        '''
                            RecNo	FieldName	SQLType	
                            1	Z_PK	        INTEGER
                            2	Z_ENT	        INTEGER
                            3	Z_OPT	        INTEGER
                            4	ZALERT	        INTEGER
                            5	ZBLOCKING	        INTEGER
                            6	ZCAPABILITIES	        INTEGER
                            7	ZCONTACTTYPE	        INTEGER
                            8	ZE2EECONTENTTYPES	        INTEGER
                            9	ZFAVORITEORDER	        INTEGER
                            10	ZFRIENDREQUESTSTATUS	        INTEGER
                            11	ZISFRIEND	        INTEGER
                            12	ZISHIDDEN	        INTEGER
                            13	ZISINADDRESSBOOK	        INTEGER
                            14	ZISRECOMMENDED	        INTEGER
                            15	ZISREMOVED	        INTEGER
                            16	ZISUNREGISTERED	        INTEGER
                            17	ZISVIEWED	        INTEGER
                            18	ZMYHOMECAPABLE	        INTEGER
                            19	ZUSERTYPE	        INTEGER
                            20	ZVIDEOCALLCAPABLE	        INTEGER
                            21	ZVOICECALLCAPABLE	        INTEGER
                            22	ZCREATEDTIME	        TIMESTAMP
                            23	ZSTATUSUPDATEDAT	        TIMESTAMP
                            24	ZADDRESSBOOKNAME	        VARCHAR
                            25	ZCOUNTRY	        VARCHAR
                            26	ZCUSTOMNAME	        VARCHAR
                account_id  27	ZMID	        VARCHAR
                nickname    28	ZNAME	        VARCHAR
                            29	ZPICTURESTATUS	        VARCHAR
                            30	ZPICTUREURL	        VARCHAR
                            31	ZPROFILEIMAGE	        VARCHAR
                            32	ZRECOMMENDresS	        VARCHAR
                username    33	ZSORTABLENAME	        VARCHAR
                signature   34	ZSTATUSMESSAGE	        VARCHAR
                            35	ZE2EEPUBLICKEYCHAIN	        BLOB
                            36	ZPUBLICKEYCHAIN	        BLOB

            FRIEND_TYPE_NONE           = 0  # 未知
            FRIEND_TYPE_FRIEND         = 1  # 好友
            FRIEND_TYPE_GROUP_FRIEND   = 2  # 群好友
            FRIEND_TYPE_FANS           = 3  # 粉丝
            FRIEND_TYPE_FOLLOW         = 4  # 关注
            FRIEND_TYPE_SPECAIL_FOLLOW = 5  # 特别关注
            FRIEND_TYPE_MUTUAL_FOLLOW  = 6  # 互相关注
            FRIEND_TYPE_RECENT         = 7  # 最近
            FRIEND_TYPE_SUBSCRIBE      = 8  # 公众号
            FRIEND_TYPE_STRANGER       = 9  # 陌生人                    
        '''   
        FRIEND_PK_MID_MAP  = {}
        FRIEND_PK_NAME_MAP = {}   
        
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'ZNAME', 'ZSORTABLENAME', 'ZMID'):
                continue
            friend = model_im.Friend()
            friend.account_id = self.cur_account_id
            friend.friend_id  = rec['ZMID'].Value
            friend.nickname   = rec['ZNAME'].Value
            friend.username   = rec['ZSORTABLENAME'].Value
            friend.remark     = rec['ZCUSTOMNAME'].Value    # 备注[TEXT]
            friend.signature  = rec['ZSTATUSMESSAGE'].Value
            friend_pk                    = rec['Z_PK'].Value   # 关联 ZGROUP 表
            FRIEND_PK_MID_MAP[friend_pk] = friend.friend_id
            pic_url = rec['ZPICTUREURL'].Value
            if pic_url and pic_url.startswith('/'):
                friend.photo = self._search_profile_img(pic_url)
            # telephone 只取第一个
            if rec['ZISINADDRESSBOOK'].Value and rec['ZMID'].Value :  
                friend.telephone = FRIEND_MID_TEL.get(rec['ZMID'].Value, '')[0]
            if rec['ZISFRIEND'].Value: # 好友
                friend.type = model_im.FRIEND_TYPE_FRIEND
                if rec['ZFAVORITEORDER'].Value: # 特别关注
                    friend.type = model_im.FRIEND_TYPE_SPECAIL_FOLLOW
            else:
                friend.type = self._convert_friend_type(rec['ZCONTACTTYPE'].Value)
            friend.deleted = 1 if rec.IsDeleted or rec['ZISREMOVED'].Value else 0         
            friend.source  = self.cur_db_source
            # provide talker_name
            FRIEND_PK_NAME_MAP[friend_pk] = friend.nickname
            self.friend_list[friend_pk] = friend
            try:
                self.im.db_insert_table_friend(friend)
            except:
                exc()

        self.im.db_commit()
        return FRIEND_PK_NAME_MAP, FRIEND_PK_MID_MAP

    @print_run_time
    def parse_ChatroomMember(self, friend_list, DEL_FRIEND_PK_CHATROOM_MID, MEMBER_PK_CHATROOM_PKS, CHATROOM_PK_MID):
        ''' account_id+'/Messages/Line.sqlite', 'ZGROUP'

            :type friend: model_im.Friend()
            :type chatroom_mid: group mid
        '''
        '''
            'Z_4MEMBERS' Z_4GROUPS 
                        
            RecNo	FieldName	SQLType	Size
            1	Z_4GROUPS	    INTEGER
            2	Z_12MEMBERS1	    INTEGER
            
            self.chatroom_id  = None  # 群ID[TEXT]
            self.member_id    = None  # 成员ID[TEXT]
            self.display_name = None  # 群内显示名称[TEX
            self.photo     = None  # 头像[TEXT]
            self.telephone = None  # 电话[TEXT]
            self.email     = None  # 电子邮箱[TEXT]
            self.gender    = GENDER_NONE  # 性别[INT]
            self.age       = None  # 年龄[INT]
            self.address   = None  # 地址[TEXT]
            self.birthday  = None  # 生日[TEXT]
            self.signature = None  # 签名[TEXT]
        '''
        for friend_pk, friend in friend_list.iteritems():
            # M2M,  friend chatroom
            for chatroom_pk in MEMBER_PK_CHATROOM_PKS.get(friend_pk, [False]):

                if chatroom_pk:
                    chatroom_mid = CHATROOM_PK_MID.get(chatroom_pk, None)
                # 如果 chatroom_pk 不在 群与群成员关系表里, 则说明是 退出的群
                # 需要根据 ZCHAT, 即 ZCHAT 表的 pk, 关联 群id(ZMID)
                else: # pk 2 mid
                    chatroom_mid = DEL_FRIEND_PK_CHATROOM_MID.get(friend_pk, None)
                if not chatroom_mid:
                    continue
                cm = model_im.ChatroomMember()
                cm.account_id   = friend.account_id # 账户ID[TEXT]
                if chatroom_mid and chatroom_mid.startswith('c'):
                    cm.chatroom_id  = chatroom_mid
                cm.member_id    = friend.friend_id  # 成员ID[TEXT]
                cm.display_name = friend.remark if friend.remark else friend.nickname # 群内显示名称[TEXT]
                cm.photo        = friend.photo      # 头像[TEXT]
                cm.telephone    = friend.telephone  
                cm.email        = friend.email
                cm.gender       = friend.gender
                cm.age          = friend.age 
                cm.address      = friend.address
                cm.birthday     = friend.birthday
                cm.signature    = friend.signature  
                cm.deleted      = friend.deleted  
                cm.source       = friend.source  
                try:
                    self.im.db_insert_table_chatroom_member(cm)
                except:
                    exc()

    @print_run_time
    def parse_Message(self, table_name, FRIEND_PK_MID_MAP, FRIEND_PK_NAME_MAP, CHAT_DICT, CHAT_PK_FRIEND_PKS):
        ''' 'P_'+account_id+'/Messages/Line.sqlite', 'ZMESSAGE'
        '''
        '''
                            RecNo	FieldName	SQLType	Size	
                            1	Z_PK	            INTEGER
                chatroom_id 2	Z_ENT	            INTEGER       # FK 关联 ZGROUP
                            3	Z_OPT	            INTEGER
                type        4	ZCONTENTTYPE	    INTEGER
                            5	ZREADCOUNT	            INTEGER
                            6	ZSENDSTATUS	            INTEGER
                send_time   7	ZTIMESTAMP	            INTEGER
                talker_id   8	ZCHAT	            INTEGER       # 关联 ZCHAT
                sender_id   9	ZSENDER	            INTEGER
                            10	ZLATITUDE	            FLOAT
                            11	ZLONGITUDE	            FLOAT
                            12	ZID	                VARCHAR
                            13	ZMESSAGETYPE	            VARCHAR
                content     14	ZTEXT	            VARCHAR
                            15	ZCONTENTMETADATA	BLOB          
                            16	ZTHUMBNAIL	            BLOB
            self.account_id  = None  # 账号ID[TEXT]
            self.talker_id   = None  # 会话ID[TEXT]
            self.talker_name = None  # 会话昵称[TEXT]
            self.sender_id   = None  # 发送者ID[TEXT]
            self.sender_name = None  # 发送者昵称[TEXT]
            self.is_sender   = None  # 自己是否为发送发[INT]
            self.msg_id      = None  # 消息ID[TEXT]
            self.type        = None  # 消息类型[INT]，MESSAGE_CONTENT_TYPE
            self.content     = None  # 内容[TEXT]
            self.media_path  = None  # 媒体文件地址[TEXT]
            self.send_time   = None  # 发送时间[INT]
            self.extra_id    = None  # 扩展ID[TEXT] 地址类型指向location_id、交易类型指向deal_i
            self.status      = None  # 消息状态[INT]，MESSAGE_STATUS
            self.talker_type = None  # 聊天类型[INT]，CHAT_TYPE
        '''
        DEL_FRIEND_PK_CHATROOM_MID = {}
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'ZTIMESTAMP', 'ZCHAT'):
                continue        
            message = model_im.Message()
            message.account_id  = self.cur_account_id
            message.msg_id      = rec['ZID'].Value
            sender_pk           = rec['ZSENDER'].Value
            message.sender_id   = FRIEND_PK_MID_MAP.get(sender_pk, None)
            message.sender_name = FRIEND_PK_NAME_MAP.get(sender_pk, None)
            message.content     = rec['ZTEXT'].Value
            message.send_time   = self._get_im_ts(rec['ZTIMESTAMP'].Value)
            message.status      = self._convert_msg_status(rec['ZSENDSTATUS'].Value)

            # MESSAGE_CONTENT_TYPE
            if IsDBNull(rec['ZSENDER'].Value) and IsDBNull(rec['ZID'].Value):
                message.type = model_im.MESSAGE_CONTENT_TYPE_SYSTEM
            else:            
                message.type = self._convert_msg_type(rec['ZCONTENTTYPE'].Value)

            if rec['Z_OPT'].Value==3 or (rec['ZCONTENTTYPE'].Value!=18 and not sender_pk and rec['ZID'].Value):
                message.is_sender = 1
                message.sender_id = self.cur_account_id
            # CHAT_TYPE 区分是 好友聊天还是群聊天 CHAT_TYPE, 2 是群, 0 是好友
            try:
                ZCHAT_ZTYPE = CHAT_DICT.get(rec['ZCHAT'].Value, {}).get('ZTYPE', None)
                message.talker_type = self._convert_chat_type(ZCHAT_ZTYPE)
            except:
                exc()
            message.talker_id = rec['ZCHAT'].Value

            # 获取已删除的群与群成员关系, 即 ZMESSAGE.ZCHAT: ZMESSAGE.sender_mid
            # 如果 ZCHAT 对应的 ZCHAT 对应的 chatroom mid 不存在于 ZGROUP 表中, 即已删除
            if message.talker_type == model_im.CHAT_TYPE_GROUP:
                # CHAT_PK_CHATROOM_MID
                msg_chat_pk = rec['ZCHAT'].Value
                if sender_pk and CHAT_DICT.get(msg_chat_pk, {}).get('chat_name', '').startswith('已退出'):
                    DEL_FRIEND_PK_CHATROOM_MID[sender_pk] = CHAT_DICT.get(msg_chat_pk, {}).get('ZMID', None)
                message.talker_name = CHAT_DICT.get(msg_chat_pk, {}).get('chat_name', None)
            else: # 非群聊
                chat_pk   = rec['ZCHAT'].Value
                friend_pks = CHAT_PK_FRIEND_PKS.get(chat_pk, None)
                message.talker_name = FRIEND_PK_NAME_MAP.get(friend_pks[0], None)

            if message.content and message.content[-4:] in ['.m4a', '.mp4']:
                # 本人发的语音, 视频, 保留在 /tmp, 文件名不变 self.root + tmp/_3997735.m4a
                message.media_path = message.content
            elif message.type in [model_im.MESSAGE_CONTENT_TYPE_IMAGE, 
                                  model_im.MESSAGE_CONTENT_TYPE_VOICE,
                                  model_im.MESSAGE_CONTENT_TYPE_VIDEO,
                                  model_im.MESSAGE_CONTENT_TYPE_ATTACHMENT]:
                msg_ZCHAT = rec['ZCHAT'].Value                
                msg_ZID   = rec['ZID'].Value
                if msg_ZID and msg_ZCHAT:
                    message.media_path = self._get_msg_media_path(CHAT_DICT, msg_ZCHAT, msg_ZID)
            # 位置
            if message.type == model_im.MESSAGE_CONTENT_TYPE_LOCATION:
                location = model_im.Location()
                message.extra_id   = location.location_id
                location.latitude  = rec['ZLATITUDE'].Value
                location.longitude = rec['ZLONGITUDE'].Value
                location.address   = rec['ZTEXT'].Value
                location.timestamp = self._get_im_ts(rec['ZTIMESTAMP'].Value)
                location.source    = self.cur_db_source
                try:
                    self.im.db_insert_table_location(location)
                except:
                    exc()
            message.source  = self.cur_db_source
            message.deleted = 1 if rec.IsDeleted else 0         
            try:
                self.im.db_insert_table_message(message)
            except:
                exc()
        self.im.db_commit()  
        return DEL_FRIEND_PK_CHATROOM_MID

    def parse_Feed(self, plist_node=None):
        ''' 5A249183-668C-4CC0-B983-C0A7EA2E657F\
                Library\Caches\PrivateStore\
                    P_u423af962f1456db6cba8465cf82bb91b\jp.naver.myhome.MBDataResults\
            myhomelist  
            timeline
        '''
        '''
            self.account_id = None  # 账号ID[TEXT]
            self.sender_id  = None  # 发布者ID[TEXT]
            self.type       = None  # 动态类型[INT]
            self.content    = None  # 动态内容[TEXT]
            self.media_path = None  # 媒体文件地址[TEXT]
            self.urls       = None  # 链接地址[json TEXT] json string ['url1', 'url2'...]
            # 预览地址[json TEXT] json string ['url1', 'url2'...]
            self.preview_urls     = None
            self.attachment_title = None  # 附件标题[TEXT]
            self.attachment_link  = None  # 附件链接[TEXT]
            self.attachment_desc  = None  # 附件描述[TEXT]
            self.send_time        = None  # 发布时间[INT]
            self.likes            = None  # 赞[TEXT] 逗号分隔like_id 例如：like_id, like_id, like_id, ...
            self.likecount        = None  # 赞数量[INT]
            self.rtcount          = None  # 转发数量[INT]
            # 评论[TEXT] 逗号分隔comment_id 例如：comment_id,comment_id,comment_id,...
            self.comments     = None
            self.commentcount = None  # 评论数量[INT]
            self.device       = None  # 设备名称[TEXT]
            self.location     = None  # 地址ID[TEXT]
        '''       
        def _print_plist(n, ind=''):
            print ind+'key: {}'.format(n.Key)
            if n.Values:
                for chch in n.Values:
                    _print_plist(chch, ind=ind+'|----')
            else:
                exc_debug(ind+'val:{}'.format(n.Value))
                
        bplist = BPReader.GetTree(plist_node.Data) if plist_node else None
        if not bplist:
            return 
            
        for feed_node in bplist:
            # _print_plist(feed_node)
            feed = model_im.Feed()
            feed.account_id = self.cur_account_id                   # 账号ID[TEXT]
            feed.sender_id  = feed_node['fromUser'].Value           # 发布者ID[TEXT]
            feed.content    = feed_node['contents']['text'].Value   # 动态内容[TEXT]
            media_node      = feed_node['contents']['media']
            try:
                for key in ('photos', 'medias', 'videos'):
                    if media_node and media_node[key]:
                        for each_media_node in media_node[key]:
                            # 媒体文件地址[TEXT] .Value ['objectID']
                            feed.media_path = each_media_node['sourceURL'].Value  
            except:
                exc()
                # _print_plist(feed_node)

            if feed_node['contents']['additionalContents']['url']:
                feed.urls = feed_node['contents']['additionalContents']['url']['targetUrl'].Value   # 动态内容[TEXT]
            feed.send_time = feed_node['postInfo']['createdTime'].Value  # 发布时间[INT]
            feed.likecount = feed_node['postInfo']['likeCount'].Value  # 赞数量[INT]
            feed.rtcount   = feed_node['postInfo']['sharedCount']['toPost'].Value \
                           + feed_node['postInfo']['sharedCount']['toTalk'].Value  # 转发数量[INT]
            feed.commentcount = feed_node['postInfo']['commentCount'].Value   # 评论数量[INT]
            # feed.type = MOMENT_TYPE_IMAGE  # 动态类型[INT] MOMENT_TYPE

            # location
            loc_node = feed_node['contents']['textLocation']['location']
            if loc_node and loc_node['latitude'] and loc_node['longitude']:
                location = model_im.Location()
                location.latitude  = loc_node['latitude'].Value
                location.longitude = loc_node['longitude'].Value
                try:
                    self.im.db_insert_table_location(location)
                except:
                    exc()            
                feed.location = location.location_id  # 地址ID[TEXT]
            feed.source = plist_node.AbsolutePath
            try:
                self.im.db_insert_table_feed(feed)
            except:
                exc()
        self.im.db_commit()       

    def parse_Search(self, table_name):
        ''' # 5A249183-668C-4CC0-B983-C0A7EA2E657F
            # \Library\Application Support\PrivateStore\P_u423af962f1456db6cba8465cf82bb91b\Search Data
            # \SearchData.sqlite

            ZRECENTSEARCHKEYWORD
            RecNo	FieldName	SQLType	Size	Precision	PKDisplay	DefaultValue	NotNull	NotNullConflictClause	Unique	UniqueConflictClause	CollateValue	FKDisplay
            1	Z_PK	        INTEGER
            2	Z_ENT	        INTEGER
            3	Z_OPT	        INTEGER
            4	ZIDX	        INTEGER
            5	ZTYPE	        INTEGER
            6	ZTIME	        TIMESTAMP
            7	ZSEARCHKEYWORD  VARCHAR   	

            self.account_id  = None  # 账号ID[TEXT]
            self.key         = None  # 搜索关键字[TEXT]
            self.create_time = None  # 搜索时间[INT]            
        '''
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'ZSEARCHKEYWORD'):
                continue              
            search = model_im.Search()
            search.account_id = self.cur_account_id
            search.key        = rec['ZSEARCHKEYWORD'].Value
            search.source     = self.cur_db_source
            search.deleted    = 1 if rec.IsDeleted else 0               
            try:
                self.im.db_insert_table_search(search)
            except:
                exc()
        self.im.db_commit()                        

    @staticmethod
    def _read_plist(plist_node, *keys):
        ''' read .plist file 

        :type plist_node: node
        :type keys: tuple(*str)
        :rtype: tuple(*str)
        '''
        res = {}
        bp = BPReader(plist_node.Data).top    
        try:
            for k in keys:
                # exc_debug(k)
                if bp[k]:
                    res[k] = bp[k].Value
                    # exc_debug(res[k])
            return res
        except:
            exc()
            return res

    def _search_profile_img(self, file_name):
        # 用户, 好友, 群 头像 存储位置: \F8B8...
        # \Library\Caches\PrivateStore\P_u423af962f1456db6cba8465cf82bb91b\Profile Images\ZMID
        if file_name[0] != '/':
            file_name = '/' + file_name
        pp = '/Library/Caches/PrivateStore/P_' \
             + self.cur_account_id + r'/Profile Images' \
             + file_name
        #exc_debug('friend pic url pattern ', pp)

        _node = self.group_root.GetByPath(pp)
        if _node:
            for file_node in _node.Children:
                file_path = file_node.AbsolutePath
                if file_path.split('/')[-1].endswith('.jpg'):
                    # exc_debug(file_path)
                    return file_path
        return 

    def _get_msg_media_path(self, CHAT_DICT, msg_ZCHAT, msg_ZID):
        ''' 获取聊天 media_path
        
            rec['ZCHAT'].Value  
            rec['ZID'].Value  
            
            下载的图片:
            5A24...\Library\Application Support\PrivateStore\P_u423...\Message Attachments\u1f...
            缓存的聊天图片:
            F8B8...\Library\Application Support\PrivateStore\P_u423...\Message Thumbnails\u1f...         
        '''
        download_media_path = '/Library/Application Support/PrivateStore/P_' \
                        + self.cur_account_id + r'/Message Attachments/' \
                        + CHAT_DICT.get(msg_ZCHAT, {}).get('ZMID', '')
        download_media_node = self.root.GetByPath(download_media_path)
        if download_media_node:
            for file_node in download_media_node.Children:
                file_path = file_node.AbsolutePath
                if file_path.split('/')[-1].startswith(msg_ZID):
                    # exc_debug(file_path)
                    return file_path
        cache_media_path = '/Library/Application Support/PrivateStore/P_' \
                        + self.cur_account_id + r'/Message Thumbnails/' \
                        + CHAT_DICT.get(msg_ZCHAT, {}).get('ZMID', '') \
                        + '/' + msg_ZID + '.thumb'
        cache_media_node = self.group_root.GetByPath(cache_media_path)
        if cache_media_node:
            file_path = cache_media_node.AbsolutePath
            return file_path        
        return 

    def _read_db(self, db_root, db_path):
        """ 读取手机数据库

        :type db_path: str
        :rtype: bool                              
        """
        db_node = db_root.GetByPath(db_path)
        self.cur_db = SQLiteParser.Database.FromNode(db_node, canceller)
        if self.cur_db is None:
            return False
        self.cur_db_source = db_node.AbsolutePath
        return True

    def _read_table(self, table_name):
        """ 读取手机数据库 - 表

        :type table_name: str
        :rtype: db.ReadTableRecords()                                       
        """
        try:
            tb = SQLiteParser.TableSignature(table_name)  
            return self.cur_db.ReadTableRecords(tb, self.extract_deleted, True)
        except:
            exc()         
            return []

    @staticmethod
    def _is_empty(rec, *args):
        ''' 过滤 DBNull 空数据, 有一空值就跳过
        
        :type rec:   rec
        :type *args: str
        :rtype: bool
        '''
        for i in args:
            if IsDBNull(rec[i].Value) or not rec[i].Value:
                return True
        return False
        
    @staticmethod
    def _convert_chat_type(ZTYPE):
        ''' ZCHAT 'ZTYPE' field to model_im.py CHAT_TYPE
        
        :type ZTYPE: int
        :rtype: int
        '''
        type_map = {
            0: model_im.CHAT_TYPE_FRIEND,
            2: model_im.CHAT_TYPE_GROUP,
        }
        try:
            return type_map[ZTYPE]
        except:
            exc_debug('new CHAT_TYPE {}!!!!!!!!!!!!!!!!!'.format(ZTYPE))

    @staticmethod
    def _convert_msg_type(ZCONTENTTYPE):
        '''ZMESSAGE  'ZMESSAGETYPE' field to model_im CHAT_TYPE
        
        :type ZTYPE: int
        :rtype: int
        '''
        type_map = {
            0: model_im.MESSAGE_CONTENT_TYPE_TEXT,        # TEXT
            1: model_im.MESSAGE_CONTENT_TYPE_IMAGE,       # 图片
            2: model_im.MESSAGE_CONTENT_TYPE_VIDEO,       # 视频
            3: model_im.MESSAGE_CONTENT_TYPE_VOICE,       # 语音
            # 5: model_im.MESSAGE_CONTENT_TYPE_,            
            6: model_im.MESSAGE_CONTENT_TYPE_VOIP,        # 网络电话
            7: model_im.MESSAGE_CONTENT_TYPE_EMOJI,       # 表情
            # 13: model_im.MESSAGE_CONTENT_TYPE_,
            14: model_im.MESSAGE_CONTENT_TYPE_ATTACHMENT, # 附件
            # 16: model_im.MESSAGE_CONTENT_TYPE_,
            18: model_im.MESSAGE_CONTENT_TYPE_SYSTEM,     # 删除图片
            100: model_im.MESSAGE_CONTENT_TYPE_LOCATION,  # 位置
        }
        try:
            return type_map[ZCONTENTTYPE]
        except:
            exc_debug('new ZCONTENTTYPE {}!!!!!!!!!!!!!!!!!'.format(ZCONTENTTYPE))

    @staticmethod
    def _convert_msg_status(ZSENDSTATUS):
        '''ZMESSAGE  'ZSENDSTATUS' field to model_im MESSAGE_STATUS_
        
        :type ZTYPE: int
        :rtype: int
        '''
        if not ZSENDSTATUS:
            return        
        type_map = {
            0:  model_im.MESSAGE_STATUS_DEFAULT,        # 
            1:  model_im.MESSAGE_STATUS_READ,           #            
            2:  model_im.MESSAGE_STATUS_UNSENT,         # 
            # 3:  model_im.MESSAGE_STATUS_UNREAD,         # 
            # 2:  model_im.MESSAGE_STATUS_SENT,           # 
        }
        try:
            return type_map[ZSENDSTATUS]
        except:
            exc_debug('new ZSENDSTATUS {}!!!!!!!!!!!!!!!!!'.format(ZSENDSTATUS))

    @staticmethod
    def _convert_friend_type(ZCONTACTTYPE):
        '''ZUSER 'ZCONTACTTYPE' field to model_im FRIEND_TYPE_
        
        :type ZTYPE: int
        :rtype: int
        '''
        if not ZCONTACTTYPE:
            return    
        type_map = {
            # 1: model_im.FRIEND_TYPE_ ,             # = 0   通讯录好友
            0: model_im.FRIEND_TYPE_NONE,           # = 0   未知
            # 2: model_im.FRIEND_TYPE_FRIEND,         # = 1   好友
            # 3: model_im.FRIEND_TYPE_FANS,           # = 3   粉丝
            # 4: model_im.FRIEND_TYPE_FOLLOW,         # = 4   关注
            5: model_im.FRIEND_TYPE_GROUP_FRIEND,   # = 2   群好友
            # 6: model_im.FRIEND_TYPE_SPECAIL_FOLLOW, # = 5   特别关注
            # 7: model_im.FRIEND_TYPE_MUTUAL_FOLLOW,  # = 6   互相关注
            8: model_im.FRIEND_TYPE_SUBSCRIBE,      # = 8   官方账号 - 公众号
            # 9: model_im.FRIEND_TYPE_RECENT,         # = 7   最近
            # 10: model_im.FRIEND_TYPE_STRANGER,       # = 9   陌生人       
        }
        try:
            return type_map[ZCONTACTTYPE]
        except:
            exc_debug('new ZCONTACTTYPE {}!!!!!!!!!!!!!!!!!'.format(ZCONTACTTYPE))

    @staticmethod
    def _convert_send_status(ZSENDSTATUS):
        '''ZMESSAGE  'ZSENDSTATUS' field to model_im MESSAGE_STATUS_
        
        :type ZTYPE: int
        :rtype: int
        
        MESSAGE_STATUS_DEFAULT = 0
        MESSAGE_STATUS_UNSENT  = 1
        MESSAGE_STATUS_SENT    = 2
        MESSAGE_STATUS_UNREAD  = 3
        MESSAGE_STATUS_READ    = 4
        '''
        if not ZSENDSTATUS:
            return           
        type_map = {
            0: model_im.MESSAGE_STATUS_DEFAULT,
            1: model_im.MESSAGE_STATUS_READ ,      
            2: model_im.MESSAGE_STATUS_UNSENT,  
        }
        try:
            return type_map[ZSENDSTATUS]
        except:
            exc_debug('new ZSENDSTATUS {}!!!!!!!!!!!!!!!!!'.format(ZSENDSTATUS))

    @staticmethod
    def _convert_ios_time(timestamp):
        ''' convert_ios_time

        :type timestamp: float, Int64  9 digit
        :rtype unixtime: int           13 digit
        '''
        if timestamp < 0 or len(str(int(timestamp))) != 9:
            return 
        dstart = DateTime(1970,1,1,0,0,0)
        cdate = TimeStampFormats.GetTimeStampEpoch1Jan2001(timestamp)
        uinixtime = int((cdate - dstart).TotalSeconds)
        return uinixtime

    @staticmethod
    def _get_im_ts(timestamp):
        ''' convert_ts 13=>10
        '''
        try:
            if isinstance(timestamp, (int, long, float, Int64)) and len(str(timestamp))==13:
                return int(str(timestamp)[:10])
        except:
            exc()
            return 
        