# coding=utf-8
import traceback
import hashlib
import re

from PA_runtime import *

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
MOMENT_TYPE_IMAGE = 1  # 正常文字图片
MOMENT_TYPE_TEXT_ONLY = 2  # 纯文字
MOMENT_TYPE_SHARED = 3  # 分享
MOMENT_TYPE_MUSIC = 4  # 带音乐的（存的是封面）
MOMENT_TYPE_EMOJI = 10  # 分享了表情包
MOMENT_TYPE_VIDEO = 15  # 视频

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
    pr = ParserResults()
    res = []
    try:
        res = LineParser(node, extract_deleted, extract_source).parse()
    except:
        exc()
    if res:
        pr.Models.AddRange(res)
        pr.Build('LINE')
    #return pr

class LineParser(object):
    def __init__(self, node, extract_deleted, extract_source):
        ''' boundId: 
            
            jp.naver.line    
                node: /private/var/mobile/Containers/Data/Application/5A249183-668C-4CC0-B983-C0A7EA2E657F
                Library\Preferences\jp.naver.line.plist

            group.com.linecorp.line
                Library\Preferences\group.com.linecorp.line.plist
                Library\\Application Support\\PrivateStore\\P_u423af962f145 6db6cba8465cf82bb91b\\Messages\\Line.sqlite    
        '''
        # print('node.AbsolutePath:', node.AbsolutePath)
        self.root = node
        self.user_plist_node = self.root.GetByPath('Library/Preferences/jp.naver.line.plist')

        ################# group.com.linecorp.line ################
        # Library\Preferences\group.com.linecorp.line.plist
        self.group_plist_node = list(self.root.FileSystem.Search('group.com.linecorp.line.plist$'))[0]
        # print(type(self.group_plist_node)) 
        # print(self.group_plist_node.AbsolutePath)
        # Library\\Application Support\\PrivateStore
        self.group_root = self.group_plist_node.Parent.Parent.Parent
        self.group_db_root = self.group_root.GetByPath('Library/Application Support/PrivateStore')

        self.extract_deleted = extract_deleted
        self.extract_source  = extract_source
        self.im = model_im.IM()        
        self.cachepath = ds.OpenCachePath("LINE")

        hash_str = hashlib.md5(self.root.AbsolutePath).hexdigest()
        self.cache_db = self.cachepath + '\\{}.db'.format(hash_str)

        self.account_list = []
        self.chatroom_map = {}          # 群 ZID: ZNAME
        self.member_chatroom_map = {}   # 群成员PK: 群PK
        self.friend_tel_map = {}        # 群成员PK: 群PK


    def parse(self):
        ''' account
            contact
            mail
            attachment
            search
            vsersion
        ''' 
        if DEBUG or self.im.need_parse(self.cache_db, VERSION_APP_VALUE):

            # if self.pre_parse_plist(plist_node=self.user_plist_node) == False:
            #     return []
            # print(self.account_list)
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
        ''' '''
        self.parse_Account(self.user_plist_node, self.group_plist_node)


        for account in self.account_list:
            self.cur_account_id = account.account_id

            account_file_name = 'P_' + account.account_id
            self.pre_parse_group_member(self.group_db_root, 
                                        account_file_name+'/Messages/Line.sqlite', 
                                        'Z_4MEMBERS')
            self.pre_parse_friend_tel(self.group_db_root, 
                                        account_file_name+'/Messages/Line.sqlite', 
                                        'ZCONTACT')

            self.parse_Chatroom(self.group_db_root, 
                                account_file_name+'/Messages/Line.sqlite', 
                                'ZGROUP')
            self.parse_Message(self.group_db_root, 
                               account_file_name+'/Messages/Line.sqlite', 
                               'ZMESSAGE')
            self.parse_Friend_ChatroomMember(self.group_db_root, 
                                             account_file_name+'/Messages/Line.sqlite', 
                                             'ZUSER')
            self.parse_Feed(self.root.GetByPath('Library/Caches/PrivateStore/P_' \
                                                + self.cur_account_id  \
                                                + '/jp.naver.myhome.MBDataResults/timeline'))
            #self.parse_FeedLike('', '')
            #self.parse_FeedComment('', '')
            #self.parse_Location('', '')
            #self.parse_Deal('', '')

            # 5A249183-668C-4CC0-B983-C0A7EA2E657F
            # \Library\Application Support\PrivateStore\P_u423af962f1456db6cba8465cf82bb91b\Search Data\SearchData.sqlite
            self.parse_Search(self.root, 
                             '/Library/Application Support/PrivateStore/' + account_file_name + '/Search Data/SearchData.sqlite', 
                             'ZRECENTSEARCHKEYWORD')


    def parse_Search(self, db_root, db_path, table_name):
        ''' 5A249183-668C-4CC0-B983-C0A7EA2E657F\Library\Application Support\PrivateStore\P_u423af962f1456db6cba8465cf82bb91b\Search Data\SearchData.sqlite
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
        if not self._read_db(db_root, db_path):
            return 
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'ZSEARCHKEYWORD'):
                continue              
            search = model_im.Search()
            search.account_id  = self.cur_account_id
            search.key         = rec['ZSEARCHKEYWORD'].Value
            search.create_time = rec['ZTIME'].Value
            search.source = self.cur_db_source
            search.deleted = 1 if rec.IsDeleted else 0               
            try:
                self.im.db_insert_table_search(search)
            except:
                exc()
        self.im.db_commit()                        


    def pre_parse_friend_tel(self, db_root, db_path, table_name):
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
        if not self._read_db(db_root, db_path):
            return 
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'ZMID', 'ZPHONENUMBER', 'ZNAME'):
                continue    
            friend_id  = rec['ZMID'].Value
            friend_tel = rec['ZPHONENUMBER'].Value

            if self.friend_tel_map.has_key(friend_id):
                if friend_tel not in self.friend_tel_map[friend_id]:
                    self.friend_tel_map[friend_id].append(friend_tel)
            else:
                self.friend_tel_map[friend_id] = [friend_tel]

    def pre_parse_group_member(self, db_root, db_path, table_name):
            ''' 'Z_4MEMBERS' Z_4GROUPS 
                            
                RecNo	FieldName	SQLType	Size
                1	Z_4GROUPS	    INTEGER
                2	Z_12MEMBERS1	    INTEGER
            '''
            if not self._read_db(db_root, db_path):
                return 
            for rec in self._read_table(table_name):
                if canceller.IsCancellationRequested:
                    return
                if self._is_empty(rec, 'Z_4GROUPS'):
                    continue    
                member_id = rec['Z_12MEMBERS1'].Value
                group_id = rec['Z_4GROUPS'].Value

                if self.member_chatroom_map.has_key(member_id):
                    if group_id not in self.member_chatroom_map[member_id]:
                        self.member_chatroom_map[member_id].append(group_id)
                else:
                    self.member_chatroom_map[member_id] = [group_id]

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

        # account.deleted   =
        account.source     = self.user_plist_node.AbsolutePath

        self.account_list.append(account)
        try:
            self.im.db_insert_table_account(account)
        except:
            exc()
        self.im.db_commit()       


    def parse_Feed(self, plist_node=None):
        ''' \5A249183-668C-4CC0-B983-C0A7EA2E657F\
                Library\Caches\PrivateStore\
                    P_u423af962f1456db6cba8465cf82bb91b\jp.naver.myhome.MBDataResults\
            myhomelist  
            timeline

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
                print ind+'val:{}'.format(n.Value)
                
        bplist = BPReader.GetTree(plist_node.Data)
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
                            feed.media_path = each_media_node['sourceURL'].Value  # 媒体文件地址[TEXT] .Value ['objectID']
            except:
                exc()
                # _print_plist(feed_node)

            exc_debug(feed.media_path)

            if feed_node['contents']['additionalContents']['url']:
                feed.urls = feed_node['contents']['additionalContents']['url']['targetUrl'].Value   # 动态内容[TEXT]
            # feed.preview_urls     = None  # 预览地址[json TEXT] json string ['url1', 'url2'...]
            # feed.attachment_title = None  # 附件标题[TEXT]
            # feed.attachment_link  = None  # 附件链接[TEXT]
            # feed.attachment_desc  = None  # 附件描述[TEXT]
            feed.send_time    = feed_node['postInfo']['createdTime'].Value  # 发布时间[INT]
            # TODO feed.likes        = None  # 赞[TEXT] 逗号分隔like_id 例如：like_id, like_id, like_id, ...
            feed.likecount    = feed_node['postInfo']['likeCount'].Value  # 赞数量[INT]
            feed.rtcount = feed_node['postInfo']['sharedCount']['toPost'].Value \
                        + feed_node['postInfo']['sharedCount']['toTalk'].Value  # 转发数量[INT]
            # feed.comments     = None # [TEXT] 逗号分隔comment_id 例如：comment_id,comment_id,comment_id,...
            feed.commentcount = feed_node['postInfo']['commentCount'].Value   # 评论数量[INT]
            # feed.device       = None  # 设备名称[TEXT]
            # feed.type = MOMENT_TYPE_IMAGE  # 动态类型[INT] MOMENT_TYPE

            loc_node = feed_node['contents']['textLocation']['location']
            if loc_node and loc_node['latitude'] and loc_node['longitude']:
                location = model_im.Location()
                location.latitude = loc_node['latitude'].Value
                location.longitude = loc_node['longitude'].Value
                try:
                    self.im.db_insert_table_location(location)
                except:
                    exc()            
                feed.location = location.location_id  # 地址ID[TEXT]
            try:
                self.im.db_insert_table_feed(feed)
            except:
                exc()
        self.im.db_commit()       

    def parse_Message(self, db_root, db_path, table_name):
        ''' 'P_'+account_id+'/Messages/Line.sqlite', 'ZMESSAGE'

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
                        15	ZCONTENTMETADATA	BLOB          # bplist 图片, 视频, 语音
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
        CHAT_TYPE_DICT = self.parse_Message_ZCHAT('ZCHAT')

        if not self._read_db(db_root, db_path):
            return 
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'ZTIMESTAMP', 'ZCHAT'):
                continue        
            message = model_im.Message()
            message.account_id  = self.cur_account_id
            message.msg_id      = rec['ZID'].Value
            message.sender_id   = rec['ZSENDER'].Value
            message.content     = rec['ZTEXT'].Value
            message.send_time   = rec['ZTIMESTAMP'].Value if len(str(rec['ZTIMESTAMP'].Value)) in (10, 13) else None
            message.type        = self._convert_msg_type(rec['ZCONTENTTYPE'].Value)
            message.status      = self._convert_msg_status(rec['ZSENDSTATUS'].Value)

            if rec['Z_OPT'].Value==3 or (rec['ZCONTENTTYPE'].Value!=18 and not rec['ZSENDER'].Value and rec['ZID'].Value):
                message.is_sender = 1


            message.talker_id   = rec['ZCHAT'].Value
            message.talker_name = CHAT_TYPE_DICT.get(rec['ZCHAT'].Value, {}).get('chat_name', None)

            #TODO 区分是 好友聊天还是群聊天 CHAT_TYPE, 2 是群, 0 是好友
            message.talker_type = self._convert_chat_type(CHAT_TYPE_DICT.get(rec['ZCHAT'].Value, {}).get('ZTYPE', None))
            if message.content and message.content[-4:] in ['.m4a', '.mp4']:
                # 自己发的语音, 视频, 保留在 /tmp, 文件名不变 self.root + tmp/_3997735.m4a
                message.media_path = message.content
            elif message.type in [model_im.MESSAGE_CONTENT_TYPE_IMAGE, 
                                  model_im.MESSAGE_CONTENT_TYPE_VOICE,
                                  model_im.MESSAGE_CONTENT_TYPE_VIDEO,
                                  model_im.MESSAGE_CONTENT_TYPE_ATTACHMENT]:
                msg_ZCHAT = rec['ZCHAT'].Value                
                msg_ZID   =  rec['ZID'].Value
                if msg_ZID and msg_ZCHAT:
                    message.media_path = self._get_msg_media_path(CHAT_TYPE_DICT, msg_ZCHAT, msg_ZID)
            # 位置
            if message.type == model_im.MESSAGE_CONTENT_TYPE_LOCATION:
                location = model_im.Location()
                message.extra_id   = location.location_id
                location.latitude  = rec['ZLATITUDE'].Value
                location.longitude = rec['ZLONGITUDE'].Value
                location.address   = rec['ZTEXT'].Value
                location.timestamp = rec['ZTIMESTAMP'].Value
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


    def parse_Message_ZCHAT(self, table_name):
        ''' parse table ZCHAT index chat list 
        
            return  {chat_id: {'ZTYPE': type, 'ZMID': id}, ...}

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
        CHAT_TYPE_DICT = {}
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            # if self._is_empty(rec, 'ZTEXT'):
            #     continue        
            chat_id   = rec['Z_PK'].Value
            chat_type = rec['ZTYPE'].Value
            chat_zmid = rec['ZMID'].Value
            chat_name = self.chatroom_map.get(rec['ZMID'].Value, '')

            CHAT_TYPE_DICT[chat_id] = {
                'ZTYPE'    : chat_type,
                'ZMID'     : chat_zmid,
                'chat_name': chat_name, # 会话对象的名称, 例如好友, 群...
            }
        return CHAT_TYPE_DICT

    def parse_Chatroom(self, db_root, db_path, table_name):
        ''' account_id+'/Messages/Line.sqlite', 'ZGROUP'

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
                            12	ZID	                VARCHAR
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
        if not self._read_db(db_root, db_path):
            return 
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'ZNAME', ) or rec['ZISACCEPTED'].Value != 1:
                continue        
            chatroom = model_im.Chatroom()
            chatroom.account_id        = self.cur_account_id
            chatroom.chatroom_id       = rec['ZID'].Value
            chatroom.name              = rec['ZNAME'].Value
            pic_url = rec['ZPICTURESTATUS'].Value
            
            chatroom.photo = self._search_profile_img(pic_url) if pic_url else None 
            # chatroom.type              = rec['Z_PK'].Value
            # chatroom.notice            = rec['Z_PK'].Value
            # chatroom.description       = rec['Z_PK'].Value
            chatroom.creator_id        = str(rec['ZCREATOR'].Value)
            # chatroom.owner_id          = rec['Z_PK'].Value
            # chatroom.member_count      = rec['Z_PK'].Value
            # chatroom.max_member_count  = rec['Z_PK'].Value
            chatroom.create_time       = rec['ZCREATEDTIME'].Value
                
            chatroom.deleted    = 1 if rec.IsDeleted else 0         
            chatroom.source     = self.cur_db_source            
            try:
                self.chatroom_map[chatroom.chatroom_id] = chatroom.name
            except:
                print 'self.chatroom_map', self.chatroom_map
                print 'chatroom.chatroom_id', chatroom.chatroom_id
                exc()
            try:
                self.im.db_insert_table_chatroom(chatroom)
            except:
                exc()
        self.im.db_commit()  

    def parse_Friend_ChatroomMember(self, db_root, db_path, table_name):
        ''' 'Line.sqlite', 'ZUSER'

                        RecNo	FieldName	SQLType	
            account_id  1	Z_PK	        INTEGER
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
                        27	ZMID	        VARCHAR
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
        if not self._read_db(db_root, db_path):
            return 
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'ZNAME', 'ZSORTABLENAME', ):
                continue
            friend = model_im.Friend()
            friend.account_id = rec['Z_PK'].Value
            friend.nickname   = rec['ZNAME'].Value
            friend.username   = rec['ZSORTABLENAME'].Value
            friend.remark     = rec['ZCUSTOMNAME'].Value    # 备注[TEXT]
            friend.signature  = rec['ZSTATUSMESSAGE'].Value

            pic_url = rec['ZPICTUREURL'].Value
            if pic_url and pic_url.startswith('/'):
                friend.photo = self._search_profile_img(pic_url)

            if rec['ZISINADDRESSBOOK'].Value and rec['ZMID'].Value :  # telephone 只取第一个
                friend.telephone = self.friend_tel_map.get(rec['ZMID'].Value, '')[0]

            if rec['ZISFRIEND'].Value: # 好友
                friend.type = model_im.FRIEND_TYPE_FRIEND
                if rec['ZFAVORITEORDER'].Value: # 特别关注
                    friend.type = model_im.FRIEND_TYPE_SPECAIL_FOLLOW
            else:
                friend.type = self._convert_friend_type(rec['ZCONTACTTYPE'].Value)
            friend.deleted = 1 if rec.IsDeleted or rec['ZISREMOVED'].Value else 0         
            friend.source  = self.cur_db_source

            for chatroom_id in self.member_chatroom_map.get(friend.account_id, []):
                self.parse_ChatroomMember(friend, chatroom_id)
            try:
                self.im.db_insert_table_friend(friend)
            except:
                exc()
        self.im.db_commit()  

    def parse_ChatroomMember(self, friend, chatroom_id):
        ''' account_id+'/Messages/Line.sqlite', 'ZGROUP'

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
        if  not self.member_chatroom_map.has_key(friend.account_id):
            return 
        cm = model_im.ChatroomMember()
        cm.account_id   = friend.account_id # 账户ID[TEXT]
        cm.chatroom_id  = chatroom_id  # 群ID[TEXT]
        cm.member_id    = friend.account_id # 成员ID[TEXT]
        cm.display_name = friend.remark if friend.remark else friend.nickname # 群内显示名称[TEXT]
        cm.photo        = friend.photo  # 头像[TEXT]
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

    @staticmethod
    def _read_plist(plist_node, *keys):
        ''' read .plist file 

        :type plist_node: node
        :type keys: tuple(*str)
        :rtype: tuple(*str)

        dir(bp) 
        ['ContainsKey', 'Equals', 'GetHashCode', 'GetKeySource', 'GetType', 'Item', 'Keys', 'MemberwiseClone', 
        'Populate', 'PrettyPrint', 'ReadMultiValue', 'ReadValue', 'ReferenceEquals', 'Source', 'ToString', 'Value', 
        '__class__', '__delattr__', '__doc__', '__format__', '__getattribute__', '__getitem__', '__hash__', '__init__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__str__', '__subclasshook__']
        '''
        res = {}
        bp = BPReader(plist_node.Data).top    
        # bp = BPReader.GetTree(plist_node.Data)
        # print(dir(BPReader))
        # print(type(bp))
        #print(bp)
        try:
            for k in keys:
                exc_debug(k)
                if bp[k]:
                    res[k] = bp[k].Value
                    exc_debug(res[k])
            return res
        except:
            exc()
            return res

    def _search_profile_img(self, file_name):
        # 用户, 好友, 群 头像 存储位置: \F8B82034-8474-4CB9-AD1E-3A703CB7C65B
        # \Library\Caches\PrivateStore\P_u423af962f1456db6cba8465cf82bb91b\Profile Images\ZMID
        if file_name[0] != '/':
            file_name = '/' + file_name
        pp = '/Library/Caches/PrivateStore/P_' \
             + self.cur_account_id + r'/Profile Images' \
             + file_name
        #print('friend pic url pattern ', pp)

        _node = self.group_root.GetByPath(pp)
        if _node:
            for file_node in _node.Children:
                file_path = file_node.AbsolutePath
                if file_path.split('/')[-1].endswith('.jpg'):
                    # print(file_path)
                    return file_path

        # exc_debug('profile_img:', file_name)
        # fs = self.root.FileSystem
        # node_list = list(fs.Search(file_name))
        # try:
        #     if len(node_list) > 1:
        #         for i in node_list:
        #             file_path = i.AbsolutePath
        #             if not file_path.endswith('.thumb'):
        #                 exc_debug('!!!!!!!!! find file_path: '+file_path)
        #                 return file_path
        #     elif len(node_list) < 1:
        #         return
        #     file_path = node_list[0].AbsolutePath
        #     exc_debug('!!!!!!!!! find file_path: '+file_path)
        #     return file_path
        # except:
        #     exc()
        #     return 

    def _get_msg_media_path(self, CHAT_TYPE_DICT, msg_ZCHAT, msg_ZID):
        ''' 获取聊天 media_path
        
            rec['ZCHAT'].Value  
            rec['ZID'].Value      
        '''
        # self.root + '/Library/Application Support/PrivateStore/P_u423af962f1456db6cba8465cf82bb91b/Message Attachments'
        # + /u1f49041c24a3b489dbf90163f8bcb293/8785959165675.mp4

        # file_pattern = self.root.AbsolutePath \
        #                + '/Library/Application Support/PrivateStore/P_' \
        #                + self.cur_account_id + r'/Message Attachments/' \
        #                + CHAT_TYPE_DICT.get(rec['ZCHAT'].Value, {}).get('ZMID', '') + r'/' \
        #                + rec['ZID'].Value + r'.*'
        # file_pattern1 = CHAT_TYPE_DICT.get(rec['ZCHAT'].Value, {}).get('ZMID', '') + r'/' + rec['ZID'].Value + '.*'

        media_path = '/Library/Application Support/PrivateStore/P_' \
                        + self.cur_account_id + r'/Message Attachments/' \
                        + CHAT_TYPE_DICT.get(msg_ZCHAT, {}).get('ZMID', '')
        _node = self.root.GetByPath(media_path)
        if _node:
            for file_node in _node.Children:
                file_path = file_node.AbsolutePath
                if file_path.split('/')[-1].startswith(msg_ZID):
                    # print(file_path)
                    return file_path
        return 

        # fs.Search 方法
        # try:
        #     if len(node_list) > 1:
        #         for i in node_list:
        #             file_path = i.AbsolutePath
        #             if not file_path.endswith('.thumb'):
        #                 exc_debug('!!!!!!!!! find file_path: '+file_path)
        #                 return file_path
        #     elif len(node_list) < 1:
        #         return
        #     file_path = node_list[0].AbsolutePath
        #     exc_debug('!!!!!!!!! find file_path: '+file_path)
        #     return file_path

    @staticmethod
    def _convert_ArrayByte_2_str(ab):
        # print(ab.ToString())
        # print(type(ab))
        # print(type(ab[0]))
        buf = ''.join([chr(x) for x in ab])
        buf = ''.join(ab).decode('utf-8')

        buf = str(buf, 'utf-8', errors='ignore')
        return buf



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
            print(dir(self.cur_db.ReadTableRecords(tb, self.extract_deleted, True)))
            return self.cur_db.ReadTableRecords(tb, self.extract_deleted, True)
        except:
            exc()         
            return []

    @staticmethod
    def _is_email_format(rec, key):
        """ 匹配邮箱地址

        :type rec: type: <rec>
        :type key: str
        :rtype: bool        
        """
        try:
            if IsDBNull(rec[key].Value) or len(rec[key].Value.strip()) < 5:
                return False
            reg_str = r'^\w+([-+.]\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$'
            match_obj = re.match(reg_str, rec[key].Value)
            if match_obj is None:
                return False      
            return True      
        except:
            print 'match email', rec[key].Value
            exc()
            return False

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
        if not ZTYPE:
            return
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
        if not ZCONTENTTYPE:
            return        
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
            # 18: model_im.MESSAGE_CONTENT_TYPE_,         # 删除图片
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

    # TODO
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
            exc_debug('new ZCONTENTTYPE {}!!!!!!!!!!!!!!!!!'.format(ZSENDSTATUS))

