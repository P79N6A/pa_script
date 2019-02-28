# coding=utf-8
__author__ = 'YangLiyuan'

import hashlib

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

CASE_NAME = ds.ProjectState.ProjectDir.Name

UNKNOWN_USER_ID       = -1
UNKNOWN_USER_NICKNAME = '未知用户'
UNKNOWN_USER_USERNAME = '未知用户'

VERSION_APP_VALUE = 2

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
    ''' Exception output '''
    try:
        if DEBUG:
            py_name = os.path.basename(__file__)
            msg = 'DEBUG {} case:<{}> :'.format(py_name, CASE_NAME)
            TraceService.Trace(TraceLevel.Warning, 
                            (msg+'{}{}').format(traceback.format_exc(), e))
    except:
        pass    

def tp(*e):
    ''' Highlight print in vs '''
    if DEBUG:
        TraceService.Trace(TraceLevel.Warning, "{}".format(e))
    else:
        pass     

def analyze_line(node, extract_deleted, extract_source):
    ''' android LINE 

        jp.naver.line.android
    '''
    tp('android_line.py is running ...')
    pr = ParserResults()
    res = []
    try:
        res = LineParser(node, extract_deleted, extract_source).parse()
    except:
        TraceService.Trace(TraceLevel.Debug, 
                           'android_line.py 解析新案例 "{}" 出错: {}'.format(CASE_NAME, traceback.format_exc()))
    if res:
        pr.Models.AddRange(res)
        pr.Build('LINE')
        tp('android_line.py is finished !')
    return pr

class LineParser(object):

    def __init__(self, node, extract_deleted, extract_source):
        ''' node: /data/data/jp.naver.line.android/databases/naver_line
        '''
        #tp(node.AbsolutePath)
        self.root = node.Parent
        self.extract_deleted = extract_deleted
        self.extract_source  = extract_source
        self.im = model_im.IM()        
        self.cachepath = ds.OpenCachePath("LINE")

        hash_str = hashlib.md5(node.AbsolutePath.encode('utf8')).hexdigest()[8:-8]
        self.cache_db = self.cachepath + '\\a_line_{}.db'.format(hash_str)
        # search profile, sticker
        self.media_node = None

        if self.root.FileSystem.Name.endswith('.tar'):
            self.rename_file_path = ['/storage/emulated', '/data/media'] 
        else:
            self.rename_file_path = None        

    def parse(self):
        if DEBUG or self.im.need_parse(self.cache_db, VERSION_APP_VALUE):
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
        ''' nvar_line '''
        if self._read_db('/naver_line'):
            self.cur_account    = self.parse_Account()
            self.cur_account_id = self.cur_account.account_id

            if self._read_db('/naver_line'):
                FRIEND_CHATROOMS, CHATROOM_MEMBER_COUNT = self.preparse_group_member('membership')
                CHAT_DICT        = self.parse_chat('chat')
                CHATROOM_ID_NAME = self.parse_Chatroom('groups', CHATROOM_MEMBER_COUNT)
                FRIEND_ID_NAME   = self.parse_Friend('contacts', FRIEND_CHATROOMS)
                self.parse_Message('chat_history', CHAT_DICT, CHATROOM_ID_NAME, FRIEND_ID_NAME)
            self.parse_Search('line_general_key_value', 'key_value_text')

    def parse_Search(self, db_name, table_name):
        ''' line_general_key_value', 'key_value_text
                FieldName	   SQLType
                key	           TEXT
                value	       TEXT
        '''
        if not self._read_db(db_name):
            return 
        for rec in self._read_table(table_name):
            if rec['key'].Value != 'SEARCH_RECENT_KEYWORDS':
                continue
            if canceller.IsCancellationRequested:
                return
            search_words = rec['value'].Value.split(u'\x1e')[:-1] if rec['value'].Value else None
            if search_words:
                for search_word in search_words:
                    search = model_im.Search()
                    search.account_id = self.cur_account_id
                    search.key        = search_word
                    search.source     = self.cur_db_source
                    search.deleted    = 1 if rec.IsDeleted else 0               
                    try:
                        search.insert_db(self.im)
                    except:
                        exc()
            self.im.db_commit()    

    def parse_Account(self):
        ''' LINE 通过以下来来确定本人 mid
                1 无法和自己聊天
                2 status=3 为已发送 (if server_id != null)
        '''
        friend_chat_ids = []
        account_id      = None
        cur_account     = None

        for rec in self._read_table('chat_history'):
            if self._is_empty(rec, 'chat_id', 'created_time'):
                continue
            if rec['status'].Value == 3 and IsDBNull(rec['from_mid'].Value) and not IsDBNull(rec['server_id'].Value):
                if rec['chat_id'].Value and rec['chat_id'].Value.startswith('u'):
                    friend_chat_ids.append(rec['chat_id'].Value)
            
        for rec in self._read_table('chat'):
            if rec['chat_id'].Value in friend_chat_ids:

                if rec['owner_mid'].Value and rec['owner_mid'].Value.startswith('u'):
                    if rec['chat_id'].Value != rec['owner_mid'].Value:
                        account_id = rec['owner_mid'].Value
                        break
                if rec['last_from_mid'].Value and rec['last_from_mid'].Value.startswith('u'):
                    if rec['chat_id'].Value != rec['last_from_mid'].Value:
                        account_id = rec['last_from_mid'].Value
                        break

        for rec in self._read_table('contacts'):
            if rec['m_id'].Value == account_id:
                account = model_im.Account()
                account.account_id = account_id
                account.nickname   = rec['server_name'].Value
                account.username   = rec['addressbook_name'].Value if rec['addressbook_name'].Value else rec['server_name'].Value
                account.signature  = rec['status_msg'].Value
                account.photo      = self._get_profile_img(rec['m_id'].Value)
                account.deleted    = 1 if rec.IsDeleted else 0
                account.source     = self.cur_db_source
                try:
                    account.insert_db(self.im)
                except:
                    exc()
                cur_account = account
                break
        # 没有用户数据, 使用默认用户 UNKNOWN_USER
        if cur_account is None:
            account = model_im.Account()
            account.account_id = account_id if account_id else UNKNOWN_USER_ID
            account.nickname   = UNKNOWN_USER_NICKNAME
            account.username   = UNKNOWN_USER_USERNAME
            account.source     = self.cur_db_source
            try:
                account.insert_db(self.im)
            except:
                exc()        
            cur_account = account
              
        self.im.db_commit()    
        return cur_account

    def preparse_group_member(self, table_name):
        ''' 'naver_line' membership 关联群与群成员
                        
            FieldName	SQLType	Size	
            id	            TEXT          # group mid
            m_id	            TEXT      # member mid
            is_accepted	        INTEGER
            updated_time	    INTEGER
            created_time	    INTEGER
        '''
        FRIEND_CHATROOMS      = {}
        CHATROOM_MEMBER_COUNT = {}
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'id', 'm_id'):
                continue    
            if len(rec['m_id'].Value) > 1 and rec['m_id'].Value[0] not in ('u', 'c', 'r'):
                continue
            if rec['is_accepted'].Value != 1:
                continue
            group_id = rec['id'].Value
            member_id = rec['m_id'].Value

            if FRIEND_CHATROOMS.has_key(member_id):
                FRIEND_CHATROOMS[member_id].append(group_id)
            else:
                FRIEND_CHATROOMS[member_id] = [group_id]

        for _, chatroom_pk_list in FRIEND_CHATROOMS.iteritems():
            for chatroom_pk in chatroom_pk_list:
                if CHATROOM_MEMBER_COUNT.has_key(chatroom_pk):
                    CHATROOM_MEMBER_COUNT[chatroom_pk] += 1    
                else:
                    CHATROOM_MEMBER_COUNT[chatroom_pk] = 1
        return FRIEND_CHATROOMS, CHATROOM_MEMBER_COUNT

    def parse_Chatroom(self, table_name, CHATROOM_MEMBER_COUNT):
        ''' naver_line group 
        '''
        '''     FieldName	    SQLType		             	
                id	                    TEXT           # group mid
                name	                TEXT
                picture_status	        TEXT
                creator	                TEXT
                status	                INTEGER
                is_first	              INTEGER
                display_type	          INTEGER
                accepted_invitation_time  INTEGER
                highlight_time	          INTEGER
                updated_time	          INTEGER
                created_time	          INTEGER
                prevented_joinby_ticket	  INTEGER
                invitation_ticket         TEXT
                favorite_timestamp        INTEGER

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
        CHATROOM_ID_NAME = {}
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'id', 'name') :
                continue 
            try:
                pk_value = rec['id'].Value
                if pk_value[0] not in  ('c', 'r') or self._is_duplicate(rec, 'id'):
                    continue
            except:
                continue
            chatroom = model_im.Chatroom()
            chatroom.account_id   = self.cur_account_id
            chatroom.chatroom_id  = rec['id'].Value
            chatroom.name         = rec['name'].Value
            chatroom.photo        = rec['picture_status'].Value
            chatroom.creator_id   = rec['creator'].Value
            chatroom.member_count = CHATROOM_MEMBER_COUNT.get(chatroom.chatroom_id, None)
            chatroom.create_time  = self._get_im_ts(rec['created_time'].Value)
            chatroom.deleted      = 1 if rec.IsDeleted else 0         
            chatroom.source       = self.cur_db_source            
            try:
                CHATROOM_ID_NAME[chatroom.chatroom_id] = chatroom.name
            except:
                exc()
            try:
                chatroom.insert_db(self.im)
            except:
                exc()
        self.im.db_commit()  
        return CHATROOM_ID_NAME

    def parse_chat(self, table_name):
        ''' naver_line chat 
        '''
        '''     FieldName	SQLType  	
                chat_id	                TEXT
                chat_name	                TEXT
                owner_mid	                TEXT
                last_from_mid	                TEXT
                last_message	                TEXT
                last_created_time	            TEXT
                message_count	                INTEGER
                read_message_count	         INTEGER
                latest_mentioned_position	 INTEGER
                type	                     INTEGER   1 是好友聊天, 3 是群聊
                is_notification	             INTEGER
                skin_key	                 TEXT
                input_text	                 TEXT
                input_text_metadata	         TEXT
                hide_member	                INTEGER
                p_timer	                INTEGER
                last_message_display_time	        TEXT
                mid_p	                TEXT
                is_archived	                INTEGER
                read_up	                TEXT
                is_groupcalling	            INTEGER
                latest_announcement_seq	    INTEGER
                announcement_view_status	INTEGER
                last_message_meta_data	    TEXT
        '''
        CHAT_DICT = {}
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'chat_id'):
                continue 
            chat_id = rec['chat_id'].Value  
            if chat_id.startswith('u') and rec['type'].Value == 1: #  好友聊天
                msg_type = model_im.CHAT_TYPE_FRIEND
            elif chat_id > 1 and chat_id[0] in ('c', 'r') and rec['type'].Value == 3: #  群聊天
                msg_type = model_im.CHAT_TYPE_GROUP
            else:
                continue
            owner_id = rec['owner_mid'].Value
            CHAT_DICT[chat_id] = {
                'msg_type': msg_type,
                'owner_id': owner_id,
            }
        return CHAT_DICT

    @print_run_time
    def parse_Message(self, table_name, CHAT_DICT, CHATROOM_ID_NAME, FRIEND_ID_NAME):
        ''' naver_line chat_history '''
        '''
            FieldName	        SQLType	          	
            id	                INTEGER
            server_id	                TEXT
            type	                INTEGER     # type       
            chat_id	                TEXT        # chat mid   c 开头是群, u 开头是好友聊天
            from_mid	                TEXT    # sender mid
            content	                TEXT         
            created_time	                TEXT
            delivered_time	                TEXT
            status	                INTEGER     # 1  3   7
            sent_count	                INTEGER
            read_count	                INTEGER
            location_name	            TEXT
            location_address	        TEXT
            location_phone	            TEXT
            location_latitude	        INTEGER
            location_longitude	        INTEGER
            attachement_image	        INTEGER
            attachement_image_height	INTEGER
            attachement_image_width	    INTEGER
            attachement_image_size	    INTEGER
            attachement_type	        INTEGER
            attachement_local_uri	    TEXT
            parameter	                TEXT
            chunks	                    BLOB
        '''
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'chat_id', 'created_time') or self._is_duplicate(rec, 'id'):
                continue
            message = model_im.Message()
            message.account_id  = self.cur_account_id
            message.talker_id   = rec['chat_id'].Value
            message.sender_id   = rec['from_mid'].Value
            if IsDBNull(rec['from_mid'].Value) and not IsDBNull(rec['server_id']):
                message.sender_id  = self.cur_account_id
            message.sender_name = FRIEND_ID_NAME.get(message.sender_id, None)
            message.is_sender   = 1 if message.sender_id == message.account_id else None
            message.content     = rec['content'].Value
            message.send_time   = self._get_im_ts(rec['created_time'].Value)
            message.status      = self._convert_send_status(rec['status'].Value)

            # CHAT_TYPE
            msg_chat = CHAT_DICT.get(rec['chat_id'].Value, {})    
            message.talker_type = msg_chat.get('msg_type', None)
            # talker_name
            if message.talker_type == model_im.CHAT_TYPE_GROUP:
                message.talker_name = CHATROOM_ID_NAME.get(rec['chat_id'].Value, None)
            else:
                message.talker_name = FRIEND_ID_NAME.get(rec['chat_id'].Value, None)
            # MESSAGE_CONTENT_TYPE
            message.type = self._convert_msg_type(rec['type'].Value, rec['attachement_type'].Value)
            # media_path
            message.media_path = self._get_msg_media_path(str(rec['id'].Value), 
                                                          rec['chat_id'].Value,
                                                          rec['parameter'].Value,
                                                          rec['attachement_local_uri'].Value,
                                                          message.type)
            # location
            if message.type == model_im.MESSAGE_CONTENT_TYPE_LOCATION:
                location = message.create_location()
                location.latitude  = rec['location_latitude'].Value * (10 ** -6)
                location.longitude = rec['location_longitude'].Value * (10 ** -6)
                location.address   = rec['location_address'].Value
                location.type      = model_im.LOCATION_TYPE_GOOGLE
                location.timestamp = self._get_im_ts(rec['created_time'].Value)
                location.source    = self.cur_db_source
            message.source  = self.cur_db_source
            message.deleted = 1 if rec.IsDeleted else 0         
            try:
                message.insert_db(self.im)
            except:
                exc()
        self.im.db_commit()

    def parse_Friend(self, table_name, FRIEND_CHATROOMS):
        ''' naver_line contacts 
        '''
        ''' FieldName	    SQLType	
            m_id	            TEXT
            contact_id	            TEXT
            contact_key	            TEXT
            name	            TEXT
            phonetic_name	            TEXT
            server_name	            TEXT
            addressbook_name	    EXT
            custom_name	            TEXT
            status_msg	            TEXT
            is_unread_status_msg	    INTEGER
            picture_status	            TEXT
            picture_path	            TEXT
            relation	            INTEGER
            status	                INTEGER
            is_first	            INTEGER
            display_type            INTEGER
            capable_flags           INTEGER
            contact_kind            INTEGER
            contact_type            INTEGER
            buddy_category          INTEGER
            buddy_icon_type         INTEGER
            is_on_air	            INTEGER
            hidden	                INTEGER
            favorite	            INTEGER
            added_time_to_friend	    INTEGER
            updated_time	            INTEGER
            created_time	            INTEGER
            recommend_params	            TEXT
            profile_music	            TEXT
            profile_update_highlight_time	INTEGER
            contact_sync_request_time	    INTEGER
            on_air_label	            INTEGER
            video_profile	            TEXT
            schema_ver	                INTEGER
            status_msg_meta_data	    TEXT
        '''
        FRIEND_ID_NAME = {}
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'm_id', 'name'):
                continue
            try:
                pk_value = rec['m_id'].Value
                if len(pk_value) != 33 or pk_value[0] != 'u' or self._is_duplicate(rec, 'm_id'):
                    continue
            except:
                continue
            friend = model_im.Friend()
            friend.account_id = self.cur_account_id
            friend.friend_id  = rec['m_id'].Value
            friend.remark     = rec['custom_name'].Value
            friend.nickname   = rec['server_name'].Value
            friend.username   = rec['addressbook_name'].Value if rec['addressbook_name'].Value else rec['server_name'].Value 
            friend.signature  = rec['status_msg'].Value
            FRIEND_ID_NAME[rec['m_id'].Value] = friend.nickname if friend.nickname else friend.username
            friend.photo = self._get_profile_img(friend.friend_id)
            ''' relation
                1 好友
                2 陌生人
            '''
            if rec['relation'].Value == 1: # 好友
                friend.type = model_im.FRIEND_TYPE_FRIEND
            else:
                friend.type = self._convert_friend_type(rec['contact_type'].Value)
            
            friend.deleted = 1 if rec.IsDeleted else 0
            friend.source  = self.cur_db_source

            for chatroom_id in FRIEND_CHATROOMS.get(friend.friend_id, []):
                self.parse_ChatroomMember(friend, chatroom_id)
            try:
                friend.insert_db(self.im)
            except:
                exc()            
        self.im.db_commit()
        return FRIEND_ID_NAME        

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
        cm = model_im.ChatroomMember()
        cm.account_id   = friend.account_id # 账户ID[TEXT]
        cm.chatroom_id  = chatroom_id       # 群ID[TEXT]
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
            cm.insert_db(self.im)
        except:
            exc()

    def _get_sticker_path(self, parameter):
        ''' LINE 表情
        
            STKPKGID	11537	
            STKVER	1	
            STKID	52002741	
            STKOPT	A	
            STKHASH		
            STK_IMG_TXT		
            message_relation_server_message_id		
            message_relation_service_code		
            message_relation_type_code

            STKTXT	[Sticker]	
            STKID	187	
            PUBLIC	true	
            STKPKGID	3	
            STKVER	100	
            message_relation_server_message_id		
            message_relation_service_code		
            message_relation_type_code	
        '''
        sticker_dict = {}
        try:
            par = parameter.split('\t')
            for i in range(0, len(par), 2):
                sticker_dict[par[i]] = par[i+1]
            img_pattern = ('/jp.naver.line.android/stickers/' 
                           + sticker_dict['STKPKGID'] + '/'
                           + sticker_dict['STKID'])
            res = self._search_media_file(img_pattern)   
            #tp('stickers >>>>>>>>> ', res)  
            return res       
        except:
            exc()
            return None

    def _get_msg_media_path(self, msg_PK, 
                                  msg_CHAT_ID, 
                                  rec_parameter, 
                                  attachement_local_uri, 
                                  message_type):
        ''' 获取聊天 media_path

        Return:
            media_path(str)

        storage/emulated/0/Android/data
                /jp.naver.line.android/storage/mo
        '''
        patterns = {
            'voice': ('jp.naver.line.android/storage/mo/' 
                            + msg_CHAT_ID + '/' 
                            + 'voice_' + msg_PK + '.aac$'),
            'file': ('jp.naver.line.android/storage/mo/' 
                            + msg_CHAT_ID + '/f/' 
                            + msg_PK),
            'image_vodeo1': ('jp.naver.line.android/storage/mo/' 
                            + msg_CHAT_ID + '/' 
                            + msg_PK + '$'),
            'image_vodeo2': ('jp.naver.line.android/storage/mo/' 
                            + msg_CHAT_ID + '/' 
                            + msg_PK + '.thumb$'),
        }
        msg_pattern = None
        if not message_type:
            return
        elif message_type == model_im.MESSAGE_CONTENT_TYPE_EMOJI:
            media_path = self._get_sticker_path(rec_parameter)
            if media_path:
                return media_path
        elif message_type == model_im.MESSAGE_CONTENT_TYPE_VOICE:
            msg_pattern = [patterns['voice']]
        elif message_type == model_im.MESSAGE_CONTENT_TYPE_ATTACHMENT:
            msg_pattern = [patterns['file']]
        elif message_type in [
                model_im.MESSAGE_CONTENT_TYPE_IMAGE, 
                model_im.MESSAGE_CONTENT_TYPE_VIDEO,
            ]:  
            msg_pattern = [patterns['image_vodeo1'], patterns['image_vodeo2']]
        else:
            return
        if msg_pattern:
            for _pattern in msg_pattern:
                file_path = self._search_media_file(_pattern)
                return file_path     
        # finally by uri   replace '/storage/emulated', '/data/media'
        if not IsDBNull(attachement_local_uri) and not attachement_local_uri:  
            attact_path = attachement_local_uri.lstrip('content://file://')
            attact_path = attact_path.replace(self.rename_file_path[0], 
                                              self.rename_file_path[1])
            return self._fs_search(attact_path)

    def _get_profile_img(self, file_name):
        ''' 附件路径 db:
            com_linecorp_linebox_android

            storage/emulated/0/Android/data
                /jp.naver.line.android
                    /storage
                        /ad
                        /g             群头像
                        /mmicon        图标
                        /mo            聊天附件, 文件名 msg_pk
                            /CHAT_MID 
                                /f     存放聊天附件
                                    MSGPK_原文件名及后缀
                                MSG_PK.thumb 
                                
                        /obse          乱码 怀疑是动态      
                            /draft_post
                            /group_cover
                            /home
                            /home_media
                            /home_profile
                            /post
                            /timeline
                        /p            profile 用户头像, 文件名称是 mid
                        /temp
                        /toyboxing     
                            /line     什么缓存都有
                        /write
                            /本人动态原图
                    /stickers
                        /11537   # STKPKGID
                            /STKID
        '''
        img_pattern =  '/jp.naver.line.android/storage/p/' + file_name
        #tp('friend pic url pattern ', pattern)
        res = self._search_media_file(img_pattern +'$')
        if not res:
            res = self._search_media_file(img_pattern + '.thumb')
        return res

    def _search_media_file(self, raw_file_path):
        ''' search file
        
        Args:
            raw_file_path (str): 
        Returns:
            str:        /storage/emulated/0/DCIM/LINE/line_ + id(15....)
            附件路径 db: com_linecorp_linebox_android
            群图片:      jp.naver.line.android/cache/image_manager_disk_cache
            视频:       /jp.naver.line.android/cache/ad2
            实际图片:    /storage/emulated/0/DCIM/LINE/line_ + id(15....)       # 不需要
        '''
        try:
            if not raw_file_path:
                return 
            if self.media_node:
                file_path = raw_file_path.split('jp.naver.line.android')[1]
                res_file_path = self.media_node.GetByPath(file_path).AbsolutePath
                if res_file_path:
                    tp('!!!!!!!!! find file_path:', res_file_path) 
                    return res_file_path
            else: # save media file node
                res_file_path = self._fs_search(raw_file_path)
                if res_file_path and 'jp.naver.line.android' in res_file_path:
                    _path = res_file_path.split('jp.naver.line.android')[0]
                    self.media_node = self.root.FileSystem.GetByPath(_path+'jp.naver.line.android')
                if res_file_path:
                    tp('!!!!!!!!! find file_path:', res_file_path) 
                return res_file_path
        except:
            return 
        
    def _fs_search(self, raw_file_path):
        ''' fs search
        
        Args:
            raw_file_path ([type]): [description]
        
        Returns:
            node.AbsolutePath: [description]
        '''
        fs = self.root.FileSystem
        try:
            return list(fs.Search(raw_file_path))[0].AbsolutePath
        except:
            return None

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
            tp('new CHAT_TYPE {}!!!!!!!!!!!!!!!!!'.format(ZTYPE))

    @staticmethod
    def _convert_msg_type(msg_type, msg_attachement_type):
        ''' naver_line  type attachement_type   field to model_im CHAT_TYPE
            
            :type status: int
            :rtype: int
        '''
        '''
        type: 
            1: text, 语音, 图片, 视频
            2: 系统消息-加入群
            4: 网络电话  
            5: 表情
            8: 上传群相册
            9: 系统消息-邀请xxx
            11: 系统消息-踢出群xxx
            13: 系统消息-删除群照片
            
        attachement_type 
            0: text
            1: img
            2: video
            3: voice
            6: 网络电话
            7: 表情
            14:file
            15:location
            16: 上传群相册
            18: 删除群照片        
        '''
        ATTACHEMENT_TYPE_MAP = {
            0: model_im.MESSAGE_CONTENT_TYPE_TEXT,        # 1 TEXT
            1: model_im.MESSAGE_CONTENT_TYPE_IMAGE,       # 2 图片
            2: model_im.MESSAGE_CONTENT_TYPE_VIDEO,       # 4 视频
            3: model_im.MESSAGE_CONTENT_TYPE_VOICE,       # 3 语音
            6: model_im.MESSAGE_CONTENT_TYPE_VOIP,        # 9 网络电话
            # 5: model_im.MESSAGE_CONTENT_TYPE_,            
            7: model_im.MESSAGE_CONTENT_TYPE_EMOJI,       # 5 表情
            # 13: model_im.MESSAGE_CONTENT_TYPE_,
            14: model_im.MESSAGE_CONTENT_TYPE_ATTACHMENT, # 10 附件
            15: model_im.MESSAGE_CONTENT_TYPE_LOCATION,   # 7 位置
            16: model_im.MESSAGE_CONTENT_TYPE_SYSTEM,     # 99 上传群相册
            18: model_im.MESSAGE_CONTENT_TYPE_SYSTEM,     # 99 删除图片
        }
        TYPE_MAP = {
            # 1: text, 语音, 图片, 视频
            2: model_im.MESSAGE_CONTENT_TYPE_SYSTEM,   # 系统消息-加入群
            # 4: 网络电话  
            5: model_im.MESSAGE_CONTENT_TYPE_EMOJI,       #表情
            8: model_im.MESSAGE_CONTENT_TYPE_SYSTEM,   # 上传群相册
            9: model_im.MESSAGE_CONTENT_TYPE_SYSTEM,   # 系统消息-邀请xxx    server_id 为空
            11: model_im.MESSAGE_CONTENT_TYPE_SYSTEM,  # 系统消息-踢出群xxx  server_id 为空
            13: model_im.MESSAGE_CONTENT_TYPE_SYSTEM,  # 系统消息-删除群照片  server_id 为空           
        }
        if msg_attachement_type in ATTACHEMENT_TYPE_MAP:
            return ATTACHEMENT_TYPE_MAP[msg_attachement_type]
        elif msg_type in TYPE_MAP:
            return TYPE_MAP[msg_type]
        else:
            return None

    @staticmethod
    def _convert_friend_type(contact_type):
        '''ZUSER 'contact_type' field to model_im FRIEND_TYPE_
        
        :type ZTYPE: int
        :rtype: int

        status: 2 屏蔽
        '''
        type_map = {
            # 1: model_im.FRIEND_TYPE_ ,              # = 0   通讯录好友
            # 0: model_im.FRIEND_TYPE_NONE,           # = 0   未知
            7: model_im.FRIEND_TYPE_FRIEND,         # = 1   好友
            # 3: model_im.FRIEND_TYPE_FANS,           # = 3   粉丝
            # 4: model_im.FRIEND_TYPE_FOLLOW,         # = 4   关注
            5: model_im.FRIEND_TYPE_GROUP_FRIEND,    # = 2   群好友
            # 6: model_im.FRIEND_TYPE_SPECAIL_FOLLOW, # = 5   特别关注
            # 7: model_im.FRIEND_TYPE_MUTUAL_FOLLOW,  # = 6   互相关注
            8: model_im.FRIEND_TYPE_SUBSCRIBE,       # = 8   官方账号 - 公众号
            # 9: model_im.FRIEND_TYPE_RECENT,        # = 7   最近
            # 0: model_im.FRIEND_TYPE_STRANGER,       # = 9   陌生人       
            2: model_im.FRIEND_TYPE_STRANGER,        # = 9   陌生人       
            -1: model_im.FRIEND_TYPE_STRANGER,       # = 9   陌生人       
        }
        try:
            return type_map[contact_type]
        except:
            tp('new contact_type {}!!!!!!!!!!!!!!!!!'.format(contact_type))

    @staticmethod
    def _convert_send_status(status):
        '''chat_history  'status' field to model_im MESSAGE_STATUS_
        
        :type ZTYPE: int
        :rtype: int

        status 没有区分是否已读
        
        MESSAGE_STATUS_DEFAULT = 0
        MESSAGE_STATUS_UNSENT  = 1
        MESSAGE_STATUS_SENT    = 2
        MESSAGE_STATUS_UNREAD  = 3
        MESSAGE_STATUS_READ    = 4
        '''
        if not status:
            return           
        type_map = {
            # 0: model_im.MESSAGE_STATUS_DEFAULT,
            1: model_im.MESSAGE_STATUS_READ,      
            # 1: model_im.MESSAGE_STATUS_UNREAD,      
            3: model_im.MESSAGE_STATUS_SENT,  
            2: model_im.MESSAGE_STATUS_UNSENT,  
            # 7: 
        }
        try:
            return type_map[status]
        except:
            tp('new status {}!!!!!!!!!!!!!!!!!'.format(status))


    def _read_db(self, db_path):
        ''' 读取手机数据库

        :type db_path: str
        :rtype: bool                              
        '''
        db_node = self.root.GetByPath(db_path)
        self.cur_db = SQLiteParser.Database.FromNode(db_node, canceller)
        if self.cur_db is None:
            return False
        self.cur_db_source = db_node.AbsolutePath
        return True

    def _read_table(self, table_name):
        ''' 读取手机数据库 - 表

        :type table_name: str
        :rtype: db.ReadTableRecords()                                       
        '''
        self._PK_LIST = []
        try:
            tb = SQLiteParser.TableSignature(table_name)  
            return self.cur_db.ReadTableRecords(tb, self.extract_deleted, True)
        except:
            exc()
            return []

    def _is_duplicate(self, rec, pk_name):
        ''' filter duplicate record
        
        Args:
            rec (record): 
            pk_name (str): 
        Returns:
            bool: rec[pk_name].Value in self._PK_LIST
        '''
        try:
            pk_value = rec[pk_name].Value
            if IsDBNull(pk_value) or pk_value in self._PK_LIST:
                return True
            self._PK_LIST.append(pk_value)
            return False
        except:
            exc()
            return True

    @staticmethod
    def _is_empty(rec, *args):
        ''' 过滤 DBNull 空数据, 有一空值就跳过
        
        :type rec:   rec
        :type *args: str
        :rtype: bool
        '''
        try:
            for i in args:

                
                if IsDBNull(rec[i].Value) or rec[i].Value in ('', ' ', None, [], {}):
                    return True
            return False
        except:
            exc()
            return True     

    @staticmethod
    def _get_im_ts(timestamp):
        ''' convert_ts 13=>10
        '''
        try:
            if isinstance(timestamp, (str, int, long, float, Int64)) and len(str(timestamp))==13:
                return int(str(timestamp)[:10])
        except:
            exc()
            return 
        
