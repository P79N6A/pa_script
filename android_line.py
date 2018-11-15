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
        TraceService.Trace(TraceLevel.Warning, "解析出错: LINE {}".format(traceback.format_exc()))
    else:
        pass 

def exc_debug(*e):
    TraceService.Trace(TraceLevel.Warning, "{}".format(e))


def analyze_line(node, extract_deleted, extract_source):
    """ android LINE 

        jp.naver.line.android   
    """
    exc_debug('android_line.py is running ...')
    pr = ParserResults()
    res = []
    try:
        res = LineParser(node, extract_deleted, extract_source).parse()
    except:
        exc()
    if res:
        pr.Models.AddRange(res)
        pr.Build('LINE')
        exc_debug('android_line.py completed!')
    return pr

class LineParser(object):

    def __init__(self, node, extract_deleted, extract_source):
        ''' node: /data/data/jp.naver.line.android/databases/naver_line

            Library\Preferences\jp.naver.line.plist

        '''
        self.root = node.Parent
        self.extract_deleted = extract_deleted
        self.extract_source  = extract_source
        self.im = model_im.IM()        
        self.cachepath = ds.OpenCachePath("LINE")

        hash_str = hashlib.md5(node.AbsolutePath).hexdigest()[8:-8]
        self.cache_db = self.cachepath + '\\a_line_{}.db'.format(hash_str)

    def parse(self):
        ''' account
            contact
            mail
            attachment
            search
            vsersion
        ''' 
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
        ''' nvar_line
        '''
        # if self._read_db('/things_user_device'):

        if self._read_db('/naver_line'):
            self.account_list = self.parse_Account()

            for account_id in self.account_list:
                # self.cur_account_id = account.account_id
                self.cur_account_id = account_id

                CHAT_DICT   = self.parse_chat('chat')
                FRIEND_CHATROOMS = self.preparse_group_member('membership')

                FRIEND_ID_NAME   = self.parse_Friend('contacts', FRIEND_CHATROOMS)
                CHATROOM_ID_NAME = self.parse_Chatroom('groups')
                self.parse_Message('chat_history', CHAT_DICT, CHATROOM_ID_NAME, FRIEND_ID_NAME)
                #self.parse_Feed('', '')
                #self.parse_FeedLike('', '')
                #self.parse_FeedComment('', '')
                #self.parse_Location('', '')
                #self.parse_Deal('', '')

    def parse_Account(self):
        ''' LINE 通过以下来来确定本人 mid
                1 无法和自己聊天
                2 status=3 为已发送 (if server_id != null)
        '''
        account_list = []
        friend_chat_ids = []
        for rec in self._read_table('chat_history'):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'chat_id', 'created_time'):
                continue
            if rec['status'].Value == 3 and IsDBNull(rec['from_mid'].Value) and not IsDBNull(rec['server_id'].Value):
                if rec['chat_id'].Value and rec['chat_id'].Value.startswith('u'):
                    friend_chat_ids.append(rec['chat_id'].Value)
            
        for rec in self._read_table('chat'):
            if canceller.IsCancellationRequested:
                return            
            if rec['chat_id'].Value in friend_chat_ids:
                if rec['owner_mid'].Value and rec['owner_mid'].Value.startswith('u'):
                    if rec['chat_id'].Value != rec['owner_mid'].Value:
                        account_id = rec['owner_mid'].Value
                        break

        for rec in self._read_table('contacts'):
            if canceller.IsCancellationRequested:
                return                  
            if rec['m_id'].Value == account_id:
                account = model_im.Account()
                account.account_id = account_id
                account.nickname   = rec['server_name'].Value
                account.username   = rec['addressbook_name'].Value if rec['addressbook_name'].Value else rec['server_name'].Value 
                account.signature  = rec['status_msg'].Value         

                # pic_url = rec['picture_path'].Value
                # if pic_url and pic_url.startswith('/'):
                # picture_path  useless, profile_img file name is m_id
                account.photo = self._get_profile_img(rec['m_id'].Value)                
                account.deleted = 1 if rec.IsDeleted else 0
                account.source  = self.cur_db_source

                account_list.append(account_id)
                try:
                    self.im.db_insert_table_account(account)
                except:
                    exc()
        self.im.db_commit()    
        return account_list

    def preparse_group_member(self, table_name):
        ''' 'naver_line' membership 关联群与群成员
                        
            FieldName	SQLType	Size	
            id	            TEXT          # group mid
            m_id	            TEXT      # member mid
            is_accepted	        INTEGER
            updated_time	    INTEGER
            created_time	    INTEGER
        '''
        FRIEND_CHATROOMS = {}
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'id', 'm_id') or not rec['id'].Value.startswith('c') or not rec['m_id'].Value.startswith('u'):
                continue    
            group_id = rec['id'].Value
            member_id = rec['m_id'].Value

            if FRIEND_CHATROOMS.has_key(member_id):
                FRIEND_CHATROOMS[member_id].append(group_id)
            else:
                FRIEND_CHATROOMS[member_id] = [group_id]
        print(FRIEND_CHATROOMS)
        return FRIEND_CHATROOMS

    def parse_Chatroom(self, table_name):
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
            if self._is_empty(rec, 'id', 'name'):
                continue        
            chatroom = model_im.Chatroom()
            chatroom.account_id        = self.cur_account_id
            chatroom.chatroom_id       = rec['id'].Value
            chatroom.name              = rec['name'].Value
            chatroom.photo             = rec['picture_status'].Value
            # chatroom.type              = rec['Z_PK'].Value
            # chatroom.notice            = rec['Z_PK'].Value
            # chatroom.description       = rec['Z_PK'].Value
            chatroom.creator_id        = rec['creator'].Value
            # chatroom.owner_id          = rec['Z_PK'].Value
            # chatroom.member_count      = rec['Z_PK'].Value
            # chatroom.max_member_count  = rec['Z_PK'].Value
            chatroom.create_time       = self._get_im_ts(rec['created_time'].Value)
                
            chatroom.deleted    = 1 if rec.IsDeleted else 0         
            chatroom.source     = self.cur_db_source            
            try:
                CHATROOM_ID_NAME[chatroom.chatroom_id] = chatroom.name
            except:
                print 'CHATROOM_ID_NAME', CHATROOM_ID_NAME
                print 'chatroom.chatroom_id', chatroom.chatroom_id
                exc()
            try:
                self.im.db_insert_table_chatroom(chatroom)
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
                last_created_time	                TEXT
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
                is_groupcalling	                INTEGER
                latest_announcement_seq	        INTEGER
                announcement_view_status	    INTEGER
                last_message_meta_data	        TEXT
        '''
        CHAT_DICT = {}
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'chat_id', ):
                continue      
            chat_id = rec['chat_id'].Value  
            if chat_id.startswith('u') and rec['type'].Value==1: #  好友聊天
                msg_type = model_im.CHAT_TYPE_FRIEND
            elif chat_id.startswith('c') and rec['type'].Value==3: #  群聊天
                msg_type = model_im.CHAT_TYPE_GROUP
            owner_id = rec['owner_mid'].Value

            CHAT_DICT[chat_id] = {
                'msg_type': msg_type,
                'owner_id': owner_id,
            }
        return CHAT_DICT

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
            if self._is_empty(rec, 'chat_id', 'created_time'):
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

            # MESSAGE_CONTENT_TYPE
            message.type = self._convert_msg_type(rec['type'].Value, rec['attachement_type'].Value)

            if message.type in [
                                model_im.MESSAGE_CONTENT_TYPE_IMAGE, 
                                model_im.MESSAGE_CONTENT_TYPE_VOICE,
                                model_im.MESSAGE_CONTENT_TYPE_VIDEO,
                                model_im.MESSAGE_CONTENT_TYPE_ATTACHMENT,
                            ]:
                message.media_path = self._get_msg_media_path(str(rec['id'].Value), rec['chat_id'].Value)
                if not message.media_path:
                    raw_file_path = rec['attachement_local_uri'].Value
                    message.media_path = self._search_file(raw_file_path)

            
            if message.talker_type == model_im.CHAT_TYPE_GROUP:
                message.talker_name = CHATROOM_ID_NAME.get(rec['chat_id'].Value, None)
            else:
                message.talker_name = FRIEND_ID_NAME.get(rec['chat_id'].Value, None)

            # location
            if message.type == model_im.MESSAGE_CONTENT_TYPE_LOCATION:
                location = model_im.Location()
                message.extra_id   = location.location_id
                location.latitude  = rec['location_latitude'].Value * (10 ** -6)
                location.longitude = rec['location_longitude'].Value * (10 ** -6)
                location.address   = rec['location_address'].Value
                location.timestamp = self._get_im_ts(rec['created_time'].Value)
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
            friend = model_im.Friend()
            friend.account_id = self.cur_account_id
            friend.friend_id  = rec['m_id'].Value
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
                # if rec['ZFAVORITEORDER'].Value: # 特别关注
                #     friend.type = model_im.FRIEND_TYPE_SPECAIL_FOLLOW
            else:
                friend.type = self._convert_friend_type(rec['contact_type'].Value)
            
            friend.deleted = 1 if rec.IsDeleted else 0
            friend.source  = self.cur_db_source

            for chatroom_id in FRIEND_CHATROOMS.get(friend.friend_id, []):
                self.parse_ChatroomMember(friend, chatroom_id)
            try:
                self.im.db_insert_table_friend(friend)
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
            self.im.db_insert_table_chatroom_member(cm)
        except:
            exc()

    def _read_db(self, db_path):
        """ 读取手机数据库

        :type db_path: str
        :rtype: bool                              
        """
        db_node = self.root.GetByPath(db_path)
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

    def _get_msg_media_path(self, msg_PK, msg_CHAT_ID):
        ''' 获取聊天 media_path

        storage/emulated/0/Android/data
                /jp.naver.line.android/storage/mo
        '''
        # msg_media_pattern_1 = 'jp.naver.line.android/storage/mo/' + msg_CHAT_ID + '/' + msg_PK + '$'
        # msg_media_pattern_2 = msg_media_pattern_1 + '.thumb' + '$'
        msg_media_pattern_3 = 'jp.naver.line.android/storage/mo/' + msg_CHAT_ID + '/f/' + msg_PK

        file_path = self._search_file(msg_media_pattern_3)
        return file_path        

    def _get_profile_img(self, file_name):
        ''' 附件路径 db:
            com_linecorp_linebox_android

            storage/emulated/0/Android/data
                /jp.naver.line.android/storage
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
        '''
        img_pattern =  '/jp.naver.line.android/storage/p/' + file_name
        #exc_debug('friend pic url pattern ', pattern)
        res = self._search_file(img_pattern +'$')
        exc_debug('res', res)
        return res if res else self._search_file(img_pattern + '.thumb')

    def _search_file(self, raw_file_path):
        ''' search file
        
        Args:
            raw_file_path (str): 
        Returns:
            str:        /storage/emulated/0/DCIM/LINE/line_ + id(15....)
            附件路径 db: com_linecorp_linebox_android
            群图片:      jp.naver.line.android\cache\image_manager_disk_cache
            视频:       \jp.naver.line.android\cache\ad2
            实际图片:    /storage/emulated/0/DCIM/LINE/line_ + id(15....)
        '''
        # if raw_file_path.startswith('file://'):
            # /storage/emulated/0/DCIM/LINE/line_1541057697077.jpg
        if IsDBNull(raw_file_path):
            return 
        file_path = raw_file_path.replace('file://', '').replace('content://', '')
        # elif raw_file_path.startswith('content://'):

        # exc_debug('file_path:', file_path)

        fs = self.root.FileSystem
        node_list = list(fs.Search(file_path))
        try:
            res_file_path = node_list[0].AbsolutePath
            exc_debug('!!!!!!!!! find file_path:', res_file_path)
            return res_file_path
        except:
            # exc_debug('not found')
            return 


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
            0: model_im.MESSAGE_CONTENT_TYPE_TEXT,        # TEXT
            1: model_im.MESSAGE_CONTENT_TYPE_IMAGE,       # 图片
            2: model_im.MESSAGE_CONTENT_TYPE_VIDEO,       # 视频
            3: model_im.MESSAGE_CONTENT_TYPE_VOICE,       # 语音
            6: model_im.MESSAGE_CONTENT_TYPE_VOIP,        # 网络电话
            # 5: model_im.MESSAGE_CONTENT_TYPE_,            
            7: model_im.MESSAGE_CONTENT_TYPE_EMOJI,       # 表情
            # 13: model_im.MESSAGE_CONTENT_TYPE_,
            14: model_im.MESSAGE_CONTENT_TYPE_ATTACHMENT, # 附件
            15: model_im.MESSAGE_CONTENT_TYPE_LOCATION,   # 位置
            16: model_im.MESSAGE_CONTENT_TYPE_SYSTEM,     # 上传群相册
            18: model_im.MESSAGE_CONTENT_TYPE_SYSTEM,     # 删除图片
        }
        TYPE_MAP = {
            # 1: text, 语音, 图片, 视频
            2: model_im.MESSAGE_CONTENT_TYPE_SYSTEM,   # 系统消息-加入群
            # 4: 网络电话  
            # 5: 表情
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
            return

    @staticmethod
    def _convert_friend_type(contact_type):
        '''ZUSER 'contact_type' field to model_im FRIEND_TYPE_
        
        :type ZTYPE: int
        :rtype: int

        status: 2 屏蔽
        '''
        type_map = {
            # 1: model_im.FRIEND_TYPE_ ,             # = 0   通讯录好友
            # 0: model_im.FRIEND_TYPE_NONE,           # = 0   未知
            # 1: model_im.FRIEND_TYPE_FRIEND,         # = 1   好友
            # 3: model_im.FRIEND_TYPE_FANS,           # = 3   粉丝
            # 4: model_im.FRIEND_TYPE_FOLLOW,         # = 4   关注
            5: model_im.FRIEND_TYPE_GROUP_FRIEND,   # = 2   群好友
            # 6: model_im.FRIEND_TYPE_SPECAIL_FOLLOW, # = 5   特别关注
            # 7: model_im.FRIEND_TYPE_MUTUAL_FOLLOW,  # = 6   互相关注
            8: model_im.FRIEND_TYPE_SUBSCRIBE,      # = 8   官方账号 - 公众号
            # 9: model_im.FRIEND_TYPE_RECENT,         # = 7   最近
            2: model_im.FRIEND_TYPE_STRANGER,       # = 9   陌生人       
        }
        try:
            return type_map[contact_type]
        except:
            exc_debug('new contact_type {}!!!!!!!!!!!!!!!!!'.format(contact_type))

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
            exc_debug('new ZCONTENTTYPE {}!!!!!!!!!!!!!!!!!'.format(status))

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
        