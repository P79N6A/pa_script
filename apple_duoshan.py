# coding=utf-8

__author__ = 'YangLiyuan'

import json

import PA_runtime
from PA_runtime import *
import clr
try:
    clr.AddReference('model_im')
    clr.AddReference('bcp_im')
    clr.AddReference('ScriptUtils')
except:
    pass
del clr

import bcp_im
import model_im 
from ScriptUtils import CASE_NAME, exc, tp, DEBUG, base_analyze, parse_decorator, BaseParser, BaseAndroidParser


VERSION_APP_VALUE = 1

DUOSHAN_MSG_TYPE =  {
    7: model_im.MESSAGE_CONTENT_TYPE_TEXT,
    27: model_im.MESSAGE_CONTENT_TYPE_IMAGE,    # 
    30: model_im.MESSAGE_CONTENT_TYPE_VIDEO,    # 视频
    32: model_im.MESSAGE_CONTENT_TYPE_TEXT,     # 回复图片
    51: model_im.MESSAGE_CONTENT_TYPE_EMOJI,    # 内置表情包
    52: model_im.MESSAGE_CONTENT_TYPE_EMOJI,    # type 1 心, type 2 跳动心
    2000: model_im.MESSAGE_CONTENT_TYPE_SYSTEM,
    2001: model_im.MESSAGE_CONTENT_TYPE_SYSTEM,
}
DUOSHAN_LOCAL_IMG_PATH = '/Library/Caches/com.maya.imagecache/data/'


@parse_decorator
def analyze_duoshan(node, extract_deleted, extract_source):
    return base_analyze(AppleDuoShanParser, 
                        node, 
                        bcp_im.CONTACT_ACCOUNT_TYPE_IM_OTHER, 
                        VERSION_APP_VALUE,
                        bulid_name='多闪',
                        db_name='DuoShan_i')


class AppleDuoShanParser(BaseParser):
    ''' Library\Preferences\my.maya.iphone.plist '''

    def __init__(self, node, db_name):
        super(AppleDuoShanParser, self).__init__(node, db_name)
        self.VERSION_VALUE_DB = model_im.VERSION_VALUE_DB
        self.root = node.Parent.Parent.Parent
        self.Generate = model_im.GenerateModel
        self.csm = model_im.IM()

        self.cur_account = None
        self.cur_account_id = None
        self.cur_media_path = []

    def parse_main(self):
        '''
            Documents/DataContainer/account   

            TIMConversationCoreInfoORM      : chat name, icon, desc
            # TIMConversationLocalInfoORM
            TIMConversationORM              : type(is chatroom or chat), CHATROOM_MEMBER_COUNT, unreadCount
            # TIMConversationSettingsInfoORM: isFavorited, muted....
            TIMParticipantORM               : FRIEND_CHATROOMS
        '''
        LOCAL_IMG = self.preparse_media_image('Library/Caches/com.maya.imagecache/manifest.sqlite', 'manifest')
        account_folder_path = 'Documents/DataContainer/account'
        for account in self.parse_account(account_folder_path):
            self.cur_account = account
            self.cur_account_id = account.account_id
            self.cur_media_path = self.root.GetByPath('Documents/DataContainer/Data/'+account.m_id+'/Publish')

            if self._read_db('Library/Application Support/ChatFiles/{}/db.sqlite'.format(self.cur_account_id)):
                CHATROOM_MEMBER_COUNT = self._get_CHATROOM_MEMBER_COUNT('TIMConversationORM')
                FRIEND_CHATROOMS = self.preparse_chatroom_member('TIMParticipantORM')

                if self._read_db('Documents/my_im.db'):
                    FRIEND_ID_NAME = self.parse_Friend('my_im_user', FRIEND_CHATROOMS, CHATROOM_MEMBER_COUNT)

                    if self._read_db('Library/Application Support/ChatFiles/{}/db.sqlite'.format(self.cur_account_id)):
                        CHATROOM_ID_NAME = self.parse_Chatroom('TIMConversationCoreInfoORM', CHATROOM_MEMBER_COUNT)
                        self.parse_Message('TIMMessageORM', FRIEND_ID_NAME, CHATROOM_ID_NAME, CHATROOM_MEMBER_COUNT, LOCAL_IMG)

    def preparse_media_image(self, db_path, table_name):
        '''
            FieldName	        SQLType                 	
            key	                    text
            filename	            text
            size	                integer
            inline_data	            blob
            modification_time	    integer
            last_access_time	    integer
            extended_data	        blob
        '''
        # wxCrack = ServiceGetter.Get[IWechatCrackUin]()
        # uin = wxCrack.CrackUinFromMd5("52fd7a8329ed15872eb4ec82b5ebc5cf")
        LOCAL_IMG = {}
        if not self._read_db(db_path):
            return LOCAL_IMG
        for rec in self._read_table(table_name):
            if self._is_empty(rec, 'key', 'filename'):
                continue
            key      = rec['key'].Value
            filename = rec['filename'].Value
            size     = rec['size'].Value

            LOCAL_IMG[key] = {
                'filename': filename,
                'size': size,
            }
        return LOCAL_IMG

    def parse_account(self, folder_path):
        ''' # Media 文件夹id
                key: profileID
                val:110609878986
                key: userID
                val:110609878986
                key: avatar
                val:http://p0.pstatp.com/origin/3796/2975850990

            # 消息文件夹 id, 消息senderId
                key: profileIMID                                                                                                                               
                val:110630858997
                key: platformUID
                val:110630858997
            key: sessionID
            val:0efb025ca021b80b0e5edd74625e33d8
            key: phoneNumber
            val:181*****167
        '''
        accounts = []
        _folder = self.root.GetByPath(folder_path)
        for db_file in _folder.Children:
            try:
                bplist = BPReader.GetTree(db_file)
                if not bplist:
                    continue
                account = model_im.Account()
                account.account_id = bplist['profileIMID'].Value
                # account.nickname = None  # 昵称[TEXT]
                # account.username = None  # 用户名[TEXT]                
                account.photo      = bplist['avatar'].Value
                account.telephone  = bplist['phoneNumber'].Value
                account.m_id       = bplist['userID'].Value     
                accounts.append(account)
                self.csm.db_insert_table_account(account)
            except:
                exc()
        self.csm.db_commit()
        return accounts

    def _get_CHATROOM_MEMBER_COUNT(self, table_name):
        ''' TIMConversationORM

            FieldName	        SQLType      	
            identifier	            TEXT
            shortID	                INTEGER
            participantsCount	    INTEGER
            isParticipant	        INTEGER
            updatedAt	            INTEGER
            unreadCount	            INTEGER
            fakeUnreadCount	        INTEGER
            minIndex	            INTEGER
            type	                INTEGER
            draftAt	                INTEGER
            draftText	            TEXT
            hasUnreadMention	    INTEGER
            inbox	                INTEGER
            ticketUpdatedAt	        INTEGER
            ticket	                TEXT
            firstPageParticipants   BLOB
            localInfo	            TEXT
        '''
        CHATROOM_MEMBER_COUNT = {}
        for rec in self._read_table(table_name):
            # skip chat with one
            if rec['type'].Value == 1:
                continue
            if (self._is_empty(rec, 'identifier') or
                self._is_duplicate(rec, 'identifier')):
                continue               
            chatroom_pk  = rec['identifier'].Value
            member_count = rec['participantsCount'].Value
            CHATROOM_MEMBER_COUNT[chatroom_pk] = member_count
        return CHATROOM_MEMBER_COUNT

    def parse_Chatroom(self, table_name, CHATROOM_MEMBER_COUNT):
        ''' TIMConversationCoreInfoORM 

            FieldName	                    SQLType                   	
            belongingConversationIdentifier	    TEXT
            infoVersion	                        INTEGER
            name	                        TEXT
            desc	                        TEXT
            icon	                        TEXT
            notice	                        TEXT
            ext	                            TEXT
        '''
        CHATROOM_ID_NAME = {}
        for rec in self._read_table(table_name):
            try:
                if (rec['belongingConversationIdentifier'].Value not in CHATROOM_MEMBER_COUNT or
                    self._is_empty(rec, 'belongingConversationIdentifier', 'name') or
                    self._is_duplicate(rec, 'belongingConversationIdentifier')):
                    continue 
                chatroom = model_im.Chatroom()
                chatroom.account_id   = self.cur_account_id
                chatroom.chatroom_id  = rec['belongingConversationIdentifier'].Value
                chatroom.name         = rec['name'].Value
                chatroom.photo        = rec['icon'].Value
                try:
                    chatroom.creator_id = json.loads(rec['ext'].Value).get('a:s_name_operator')
                except:
                    pass
                chatroom.member_count = CHATROOM_MEMBER_COUNT.get(chatroom.chatroom_id, None)
                chatroom.create_time  = rec['infoVersion'].Value
                chatroom.deleted      = 1 if rec.IsDeleted else 0         
                chatroom.source       = self.cur_db_source            
                try:
                    CHATROOM_ID_NAME[chatroom.chatroom_id] = chatroom.name
                except:
                    exc()
                chatroom.insert_db(self.csm)
            except:
                exc()
        self.csm.db_commit()  
        return CHATROOM_ID_NAME

    def preparse_chatroom_member(self, table_name):
        ''' TIMParticipantORM
                        
            FieldName	                    SQLType                           	
            userID	                            INTEGER
            belongingConversationIdentifier	    TEXT
            sortOrder	                        INTEGER
            role	                            INTEGER
            alias	                            TEXT
        '''
        FRIEND_CHATROOMS = {}
        for rec in self._read_table(table_name, read_delete=False):
            if self._is_empty(rec, 'userID', 'belongingConversationIdentifier'):
                continue    
            member_id = str(rec['userID'].Value)
            group_id  = rec['belongingConversationIdentifier'].Value
            if FRIEND_CHATROOMS.has_key(member_id) and group_id not in FRIEND_CHATROOMS[member_id]:
                FRIEND_CHATROOMS[member_id].append(group_id)
            else:
                FRIEND_CHATROOMS[member_id] = [group_id]
        return FRIEND_CHATROOMS

    def parse_Friend(self, table_name, FRIEND_CHATROOMS, CHATROOM_MEMBER_COUNT):
        ''' my_im_user

            FieldName	        SQLType             	
            userID	                INTEGER
            account	                TEXT
            accountChange	        INTEGER
            age	                    INTEGER
            avatarURL	            TEXT
            gender	                INTEGER
            imUID	                INTEGER
            introVideoInfo	        BLOB
            isNewRecommendFriend	INTEGER
            logpb	                BLOB
            nickName	            TEXT
            originalNickName	    TEXT
            reason	                TEXT
            reasonType	            TEXT
            relationType	        INTEGER
            signiture	            TEXT
            storyBlockStatus	    BLOB
            updateTimeStamp	        INTEGER
            userType	            INTEGER
        '''
        FRIEND_ID_NAME = {}

        for rec in self._read_table(table_name):
            if (self._is_empty(rec, 'userID') or 
                self._is_duplicate(rec, 'userID')):
                continue    
            friend = model_im.Friend()
            # friend.signature
            # friend.remark
            friend.account_id = self.cur_account_id
            # friend.friend_id  = str(rec['userID'].Value)
            friend.friend_id = str(rec['imUID'].Value)           # 未注册的好像为 0 只有 userID
            friend.age       = rec['age'].Value
            friend.nickname  = rec['nickName'].Value
            friend.username  = rec['account'].Value if rec['account'].Value else rec['originalNickName'].Value
            friend.fullname  = rec['originalNickName'].Value
            # friend.photo = self._get_profile_img(friend.friend_id)
            friend.photo     = rec['avatarURL'].Value
            FRIEND_ID_NAME[friend.friend_id] = friend.nickname if friend.nickname else friend.username
            # relation
            #     1 好友
            #     2 陌生人
            # if rec['relation'].Value == 1: # 好友
            #     friend.type = model_im.FRIEND_TYPE_FRIEND
            # else:
            #     friend.type = self._convert_friend_type(rec['contact_type'].Value)
            friend.deleted = 1 if rec.IsDeleted else 0
            friend.source  = self.cur_db_source

            for chatroom_id in FRIEND_CHATROOMS.get(friend.friend_id, []):
                self.save_as_ChatroomMember(friend, chatroom_id)

            friend.insert_db(self.csm)
        self.csm.db_commit()
        return FRIEND_ID_NAME     

    def save_as_ChatroomMember(self, friend, chatroom_id):
        ''' 
            self.chatroom_id  = None  # 群ID[TEXT]
            self.member_id    = None  # 成员ID[TEXT]
            self.display_name = None  # 群内显示名称[TEX
            self.photo        = None  # 头像[TEXT]
            self.telephone    = None  # 电话[TEXT]
            self.email        = None  # 电子邮箱[TEXT]
            self.gender       = GENDER_NONE  # 性别[INT]
            self.age          = None  # 年龄[INT]
            self.address      = None  # 地址[TEXT]
            self.birthday     = None  # 生日[TEXT]
            self.signature    = None  # 签名[TEXT]
        '''
        try:
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
            cm.insert_db(self.csm)
        except:
            exc()

    def parse_Message(self, table_name, FRIEND_ID_NAME, 
                                        CHATROOM_ID_NAME, 
                                        CHATROOM_MEMBER_COUNT,
                                        LOCAL_IMG):
        '''TIMMessageORM

            FieldName	                    SQLType	                   	
            identifier	                        TEXT
            serverMessageID	                    INTEGER
            belongingConversationIdentifier	    TEXT
            sender	                            INTEGER
            serverCreatedAt	                    INTEGER
            localCreatedAt	                    INTEGER
            content	                            TEXT
            ext	                                TEXT
            localInfo	                        TEXT
            status	                            INTEGER
            type	                            INTEGER
            deleted	                            INTEGER
            hasRead	                            INTEGER
            messageVersion	                    INTEGER
            pullIndex	                        INTEGER
            orderIndex	                        INTEGER
            unreadMention	                    INTEGER
        '''
        for rec in self._read_table(table_name):
            try:
                if (self._is_empty(rec, 'identifier', 'content') or 
                    self._is_duplicate(rec, 'identifier')):
                    continue
                message = model_im.Message()
                message.account_id = self.cur_account_id
                message.talker_id  = rec['belongingConversationIdentifier'].Value
                message.sender_id  = str(rec['sender'].Value)
                message.is_sender  = 1 if message.sender_id == self.cur_account_id else 0
                message.send_time  = rec['serverCreatedAt'].Value
                message.source     = self.cur_db_source
                message.deleted    = 1 if rec.IsDeleted else 0
            # TEXT
                _data = json.loads(rec['content'].Value)
                if rec['type'].Value == 52:
                    if _data.get('type') == 1:
                        message.content = '[心]'
                    elif _data.get('type') == 2:
                        message.content = '[跳动的心]'
                else:
                    message.content = self._get_msg_text(_data)

            # media_path
                if not message.content:
                    message.media_path = self._get_msg_media_path(_data, LOCAL_IMG)
            # CHAT_TYPE
                # chatroom
                if message.talker_id in CHATROOM_MEMBER_COUNT:
                    message.talker_type = model_im.CHAT_TYPE_GROUP 
                    message.talker_name = CHATROOM_ID_NAME.get(message.talker_id, None)
                # solo
                else:
                    message.talker_type = model_im.CHAT_TYPE_FRIEND 
                    if message.is_sender:
                        receiver_id = self._get_receiver_id(rec['belongingConversationIdentifier'].Value)
                        message.talker_name = FRIEND_ID_NAME.get(receiver_id, None)
                    else:
                        message.talker_name = FRIEND_ID_NAME.get(message.sender_id, None)
            # status sender_name
                if message.is_sender:
                    message.status = model_im.MESSAGE_STATUS_SENT if rec['status'].Value==200 else model_im.MESSAGE_STATUS_UNSENT
                    message.sender_name = self.cur_account.nickname if self.cur_account else None
                else:
                    message.status = model_im.MESSAGE_STATUS_READ if rec['hasRead'].Value else model_im.MESSAGE_STATUS_UNREAD
                    message.sender_name = FRIEND_ID_NAME.get(message.sender_id, None)
            # MESSAGE_CONTENT_TYPE
                message.type = DUOSHAN_MSG_TYPE.get(rec['type'].Value, None)



                message.insert_db(self.csm)
            except:
                exc()
        self.csm.db_commit()

    def _get_receiver_id(self, muti_chat_id):
        try:
            _l = filter(lambda x: len(x) >= len(self.cur_account_id), muti_chat_id.split(':'))
            _l.remove(self.cur_account_id)
            if _l:
                return _l[0]
        except:
            exc()

    def _get_msg_text(self, json_data):
        '''
            {
                "video": {
                        "tkey": "tos-cn-o-0061/46fa79c6fa2a403eb3f705e81a62bb76",
                        "skey": "45ae43b1668387ec3ced6b0e58209cdd",
                        "md5":" "
                    },
                "poster": {
                        "oid":"tos-cn-o-0061/a03d614ffa1be06d027e84c1daa30262",
                        "skey":"a275426cd2f6f7652bbfb8bd0c821889ef97b4cbd375bfc6a9cb237d0172cbee",
                        "md5":"5aec1a17303c9fe6347e8e5ea215a37e"
                    },
                "height":960,
                "width":544
            }

            {
                "text":"Ssjksks",
                "post_type":0
            }

            {
                "check_pics":["tos-cn-o-0812/3fb47609d1f94d73b4f555f7aceb2a62"],
                "duration":11120,
                "from_gallery":1,
                "height":540,
                "mass_msg":0,
                "md5":"9d2de4e8b5529a712a65c03bae1f133b",
                "post_type":0,
                "poster":{
                    "md5":"e5e1095a64b84e8329ce6d39e7952c5a",
                    "oid":"tos-cn-o-0061/d37ddd9a5888467a8405baf3827b8fcc",
                    "skey":"3139e74b5e5d0099965841805873d23364902f3a9a16a66baacbebf599789d14"
                },
                "sub_message_type":0,
                "video": {
                    "md5":"92942606945d8d0f1377fcb356942a9b",
                    "skey":"7be5f675ca10b76ca1a3422209ab7d54",
                    "tkey":"tos-cn-o-0061/a5c685a44b434b8d8f79d78e4f59b4cc"
                },
                "width":960,
                "aweType":0,
                "classic_show_type":0,
                "msg_from":0,
                "msgHint":""
            }
        Args:
            msg_content ([type]): 
        '''
        try:
            if json_data.has_key('text'):
                return json_data.get('text')
        except:
            exc()

    def _get_msg_media_path(self, json_data, LOCAL_IMG):
        '''
        Media Path:
            Documents/DataContainer/Data/110609878986/Publish    # self.account.mid
                /Image/                 
                /OriginalVideo/           
                    /96C32D00-F5A7-45BF-A749-CDCD1FA2BDFB
                        96C32D00-F5A7-45BF-A749-CDCD1FA2BDFB
                        DraftVideo_1548212946.405598.mp4
                /Video/
                    9dd1328e2287a0e5274ebb586bf4a6b8.mp4
            {
                "height":183,
                "image_type":"webp",
                "url":{
                    "uri":"",
                    "url_list":["http://dl2.weshineapp.com/300/bb8801168996fbbc5b99c7f6ae26a537.webp?v\u003d5993cba02eaf3"]},
                    "width":300,
                    "aweType":0,
                    "classic_show_type":0,
                    "msg_from":0,
                    "msgHint":""
            }

            {
                "poster": {
                        "uri":"tos-cn-o-0061\/3510d97cd0a94922a27553e49ce33362"
                    },
                "post_type":1,
                "height":1024,
                "width":576,
                "check_pics":["tos-cn-o-0812\/53c127df0eb44908ac78bd138165b0b2"],
                "duration":5836,
                "from_gallery":0,
                "video":{"vid":"v0303db10000bh3tlvllt63vlha4sbfg"},
                "mass_msg":1,
                "check_texts":["my"]
            }
        '''
        try:    
            tp(json_data)
            img_uri = ''
            filename = ''
            # 视频
            if json_data.has_key('story'):
                img_uri = json_data.get('story', {}).get('cover', {}).get('uri', None)
            elif json_data.has_key('poster'):
                if json_data.has_key('video'):
                    img_uri = json_data.get('poster', {}).get('oid', None)
                else:
                    img_uri = json_data.get('poster', {}).get('uri', None)
            # 图片
            elif json_data.has_key('resource_url'):
                img_uri = json_data.get('resource_url', {}).get('oid', '')

            # 官方动图表情
            if 'webp' in [json_data.get('image', None), json_data.get('image_type', None)]:
                img_uri = json_data.get('url', {}).get('url_list', [''])[0]

            # filename
            if img_uri:
                if LOCAL_IMG.has_key(img_uri):
                    filename = LOCAL_IMG.get(img_uri, {}).get('filename')
                else:
                    for k in LOCAL_IMG:
                        if re.search(img_uri, k):
                            filename = LOCAL_IMG.get(k, {}).get('filename')
                if filename:
                    _path = DUOSHAN_LOCAL_IMG_PATH + filename
                    _node = self.root.GetByPath(_path)
                    if _node:
                        return _node.AbsolutePath

                return img_uri
        except:
            exc()

