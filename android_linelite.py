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

VERSION_APP_VALUE = 1

UNKNOWN_USER_ID       = -1
UNKNOWN_USER_NICKNAME = '未知用户'
UNKNOWN_USER_USERNAME = '未知用户'


def exc(e=''):
    ''' Exception output '''
    try:
        if DEBUG:
            py_name = os.path.basename(__file__)
            msg = 'DEBUG {} case:<{}> :'.format(py_name, CASE_NAME)
            TraceService.Trace(TraceLevel.Warning, (msg+'{}{}').format(traceback.format_exc(), e))
    except:
        pass   

def tp(*e):
    ''' Highlight print in vs '''
    if DEBUG:
        TraceService.Trace(TraceLevel.Warning, '{}'.format(e))
    else:
        pass  

def analyze_linelite(node, extract_deleted, extract_source):
    ''' android LINE LITE com.linecorp.linelite '''
    tp('>>> android_linelite.py is running ...')
    pr = ParserResults()
    res = []
    try:
        res = LineParser(node, extract_deleted, extract_source).parse()
    except:
        TraceService.Trace(TraceLevel.Debug, 
                           'android_line.py 解析新案例 "{}" 出错: {}'.format(CASE_NAME, traceback.format_exc()))
    if res:
        pr.Models.AddRange(res)
        pr.Build('LINE Lite')
        tp('>>> android_linelite.py is finished !')
    return pr


class LineParser(object):

    def __init__(self, node, extract_deleted, extract_source):
        ''' node: /data/data/com.linecorp.linelite/databases/LINE_LITE '''
        self.root = node.Parent
        self.extract_deleted = extract_deleted
        self.extract_source  = extract_source
        self.im = model_im.IM()        
        self.cachepath = ds.OpenCachePath('LINE_LITE')

        hash_str = hashlib.md5(node.AbsolutePath.encode('utf8')).hexdigest()[8:-8]
        self.cache_db = self.cachepath + '\\a_linelite_{}.db'.format(hash_str)

        self.profile_node_4_search = self.root.FileSystem

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
        ''' LINE_LITE

        REAL_CONTACT
        REAL_OFFICIAL_ACCOUNT_DETAIL  # 存放官方用户
        REAL_LOCAL_ID_TO_CONTACT_ID   # 与本地 contact 表关联的用户
        REAL_JOINED_GROUP             # 加入的群
        REAL_INVITED_GROUP            # 邀请的群
        REAL_GROUP                    # 群数据
        REAL_CHAT_DTO_STORE           # 存放 chat 
        '''
        if self._read_db('/LINE_LITE'):
            account = self.parse_Account()
            self.cur_account_id = account.account_id

            CHATROOM_ID_NAME, FRIEND_CHATROOMS = self.parse_Chatroom('REAL_GROUP')
            FRIEND_ID_NAME = self.parse_Friend('REAL_CONTACT', FRIEND_CHATROOMS)
            CHATS = self.parse_Chats('REAL_CHAT_DTO_STORE')
            # tp(CHATS)
            for chat_id in CHATS:
                chat_table_name = 'REAL_' + chat_id
                self.parse_Message(chat_table_name, FRIEND_ID_NAME, CHATROOM_ID_NAME)

    def parse_Account(self):
        ''' LINE 通过以下来来确定本人 mid
                1 无法和自己聊天
                2 status=3 为已发送 (if server_id != null)
        '''
        # friend_chat_ids = []
        account_id  = None
        cur_account = None

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

    def parse_Friend(self, table_name, FRIEND_CHATROOMS):
        ''' LINE_LITE contacts 

        Args:
            blob_data (bytearray): [description]
                0B 00 01 00 00 00 21            
                frined_id
                0A 00 02 00 00 
                timestamp 
                00 0A 00 00 00 05 08 00 0B 00 00 00 03 08 00 15 00 00 00 02 F0 B0 01 60 00 00
                09
                00 0A 00 00 00 05 08 00 0B 00 00 00 03 08 00 15 00 00 00 02 0B 00 16 00 00 00
                07           
                E38286E38186E3818D0B0018000000                            
        Return:
            chatroom_res (dict): 
        ''' 
        '''  
            FieldName	SQLType	
            _key	    STRING
            _val	    BLOB          0B 分隔符
        '''              
        FRIEND_ID_NAME = {}
        LEN_FRIEND_ID_POS = int('0x06', 16)
        LEN_USERNAME_POS  = [int('0x47', 16), int('0x4E', 16)]

        for rec in self._read_table(table_name, deleted=False):
            if self._is_empty(rec, '_key', '_val'):
                continue
            try:
                pk_value = rec['_key'].Value
                if len(pk_value) != 33 or pk_value[0] != 'u' or self._is_duplicate(rec, '_key'):
                    continue
            except:
                continue
            friend = model_im.Friend()
            blob_data = self._2_list(rec['_val'].Value)
            friend.account_id = self.cur_account_id
            friend.friend_id  = self._get_hexdata_by_lenpos(blob_data, LEN_FRIEND_ID_POS)

            if len(str(self._hex_2_time(blob_data, int('0x2D', 16)))) == 10:
                friend.nickname = self._get_hexdata_by_lenpos(blob_data, LEN_USERNAME_POS[1])
            else:
                friend.nickname = self._get_hexdata_by_lenpos(blob_data, LEN_USERNAME_POS[0])
            # friend.username = friend_res.get('friend_id', None)
            # friend.signature = friend_res.get('friend_id', None)
            friend.photo = self._get_profile_img(friend.friend_id)
            FRIEND_ID_NAME[friend.friend_id] = friend.nickname if friend.nickname else None
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

    def parse_Chatroom(self, table_name):
        ''' REAL_GROUP 包含群成员

            FieldName	SQLType	   	
            _key	        STRING     # chat_id
            _val	        BLOB       # 
        '''
        CHATROOM_ID_NAME = {}
        FRIEND_CHATROOMS = {}
        for rec in self._read_table(table_name, deleted=False):
            if self._is_empty(rec, '_key', '_val') :
                continue 
            try:
                pk_value = rec['_key'].Value
                if pk_value[0] not in  ('c', 'r') or self._is_duplicate(rec, '_key'):
                    continue
            except:
                continue
            CHATROOM_NAME, CHATROOM_MEMBERS = self._read_chatroom_hex(rec['_val'].Value)

            chatroom = model_im.Chatroom()
            chatroom.account_id   = self.cur_account_id
            chatroom.chatroom_id  = rec['_key'].Value
            chatroom.name         = CHATROOM_NAME if CHATROOM_NAME else None
            chatroom.member_count = len(CHATROOM_MEMBERS)
            chatroom.creator_id   = CHATROOM_MEMBERS[0] if CHATROOM_MEMBERS else None
            # chatroom.create_time  = 
            chatroom.deleted      = 1 if rec.IsDeleted else 0         
            chatroom.source       = self.cur_db_source   

            for friend_id in CHATROOM_MEMBERS:
                if FRIEND_CHATROOMS.has_key(friend_id):
                    FRIEND_CHATROOMS[friend_id].add(chatroom.chatroom_id)
                else:
                    chatrooms = set()
                    chatrooms.add(chatroom.chatroom_id)
                    FRIEND_CHATROOMS[friend_id] = chatrooms  
            try:
                CHATROOM_ID_NAME[chatroom.chatroom_id] = chatroom.name
            except:
                exc()
            try:
                chatroom.insert_db(self.im)
            except:
                exc()
        self.im.db_commit()  
        return CHATROOM_ID_NAME, FRIEND_CHATROOMS
        
    def _read_chatroom_hex(self, blob_data):
        ''' lalala
        
        Args:
            blob_data (bytearray): [description]
        Return:
            chatroom_res (dict): 
        '''
        list_data = self._2_list(blob_data)
        chatroom_str = self._asciis_2_str(list_data)
        CHATROOM_NAME_POS = int('0x39', 16)
        CHATROOM_NAME     = self._get_hexdata_by_lenpos(list_data, CHATROOM_NAME_POS)
        CHATROOM_MEMBERS  = re.findall(r'u[a-z0-9]{32}', chatroom_str)
        return CHATROOM_NAME, CHATROOM_MEMBERS

    def parse_Chats(self, table_name):
        ''' REAL_CHAT_DTO_STORE

            FieldName	SQLType	   	
            _key	        STRING     # chat_id
            _val	        BLOB       # 
                lastCreatedTimeZ..
                notificationI..
                readMessageCountZ..
                showMemberI..
                totalMessageCountL..
                chatIdt..
                Ljava/lang/String;L..
                inputTextq.~..L..
                lastFromMidq.~..L..
                lastMessageq.~..L..
                memberMid
        '''
        chats = []
        for rec in self._read_table(table_name):
            if self._is_empty(rec, '_key', '_val') or self._incorrect_chat_id(rec, '_key'):
                continue   
            chat_name = rec['_key'].Value
            if chat_name and chat_name[0] not in ('c', 'r', 'u'):
                continue
            chat_id = rec['_key'].Value
            # _val    = rec['_val'].Value            
            chats.append(chat_id)
        return set(chats)
            
    def parse_Message(self, table_name, FRIEND_ID_NAME, CHATROOM_ID_NAME):
        ''' REAL_ + chat_id

            FieldName	SQLType
            _key	    INTEGER
            _val	    BLOB
        '''
        if table_name  not in self.cur_db.Tables:
            return 
        for rec in self._read_table(table_name):
            if self._is_empty(rec, '_key', '_val'):
                continue         
            _val = rec['_val'].Value
            # tp(table_name + '_' + str(int(rec['_key'].Value)))
            msg_res = self._read_msg(_val, table_name)
            if not msg_res:
                return 
            message = model_im.Message()
            message.account_id  = self.cur_account_id
            message.talker_id   = msg_res.get('talker_id', '')
            message.content     = msg_res.get('content', None)
            message.sender_id   = msg_res.get('sender_id', None)
            message.send_time   = msg_res.get('send_time', None)
            message.sender_name = FRIEND_ID_NAME.get(message.sender_id, None)
            if msg_res.get('is_sender', None):
                message.is_sender = 1 
                message.status    = model_im.MESSAGE_STATUS_SENT
            else:
                message.status = model_im.MESSAGE_STATUS_READ
            # talk_name, talk_type 
            if message.talker_id.startswith('u'):
                message.talker_type = model_im.CHAT_TYPE_FRIEND
                message.talker_name = FRIEND_ID_NAME.get(message.talker_id, None)
            elif message.talker_id and message.talker_id[0] in ('c', 'r'):
                message.talker_type = model_im.CHAT_TYPE_GROUP
                message.talker_name = CHATROOM_ID_NAME.get(message.talker_id, None)
            else:
                continue
            # media
            if msg_res and 'media' in msg_res:
                message.media_path = msg_res.get('media', '')
                if 'image' in message.media_path:
                    message.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                elif 'audio' in message.media_path:
                    message.type = model_im.MESSAGE_CONTENT_TYPE_VOICE
                elif 'video' in message.media_path:
                    message.type = model_im.MESSAGE_CONTENT_TYPE_VIDEO
            elif msg_res.get('is_call', None):
                message.type = model_im.MESSAGE_CONTENT_TYPE_VOIP
            # attachment
            elif msg_res.get('attach_file_name', None):
                message.type = model_im.MESSAGE_CONTENT_TYPE_ATTACHMENT
                message.media_path = msg_res.get('attach_file_path', None)
            else:
                message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
            try:
                message.insert_db(self.im)
            except:
                exc()
        self.im.db_commit()

    def _read_msg(self, data, table_name):
        ''' read blob <Array Bytes> -> {}

        Args:
            data (<Array Bytes>): rec['xx'].Value 
            START_FLAG (`list` of `int`): [int('0x74', 16)] for linelite
            START_FLAG_POS (int): 
        Returns:
            `dict` of 4` str` [msg_time, chat_id, msg_content, sender_id]

        从 515 开始 
            1 会话对象id  
            2 msg_content
        '''
        # tp('table_name',  table_name)
        res = {}
        START_FLAG = int('0x74', 16)
        START_FLAG_POS = int('0x203', 16)

        pattern_res = self._get_msg_pattern(data)
        if pattern_res != [
                'createdTime', 
                'deliveredTime', 
                'downloadComplete', 
                'latitude', 
                'longitude', 
                'chatId', 
                'content', 
                'fromMid', 
                'id', 
                'Ljava/lang/Integer;', 
                'locationAddress', 
                'locationPhone', 
                'locationTitle', 
                'parameters', 
                'readCount', 
                'sentCount', 
                'serverId', 
                'status', 
                'Lcom/linecorp/linelite/app/main/chat/StatusType;', 
                'type', 
                'Lcom/linecorp/linelite/app/main/chat/HistoryType;']:
            return {}
        data = self._2_list(data)
        hexstr_data = self._hexint_2_hexstr(data)

        if not data or not hexstr_data:
            return {}
        res['send_time'] = int(self._hex_2_time(data, int('0x1E4', 16)))
        for k in ('talker_id', 'content', 'sender_id'):
            # no content
            if self._hexint_2_hexstr(data[START_FLAG_POS: START_FLAG_POS+3]) == '707400':
                START_FLAG_POS += 1
                continue
            try:
                if data[START_FLAG_POS] != START_FLAG:
                    break
            except:
                tp(data)
                tp(res)
                tp(self._asciis_2_str(data))
            # 表示数据长度的值的位置
            len_data_start = START_FLAG_POS + 1
            len_data_end   = START_FLAG_POS + 2               
            data_length = data[len_data_start] * 16 + data[len_data_end]
            chat_id_start = len_data_end + 1                           
            chat_id_ends  = chat_id_start + data_length       
            str_var = self._asciis_2_str(data[chat_id_start: chat_id_ends])
            res[k] = str_var

            START_FLAG_POS = chat_id_ends
            # media
                # MEDIA_DIV chat_id 之后  
                #    s r 0x00 0x11 java.lang.Integer
                # 70 71 00 7E 00 06 73 72 00
                # 70 71007E0006737200
                #    71007E0006737200   voice audio
            for _div in ('7071007e0006737200', '7070737200', '71007e0006737200'):
                if hexstr_data[START_FLAG_POS*2:].startswith(_div):
                    MEDIA_DIV = '116a6176612e6c616e672e496e746567657212e2a0a4f781873802000149000576616c75'\
                              + '65787200106a6176612e6c616e672e4e756d62657286ac951d0b94e08b02000078700000'
                    len_media_data = len(_div)/2+72
                    if self._hexint_2_hexstr(data[START_FLAG_POS: START_FLAG_POS+len_media_data]) == _div+MEDIA_DIV:
                        media_path = self._get_msg_media_path(data, START_FLAG_POS+len_media_data, table_name)
                        if media_path:
                            res['media'] = media_path
        # is_sender
        try:
            # status: 01 接受 电话 坐标 文字 图片   02 发送 文字  # 06 发送 .txt 图片
            # status 之前 'sr./com.linecorp.linelite.app.main.chat.HistoryType'
            AFTER_STATUS_PATTERN = '7372002f636f6d2e6c696e65636f72702e6c696e656c6974652e6170702e6d61696e2e636861742e486973746f727954797065'
            status_pos = hexstr_data.find(AFTER_STATUS_PATTERN)
            if status_pos != -1:
                is_sender_data = data[status_pos/2 - 1]
                res['is_sender'] = False if is_sender_data == 1 else True
        except:
            exc()
            pass
        if not res.get('sender_id', None):
            if res.get('is_sender', None):
                res['sender_id'] = self.cur_account_id
            elif table_name.split('_')[1].startswith('u'):
                res['sender_id'] = table_name.split('_')[1]
                
        if not res.get('media', None):
            # call  
            if 'Call History :' in res.get('content', '') and 'millisecs, Result' in res.get('content', ''):
                res['is_call'] = True
            # attachement FILE_EXPIRE_TIMESTAMP
            elif not res.get('content', None) and '46494c455f4558504952455f54494d455354414d50' in hexstr_data:
                str_data = self._asciis_2_str(data)
                attach_pattern = r'FILE_EXPIRE_TIMESTAMP\t(\d{13})\tFILE_NAME\t(\S+)\tFILE_SIZE\t(\d+)\tLOCAL_FILE_PATH\t(.*?)sq\x00~'
                match_res = re.search(attach_pattern, str_data)
                try:
                    res['attach_file_name'] = match_res.group(2)
                    res['attach_file_path'] = match_res.group(4)
                except:
                    pass
        return res

    def _get_msg_pattern(self, blob_data):
        ''' get blob pattern '''
        '''
            div = ('q\x00~\x00\x01', 'q\x00~\x00\x02')
                    71 00 7E 01       71 00 7E 00 02
                    content 之后 图片之前:    
                    s r 0x000x11 java.lang.Integer
                   73 72 00 11   6A ... 

            media div: 
                    p  q 00  ~ 00 06  s  r 00   
                   70 71 00 7E 00 06 73 72 00

                    p  p  s  r 00
                   70 70 73 72 00
        '''      
        res = []
        data = self._2_list(blob_data)
        START_FLAG_POS = int('0x45', 16)

        p0 = self._asciis_2_str(data[int('0x08', 16): int('0x3A', 16)])
        BLOB_PATTERN = 'com.linecorp.linelite.app.main.chat.ChatHistoryDto' 
                        
        if p0 != BLOB_PATTERN or data[START_FLAG_POS] != int('0x4A', 16):
            return 'p0: {}'.format(p0)
        START_FLAG_1 = (int('0x4A', 16), int('0x44', 16), int('0x4C', 16), int('0x5A', 16)) # J Z D L
        while True:
            # createdTimeJ  deliveredTimeZ downloadCompleteD latitudeD longitudeL chatIdt
            if data[START_FLAG_POS] not in START_FLAG_1:
                break
            len_data_start = START_FLAG_POS + 1
            len_data_end   = START_FLAG_POS + 2                       
            data_length   = data[len_data_start] * 16 + data[len_data_end]
            chat_id_start = len_data_end + 1                            
            chat_id_ends  = chat_id_start + data_length
            str_var = self._asciis_2_str(data[chat_id_start: chat_id_ends])
            res.append(str_var)

            START_FLAG_POS = chat_id_ends

        if data[START_FLAG_POS] != int('0x74', 16): # t
            return res
        data_length = data[START_FLAG_POS + 1] * 16 + data[START_FLAG_POS + 2]
        str_var = self._asciis_2_str(data[START_FLAG_POS + 3: START_FLAG_POS + 3 + data_length])
        if str_var != 'Ljava/lang/String;':
            return res
        START_FLAG_POS = START_FLAG_POS + 3 + data_length
                        #   L               q               D               Z           t
        START_FLAG_2 = (int('0x4C', 16), int('0x71', 16), int('0x44', 16), int('0x5A', 16), int('0x74', 16))
        while True:
            # 数据在前, 分隔在后   content 
            # 71 00 7E 00 01 : q\x00~\x00\x01
            len_data_start = START_FLAG_POS + 1
            len_data_end   = START_FLAG_POS + 2                       
            data_length   = data[len_data_start] * 16 + data[len_data_end]
            chat_id_start = len_data_end + 1                            
            chat_id_ends  = chat_id_start + data_length

            if data[START_FLAG_POS] not in START_FLAG_2:
                START_FLAG_POS = chat_id_ends
                break
            str_var = self._asciis_2_str(data[chat_id_start: chat_id_ends])
            res.append(str_var)

            div = ('q\x00~\x00\x01', 'q\x00~\x00\x02')
            if self._asciis_2_str(data[chat_id_ends: chat_id_ends+5]) in div:
                START_FLAG_POS = chat_id_ends + 5     
            else:
                START_FLAG_POS = chat_id_ends
        return res

    def _convert_send_status(self, is_sender):
        if not is_sender:
            return           
        type_map = {
            1: model_im.MESSAGE_STATUS_READ,      
            3: model_im.MESSAGE_STATUS_SENT,  
        }
        try:
            return type_map[is_sender]
        except:
            pass

    def _get_msg_media_path(self, blob_data, media_pos, table_name):
        ''' parse media 
            com.linecorp.linelite\cache\REAL_u423af962f1456db6cba8465cf82bb91b
        
        Args:
            blob_data (<Array Bytes>):
            media_pos (int): media_id_hex == data[media_pos, media_pos+2]
            table_name (str)
        Returns:
            media_path (str)
        '''
        try:
            media_id = str(self._sum_of_hex(blob_data[media_pos: media_pos + 2]))
            if not media_id:
                return ''
            media_path = 'com.linecorp.linelite/cache/' 

            # images
            for postfix in ('O', 'P'):
                image_media_id = table_name.split('_')[1] + media_id
                full_file_path = media_path + table_name + '/images' + '/' + image_media_id
                res = self._fs_search(full_file_path + postfix)  # oringin
                if res: 
                    return res
            for file_type in ('audio', 'video'):
                file_path = media_path + table_name + '/' + file_type + '/' + media_id
                res = self._fs_search(file_path)
                if res:
                    return res
        except:
            exc()
            return ''

    @staticmethod
    def _sum_of_hex(list_of_hex):
        ''' [16, 15] -> ['0x10', '0x0F'] -> int(0x100F, 16)
        
        Args:
            list_of_hex (list): 
        Returns:
            sum_int (int): int 
        '''
        try:
            sum_hex_str = ''.join(map(lambda x: hex(int(x)).replace('0x', '') if x>15 else hex(int(x)).replace('0x', '0'), list_of_hex))
            sum_int = int(sum_hex_str, 16)
            return sum_int
        except:
            tp()
            return 0

    def _hex_2_time(self, data_blob, time_pos):
        ''' map(lambda x: int(x, 16), ['01', '67', '4C', '80', '72', 'DF', '08']) -> 1543481317
        
        Args:
            data_blob (list of int): []
        Returns:
            ts (int): 10 digit
        '''
        try:
            time_data = data_blob[time_pos: time_pos+6]
            _ts = int(self._sum_of_hex(time_data)/1000)
            return _ts
        except:
            exc()
            return 0

    @staticmethod
    def _hexint_2_hexstr(list_of_hex):
        ''' [16, 15] -> ['0x10', '0x0F'] -> '100F'
        
        Args:
            list_of_hex (list): 
        Returns:
            hexstr: (str)       
        '''
        try:
            sum_hex_str = ''.join(map(lambda x: hex(x).replace('0x', '') if x>15 else hex(x).replace('0x', '0'), list_of_hex))
            return sum_hex_str
        except:
            tp()
            return ''

    def _get_hexdata_by_lenpos(self, list_data, len_pos):
        ''' return hexdata by position of data length
        
        Args:
            list_data (list of hexint): 
            len_pos (int): 
        Returns:
            hexdata (str): 正常字符串
        '''
        try:
            end_pos = len_pos + 1 + list_data[len_pos]
            raw_hexdata = list_data[len_pos+1: end_pos]
            res = self._asciis_2_str(raw_hexdata)
            return res 
        except:
            exc()
            return ''

    @staticmethod
    def _asciis_2_str(asciis):
        ''' convert hex list to string

        Args:
            list(ArrayByte) (list of hexint): 
        Returns:
            string (str):
        '''
        try:
            s = ''.join([chr(x) for x in asciis]).decode('utf8', 'ignore')
            return s
        except:
            exc()
            return ''

    def _get_profile_img(self, file_name):
        ''' 附件路径 华为 meta8:  
            com.linecorp.linelite/cache/
                REAL_profileThumbnailImage             # 用户头像 thumb
                REAL_profileOriginalImage              # 用户头像 原图
                REAL_stickers                          # 表情
                REAL_c03826bc5691f830ef2a7838989c60a88 # 即 聊天表 表名
        '''
        img_pattern_origin =  'cache/REAL_profileOriginalImage/' + file_name
        img_pattern_thumb  =  'cache/REAL_profileThumbnailImage/' + file_name
        
        try:
            for pattern in (img_pattern_origin, img_pattern_thumb):
                res_file_path = list(self.profile_node_4_search.Search(pattern))
                if res_file_path:
                    if self.profile_node_4_search == self.root.FileSystem:  
                        # com.linecorp.linelite
                        self.profile_node_4_search = res_file_path[0].Parent.Parent.Parent 
                    return res_file_path[0].AbsolutePath   
        except:
            exc()
            return ''

    def _fs_search(self, raw_file_path):
        ''' search file
        
        Args:
            raw_file_path (str): 
        Returns:
        '''
        try:
            fs = self.root.FileSystem
            res_file_path = list(fs.Search(raw_file_path))[0].AbsolutePath
            tp('!!!!!!!!! find file_path:', res_file_path)
            return res_file_path
        except:
            # tp('not found')
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
    def _get_im_ts(timestamp):
        ''' convert_ts 13=>10
        '''
        try:
            if isinstance(timestamp, (str, int, long, float, Int64)) and len(str(timestamp))==13:
                return int(str(timestamp)[:10])
        except:
            exc()
            return 

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

    def _read_table(self, table_name, deleted=None):
        ''' 读取手机数据库 - 表

        :type table_name: str
        :rtype: db.ReadTableRecords()                                       
        '''
        self._PK_LIST = []
        if deleted is None:
            deleted = self.extract_deleted
        try:
            tb = SQLiteParser.TableSignature(table_name)  
            return self.cur_db.ReadTableRecords(tb, deleted, True)
        except:
            # exc()
            tp('error table name', table_name)
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
    def _incorrect_chat_id(rec, *args):
        ''' filter control character '''
        NON_PATTERN = r'[\x00-\x08\x0b-\x0c\x0e-\x1f]'
        _PATTERN = r'[a-zA-Z0-9_]+$'
        try:
            for i in args:
                raw_str = rec[i].Value
                if re.search(NON_PATTERN, raw_str) or not re.match(_PATTERN, raw_str):
                    return True
            return False
        except:
            return True

    @staticmethod
    def _2_list(blob_data):
        ''' array bytes -> [int...] '''
        try:
            return map(lambda x: int(x), list(blob_data))
        except:
            exc()
            return []
