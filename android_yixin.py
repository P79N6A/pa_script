#coding=utf-8

import clr
try:
    clr.AddReference('model_im')
    clr.AddReference('bcp_im')
except:
    pass
del clr

from PA_runtime import *
import hashlib
import json
import model_im
import bcp_im
import gc

DEBUG = True
DEBUG = False

# app 数据库版本
VERSION_APP_VALUE = 2

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
        TraceService.Trace(TraceLevel.Warning, '{}'.format(e))
    else:
        pass


def analyze_yixin(root, extract_deleted, extract_source):
    tp('android_yixin.py is running ...')
    if root.AbsolutePath == '/data/media/0/Android/data/im.yixin':
        return
    pr = ParserResults()
    models = YiXinParser(root, extract_deleted, extract_source).parse()
    mlm = ModelListMerger()
    pr.Models.AddRange(list(mlm.GetUnique(models)))
    pr.Build('易信')
    gc.collect()
    
    tp('android_yixin.py is finished !')
    return pr

def execute(node,extracteDeleted):
    return analyze_yixin(node, extracteDeleted, False)

class YiXinParser():
    def __init__(self, node, extract_deleted, extract_source):
        self.extract_deleted = False
        self.extract_source = extract_source
        self.root = node
        self.im = model_im.IM()
        self.cache_path = ds.OpenCachePath('YiXin')
        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)
        hash_str = hashlib.md5(node.AbsolutePath.encode('utf8')).hexdigest()[8:-8]
        self.cache_db = os.path.join(self.cache_path, 'a_line_{}.db'.format(hash_str))
        
        self.media_node = None

    def parse(self):
        if DEBUG or self.im.need_parse(self.cache_db, VERSION_APP_VALUE):
            self.im.db_create(self.cache_db)
            user_list = self.get_user_list()
            for user in user_list:
                self.friends = {}
                self.chatrooms = {}
                self.user = user
                self.parse_user()
                self.user = None
                self.friends = None
                self.chatrooms = None
            self.im.db_insert_table_version(model_im.VERSION_KEY_DB, model_im.VERSION_VALUE_DB)
            self.im.db_insert_table_version(model_im.VERSION_KEY_APP, VERSION_APP_VALUE)
            self.im.db_commit()
            self.im.db_close()

        nameValues.SafeAddValue(bcp_im.CONTACT_ACCOUNT_TYPE_IM_YIXIN, self.cache_db)

        models  = self.get_models_from_cache_db()
        return models

    def get_models_from_cache_db(self):
        models = model_im.GenerateModel(self.cache_db).get_models()
        return models

    def get_user_list(self):
        user_list = []
        for file in os.listdir(self.root.PathWithMountPoint):
            if file.isdigit():
                user_list.append(file)
        return user_list

    def parse_user(self):
        self.get_user()
        self.get_contacts()
        self.get_chats()

    def get_user(self):
        ''' main.db - yixin_contact
        
            FieldName	SQLType	Size	Precision	PKDisplay	
            uid         	Varchar	16
            yid         	Varchar	64
            ecpid          	Varchar	64
            mobile         	varchar	16
            email          	Varchar	64
            nickname           Varchar	64
            photourl           Varchar	256
            gender         	INTEGER
            birthday           Varchar	16
            address        	Varchar	128
            signature          Varchar	64
            bkimage        	Varchar	256
            fullspelling       Varchar	128
            shortspelling      Varchar	64
            sinaweibo          Varchar	64
            qqweibo        	archar	64
            renren         	Varchar	64
            config         	Varchar	512
            socials        	Varchar	512
        '''
        if self.user is None:
            return

        account = model_im.Account()
        account.account_id = self.user

        dbPath = self.root.GetByPath(self.user + '/main.db')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            self.im.db_insert_table_account(account)
            self.im.db_commit()
            return

        account.source = dbPath.AbsolutePath
        if 'yixin_contact' in db.Tables:
            ts = SQLiteParser.TableSignature('yixin_contact')
            SQLiteParser.Tools.AddSignatureToTable(ts, "uid", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                if account.account_id != rec['uid'].Value:
                    continue
                account.username = rec['nickname'].Value
                self.username = account.username
                account.gender = 2 if rec['gender'].Value == 0 else 1
                account.email = rec['email'].Value
                account.signature = rec['signature'].Value
                account.address = rec['address']
        self.im.db_insert_table_account(account)
        self.im.db_commit()

    def get_contacts(self):
        if self.user is None:
            return

        dbPath = self.root.GetByPath(self.user + '/main.db')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return

        if 'yixin_contact' in db.Tables:
            ts = SQLiteParser.TableSignature('yixin_contact')
            SQLiteParser.Tools.AddSignatureToTable(ts, "uid", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                if canceller.IsCancellationRequested:
                    return
                id = rec['uid'].Value
                if id in self.friends:
                    continue

                friend = model_im.Friend()
                friend.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                friend.source = 'YiXin'
                friend.account_id = self.user
                friend.friend_id = id
                friend.nickname = rec['nickname'].Value
                friend.photo = rec['photourl'].Value
                friend.gender = 2 if rec['gender'].Value == 0 else 1
                friend.signature = rec['signature'].Value
                friend.email = rec['email'].Value
                friend.address = rec['address'].Value
                friend.type = model_im.FRIEND_TYPE_FRIEND
                self.friends[id] = friend
                if 'buddylist' in db.Tables:
                    ts = SQLiteParser.TableSignature('buddylist')
                    SQLiteParser.Tools.AddSignatureToTable(ts, "uid", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                    for rec in db.ReadTableRecords(ts, self.extract_deleted):
                        if friend.friend_id != rec['uid']:
                            continue
                        friend.remark = rec['alias'].Value
                self.im.db_insert_table_friend(friend)
                
        if 'tinfo' in db.Tables:
            ts = SQLiteParser.TableSignature('tinfo')
            SQLiteParser.Tools.AddSignatureToTable(ts, "tid", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                id = rec['tid'].Value
                if id in self.chatrooms:
                    continue

                chatroom = model_im.Chatroom()
                chatroom.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                chatroom.source = 'YiXin'
                chatroom.account_id = self.user
                chatroom.chatroom_id = id
                chatroom.name = rec['defaultname'].Value
                chatroom.creator_id = rec['creator'].Value
                chatroom.photo = rec['photo'].Value
                chatroom.member_count = rec['membercount'].Value
                chatroom.type = model_im.CHATROOM_TYPE_NORMAL
                self.chatrooms[id] = chatroom
                self.im.db_insert_table_chatroom(chatroom)
        
                if 'tuser' in db.Tables:
                    ts = SQLiteParser.TableSignature('tuser')
                    SQLiteParser.Tools.AddSignatureToTable(ts, "tid", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                    for rec in db.ReadTableRecords(ts, self.extract_deleted):
                        if canceller.IsCancellationRequested:
                            return
                        room_id = rec['tid'].Value
                        if chatroom.chatroom_id != room_id:
                            continue

                        chatroom_member = model_im.ChatroomMember()
                        chatroom_member.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                        chatroom_member.source = 'YiXin'
                        chatroom_member.account_id = self.user
                        chatroom_member.chatroom_id = room_id
                        chatroom_member.member_id = rec['uid'].Value
                        friend = self.friends.get(chatroom_member.member_id)
                        if friend is not None:
                            chatroom_member.display_name = friend.nickname
                            chatroom_member.email = friend.email
                            chatroom_member.gender = friend.gender
                            chatroom_member.address = friend.address
                            chatroom_member.signature = friend.signature
                            chatroom_member.photo = friend.photo
                        self.im.db_insert_table_chatroom_member(chatroom_member)

        if 'painfo' in db.Tables:
            ts = SQLiteParser.TableSignature('painfo')
            SQLiteParser.Tools.AddSignatureToTable(ts, "uid", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                if canceller.IsCancellationRequested:
                    return
                id = rec['uid'].Value
                if id in self.friends:
                    continue

                friend = model_im.Friend()
                friend.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                friend.source = 'YiXin'
                friend.account_id = self.user
                friend.friend_id = id
                friend.nickname = rec['nickname'].Value
                friend.photo = rec['photourl'].Value
                friend.gender = rec['gender'].Value
                friend.signature = rec['signature'].Value
                friend.type = model_im.FRIEND_TYPE_FOLLOW
                self.friends[friend.friend_id] = friend
                self.im.db_insert_table_friend(friend)
        self.im.db_commit()
                        
    def get_chats(self):
        '''
            FieldName	SQLType         	
            seqid	        Long
            msgid	        Varchar
            id	            Varchar
            fromid	        Varchar
            sessiontype	    Integer
            time	        Long
            status	        Integer
            direct	        Integer
            msgtype	        Integer
            content	        Varchar
            extra	        TEXT
            attachstr	    Varchar
            msgSvrId	    Long
        '''
        if self.user is None:
            return

        dbPath = self.root.GetByPath(self.user + '/msg.db')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return

        for id in self.friends.keys() or self.chatrooms.keys():
            if 'msghistory' in db.Tables:
                ts = SQLiteParser.TableSignature('msghistory')
                SQLiteParser.Tools.AddSignatureToTable(ts, "tid", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                for rec in db.ReadTableRecords(ts, self.extract_deleted):
                    if canceller.IsCancellationRequested:
                        return
                    if id != rec['id'].Value:
                        continue
                    friend = self.friends.get(id)
                    
                    message = model_im.Message()
                    message.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    message.source = dbPath.AbsolutePath
                    message.account_id = self.user
                    message.talker_id = id
                    message.talker_type = model_im.CHAT_TYPE_FRIEND if id in self.friends.keys() else model_im.CHAT_TYPE_GROUP
                    message.talker_name = friend.nickname
                    message.is_sender = model_im.MESSAGE_TYPE_SEND if rec['fromid'].Value == self.user else model_im.MESSAGE_TYPE_RECEIVE
                    message.sender_id = rec['fromid'].Value
                    message.sender_name = self.username if message.is_sender == model_im.MESSAGE_TYPE_SEND else message.talker_name 
                    message.msg_id = rec['msgid'].Value
                    message.type = self.parse_message_type(rec['msgtype'].Value)
                    message.send_time = rec['time'].Value
                    message.content = rec['content'].Value

                    message.media_path = self.get_media_path(rec['attachstr'].Value, message.type)

                    if message.type == model_im.MESSAGE_CONTENT_TYPE_LOCATION:
                        message.location_obj = message.create_location()
                        message.location_id = self.get_location(message.location_obj, message.content, rec['attachstr'].Value, message.send_time)

                    self.im.db_insert_table_message(message)
        self.im.db_commit()

    def parse_message_type(self, raw_msgtype):
        msgtype = model_im.MESSAGE_CONTENT_TYPE_TEXT
        if raw_msgtype == 1:
            msgtype = model_im.MESSAGE_CONTENT_TYPE_IMAGE
        elif raw_msgtype == 2:
            msgtype = model_im.MESSAGE_CONTENT_TYPE_VOICE
        elif raw_msgtype == 3:
            msgtype = model_im.MESSAGE_CONTENT_TYPE_VIDEO
        elif raw_msgtype == 4:
            msgtype = model_im.MESSAGE_CONTENT_TYPE_LOCATION
        return msgtype

    def get_media_path(self, attachstr, msg_type):
        '''[summary]
        
        Args:
            attachstr ([type]): [description]
            type ([type]): [description]
        
        Returns:
            [type]: [description]
            {
                'id': 27,
                'key': '3d7e404c305007a660a95bcfbd81543d',
                'medialen': 13153,
                'mimetype': 'video/mp4',
                'name': '3d7e404c305007a660a95bcfbd81543d.mp4',
                'size': 1617189,
                'status': 2,
                'url': 'http://nos-yx.netease.com/yixinpublic/pr_azmjz0hhtlspvuaamywglw==_50_1524639201_15502109'
            }            
        '''
        try:
            if IsDBNull(attachstr) or not attachstr:
                return None
            obj = json.loads(attachstr)
            file_name = obj.get('name', None)
            relative_file_path = file_name[0:2] + '/' + file_name[2:4] + '/' + file_name

            media_patterns = {
                'audio': 'Yixin/audio/',
                'image': 'Yixin/image/',
                'video': 'Yixin/video/',
            }
            if msg_type == model_im.MESSAGE_CONTENT_TYPE_VOICE:
                pattern = media_patterns['audio'] + relative_file_path
            elif msg_type == model_im.MESSAGE_CONTENT_TYPE_IMAGE:
                pattern = media_patterns['image'] + relative_file_path
            elif msg_type == model_im.MESSAGE_CONTENT_TYPE_VIDEO:
                pattern = media_patterns['video'] + relative_file_path
            else:
                return 
            
            media_path =  self._search_media_file(pattern)
            return media_path
        except:
            return None
            
    def get_location(self, location, content, attachstr, time):
        location.account_id = self.user
        location.timestamp = time
        location.type = model_im.LOCATION_TYPE_GOOGLE
        location.latitude = content.split(',')[0]
        location.longitude = content.split(',')[1]
        try:
            obj = json.loads(attachstr)
            location.address = obj['desc']
        except:
            traceback.print_exc()
        self.im.db_insert_table_location(location)
        self.im.db_commit()
        return location.location_id

    def _search_media_file(self, raw_file_path, path_flag='Yixin'):
        try:
            if not raw_file_path:
                return 
            if self.media_node:
                file_path = raw_file_path.split(path_flag)[1]
                _node = self.media_node.GetByPath(file_path)
                if _node:
                    tp('!!!!!!!!! find file_path:', _node.AbsolutePath) 
                    return _node.AbsolutePath
            else:  # save media file node
                res_file_path = self._fs_search(raw_file_path)
                if res_file_path and path_flag in res_file_path:
                    _path = res_file_path.split(path_flag)[0]
                    self.media_node = self.root.FileSystem.GetByPath(_path+path_flag)
                if res_file_path:
                    tp('!!!!!!!!! find file_path:', res_file_path) 
                return res_file_path
        except:
            exc()
            return         

    def _fs_search(self, raw_file_path):
        ''' fs search
        
        Args:
            raw_file_path ([type]): 
        
        Returns:
            node.AbsolutePath: 
        '''
        fs = self.root.FileSystem
        try:
            return list(fs.Search(raw_file_path))[0].AbsolutePath
        except:
            return None


