#coding=utf-8

import clr
try:
    clr.AddReference('model_im')
    clr.AddReference('bcp_im')
    clr.AddReference('ScriptUtils')
except:
    pass
del clr

from PA_runtime import *
import hashlib
import json
import model_im
import bcp_im
import gc
from ScriptUtils import DEBUG, CASE_NAME, exc, tp, base_analyze, parse_decorator, BaseAndroidParser 


# app 数据库版本
VERSION_APP_VALUE = 4

@parse_decorator
def analyze_yixin(root, extract_deleted, extract_source):
    if root.AbsolutePath == '/data/media/0/Android/data/im.yixin/mobidroid.sqlite':
        return
    pr = ParserResults()
    _pr = base_analyze(AndroidYiXinParser, 
                       root, 
                       bcp_im.CONTACT_ACCOUNT_TYPE_IM_YIXIN, 
                       VERSION_APP_VALUE,
                       build_name='易信',
                       db_name='yixin_i')  
    gc.collect()
    pr.Add(_pr)
    return pr
                    

class AndroidYiXinParser(BaseAndroidParser):
    def __init__(self, node, db_name):
        super(AndroidYiXinParser, self).__init__(node, db_name)
        self.root = node.Parent.Parent
        self.csm = model_im.IM()
        self.Generate = model_im.GenerateModel
        self.user_id_list = []
        self.media_node = None
        self.user_id = ''

    def parse(self, BCP_TYPE, VERSION_APP_VALUE):
        self.user_id_list = self.get_user_id_list()
        if not self.user_id_list:
            return []
        model = super(AndroidYiXinParser, self).parse(BCP_TYPE, VERSION_APP_VALUE)
        mlm = ModelListMerger()
        return list(mlm.GetUnique(model))

    def parse_main(self):
        for user in self.user_id_list:

            self.friends = {}
            self.chatrooms = {}
            self.user_id = user
            tp(user)
            self.get_user()
            self.get_contacts()
            self.get_chats()

            self.user_id = ''
            self.friends = None
            self.chatrooms = None

    def get_user_id_list(self):
        user_id_list = []
        for file_name in os.listdir(self.root.PathWithMountPoint):

            if file_name.isdigit():
                user_id_list.append(file_name)
        return user_id_list

    def get_user(self):
        ''' main.db - yixin_contact
        
            FieldName	SQLType	
            uid         	Varchar	16
            yid         	Varchar	64
            ecpid          	Varchar	64
            mobile         	varchar	16
            email          	Varchar	64
            nickname        Varchar	64
            photourl        Varchar	256
            gender         	INTEGER
            birthday        Varchar	16
            address        	Varchar	128
            signature       Varchar	64
            bkimage        	Varchar	256
            fullspelling    Varchar	128
            shortspelling   Varchar	64
            sinaweibo       Varchar	64
            qqweibo        	archar	64
            renren         	Varchar	64
            config         	Varchar	512
            socials        	Varchar	512
        '''
        account = model_im.Account()
        account.account_id = self.user_id

        dbPath = self.root.GetByPath(self.user_id + '/main.db')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            self.csm.db_insert_table_account(account)
            self.csm.db_commit()
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
                account.address = rec['address'].Value
        self.csm.db_insert_table_account(account)
        self.csm.db_commit()
        
    def get_contacts(self):
        dbPath = self.root.GetByPath(self.user_id + '/main.db')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return

        if 'yixin_contact' in db.Tables:
            ts = SQLiteParser.TableSignature('yixin_contact')
            SQLiteParser.Tools.AddSignatureToTable(ts, "uid", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                _id = rec['uid'].Value
                if _id in self.friends:
                    continue

                friend = model_im.Friend()
                friend.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                friend.source = 'YiXin'
                friend.account_id = self.user_id
                friend.friend_id = _id
                friend.nickname = rec['nickname'].Value
                friend.photo = rec['photourl'].Value
                friend.gender = 2 if rec['gender'].Value == 0 else 1
                friend.signature = rec['signature'].Value
                friend.email = rec['email'].Value
                friend.address = rec['address'].Value
                friend.type = model_im.FRIEND_TYPE_FRIEND
                self.friends[_id] = friend
                if 'buddylist' in db.Tables:
                    ts = SQLiteParser.TableSignature('buddylist')
                    SQLiteParser.Tools.AddSignatureToTable(ts, "uid", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                    for rec in db.ReadTableRecords(ts, self.extract_deleted):
                        if friend.friend_id != rec['uid']:
                            continue
                        friend.remark = rec['alias'].Value
                self.csm.db_insert_table_friend(friend)
                
        if 'tinfo' in db.Tables:
            ts = SQLiteParser.TableSignature('tinfo')
            SQLiteParser.Tools.AddSignatureToTable(ts, "tid", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                _id = rec['tid'].Value
                if _id in self.chatrooms:
                    continue

                chatroom = model_im.Chatroom()
                chatroom.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                chatroom.source = 'YiXin'
                chatroom.account_id = self.user_id
                chatroom.chatroom_id = _id
                chatroom.name = rec['defaultname'].Value
                chatroom.creator_id = rec['creator'].Value
                chatroom.photo = rec['photo'].Value
                chatroom.member_count = rec['membercount'].Value
                chatroom.type = model_im.CHATROOM_TYPE_NORMAL
                self.chatrooms[_id] = chatroom
                self.csm.db_insert_table_chatroom(chatroom)
        
                if 'tuser' in db.Tables:
                    ts = SQLiteParser.TableSignature('tuser')
                    SQLiteParser.Tools.AddSignatureToTable(ts, "tid", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                    for rec in db.ReadTableRecords(ts, self.extract_deleted):
                        room_id = rec['tid'].Value
                        if chatroom.chatroom_id != room_id:
                            continue

                        chatroom_member = model_im.ChatroomMember()
                        chatroom_member.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                        chatroom_member.source = 'YiXin'
                        chatroom_member.account_id = self.user_id
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
                        self.csm.db_insert_table_chatroom_member(chatroom_member)

        if 'painfo' in db.Tables:
            ts = SQLiteParser.TableSignature('painfo')
            SQLiteParser.Tools.AddSignatureToTable(ts, "uid", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                _id = rec['uid'].Value
                if _id in self.friends:
                    continue

                friend = model_im.Friend()
                friend.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                friend.source = 'YiXin'
                friend.account_id = self.user_id
                friend.friend_id = _id
                friend.nickname = rec['nickname'].Value
                friend.photo = rec['photourl'].Value
                friend.gender = rec['gender'].Value
                friend.signature = rec['signature'].Value
                friend.type = model_im.FRIEND_TYPE_FOLLOW
                self.friends[friend.friend_id] = friend
                self.csm.db_insert_table_friend(friend)
        self.csm.db_commit()
                        
    def get_chats(self):
        '''
            FieldName	SQLType         	
            seqid	        Long    PK
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
        dbPath = self.root.GetByPath(self.user_id + '/msg.db')
        if not self._read_db(node=dbPath):
            return
        for _id in self.friends.keys() or self.chatrooms.keys():
            if 'msghistory' in self.cur_db.Tables:
                for rec in self._read_table('msghistory'):
                    if (self._is_empty(rec, 'seqid', 'id')
                        or self._is_duplicate(rec, 'seqid')):
                        continue
                    if _id != rec['id'].Value:
                        continue
                    friend = self.friends.get(_id)
                    
                    message = model_im.Message()
                    message.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    message.source = dbPath.AbsolutePath
                    message.account_id = self.user_id
                    message.talker_id = _id
                    message.talker_type = model_im.CHAT_TYPE_FRIEND if _id in self.friends.keys() else model_im.CHAT_TYPE_GROUP
                    message.talker_name = friend.nickname
                    message.is_sender = model_im.MESSAGE_TYPE_SEND if rec['fromid'].Value == self.user_id else model_im.MESSAGE_TYPE_RECEIVE
                    message.sender_id = rec['fromid'].Value
                    message.sender_name = self.username if message.is_sender == model_im.MESSAGE_TYPE_SEND else message.talker_name 
                    message.msg_id = rec['msgid'].Value
                    message.type = self.parse_message_type(rec['msgtype'].Value)
                    message.send_time = rec['time'].Value
                    message.content = parse_yixin_msg_content(rec['content'].Value)
                    message.media_path = self.get_media_path(rec['attachstr'].Value, message.type)

                    if message.type == model_im.MESSAGE_CONTENT_TYPE_LOCATION:
                        message.location_obj = message.create_location()
                        message.location_id = self.get_location(message.location_obj, rec['content'].Value, rec['attachstr'].Value, message.send_time)

                    self.csm.db_insert_table_message(message)
        self.csm.db_commit()

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
        location.account_id = self.user_id
        location.timestamp = time
        location.type = model_im.LOCATION_TYPE_GOOGLE
        location.latitude = content.split(',')[0]
        location.longitude = content.split(',')[1]
        try:
            obj = json.loads(attachstr)
            location.address = obj['desc']
        except:
            traceback.print_exc()
        self.csm.db_insert_table_location(location)
        self.csm.db_commit()
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


def parse_yixin_msg_content(_content):
    ''' b = {
            "subtype": 101,
            "date": 1541492234,
            "data": {
                "items": [
                    {
                        "title": "【有奖】明日之后：一人一狗闯末日",
                        "subTitle": "末日之下暗藏危机！幸好有狗子相伴~上传你与【明日之后】中狗子的合影，100%得游戏礼包，更有机会获得游戏周边！",
                        "desc": "末日之下暗藏危机！幸好有狗子相伴~上传你与【明日之后】中狗子的合影，100%得游戏礼包，更有机会获得游戏周边！",
                        "linkurl": "http://wap.plus.yixin.im/wap/material/viewImageText?id=40724268",
                        "start": 0,
                        "end": 0,
                        "image": {
                            "key": "439ec256003de90ed93677dc215d8c82",
                            "name": "公众号-720x400.jpg",
                            "size": 93450,
                            "url": "http://nos-yx.netease.com/yixinpublic/pr_vri4f9p8nqvxfoickygkxw==_1541399868_345256264"
                        },
                        "subsubtype": 0
                    }
                ]
            }
        }
    ''' 
    try:
        _d = json.loads(_content)
        res = []
        for _item in _d.get('data', {}).get('items', []):
            res.append(_item.get('linkurl', ''))
        return ', '.join(_linkurl for _linkurl in res)
    except:
        return _content

