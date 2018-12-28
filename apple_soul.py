# -*- coding: utf-8 -*-
__author__ = "TaoJianping"

import clr

clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')

try:
    clr.AddReference('model_im')
    clr.AddReference('bcp_im')
    clr.AddReference('tao')
except Exception:
    pass

del clr

import model_im
import PA_runtime
from tao import ModelCol, RecoverTableHelper, TaoUtils, ParserBase, TimeHelper, FieldType, FieldConstraints
import System
from PA_runtime import *
from System.Data.SQLite import *
from System.Xml.Linq import *
from System.Xml.XPath import Extensions as XPathExtensions
from PA.InfraLib.Extensions import PlistHelper

# CONST
Soul_VERSION = 1
DEBUG = True


class SoulParser(ParserBase):
    def __init__(self, root, extract_deleted, extract_source):
        super(SoulParser, self).__init__(
            self._get_root_node(root, times=3),
            extract_deleted,
            extract_source,
            app_name="Soul",
            app_version=Soul_VERSION,
            debug=DEBUG,
        )

        self.model_im_col = self.load_im_model()
        self.data_node = self._copy_root()

        self.friend_col = None
        self.friend_recover_col = None
        self._account_list = []

    def _search_account_info_file(self):
        node = next(iter(self.data_node.Search("com.soulapp.cn.plist$")), None)
        return node

    def _search_friends_db(self):
        node = next(iter(self.data_node.Search("userinfo.db$")), None)
        return node

    def _search_message_db(self, account_id):
        messge_db_name = "{}.db".format(account_id)
        return next(iter(self.data_node.Search(messge_db_name + "$")), None)

    def _search_account_list(self):
        account_list = []
        target_node = self.data_node.GetByPath("/Documents/HyphenateSDK/easemobDB")
        nodes = target_node.Search("^/\d+.db$")
        for n in nodes:
            account_id = n.Name.split(".")[0]
            account_list.append(account_id)
        return account_list

    def _get_account_id_from_conversation_id(self, conversation_id):
        for account_id in self._account_list:
            if conversation_id.startswith(account_id):
                return account_id
        return None

    @staticmethod
    def _get_video_path(msg_ext_info):
        for d in msg_ext_info:
            (key, value), = d.items()
            if key == 'remoteUrl':
                return value
        return None

    def _get_audio_path(self, msg_body):
        """audio 有远程路径和本地路径，优先返回本地路径，没有则返回远程路径"""
        url = msg_body['url']
        file_name = msg_body['filename']
        file_ = self._search_file(file_name)
        if file_ is not None:
            return file_.PathWithMountPoint
        else:
            return url

    def _get_image_path(self, msg_ext_info):
        for d in msg_ext_info:
            for k, v in d.items():
                if k == 'images':
                    images_array = TaoUtils.json_loads(v)
                    return [i['imageUrl'] for i in images_array]
        return None

    def _get_card_desc(self, msg_ext_info):
        for d in msg_ext_info:
            for k, v in d.items():
                if k == 'cardDescs':
                    desc = "".join([w for w in TaoUtils.json_loads(v)])
                    return desc
        return None

    @staticmethod
    def _judge_msg_type(msg_ext_list):
        """
        判断消息的类型
        :param msg_ext_info: 里面包含了消息的类型信息
        :return:
        """
        msg_ext_info = {}
        for d in msg_ext_list:
            msg_ext_info.update(d)
        message_type = msg_ext_info.get('messageType', None)
        audio_chat = msg_ext_info.get('audio_chat', None)
        is_introduction_card = msg_ext_info.get('IntroductionCard', None)
        is_chat_prompt = msg_ext_info.get('ChatPrompt', None)

        if message_type == "TXT" and audio_chat == "1":
            return model_im.MESSAGE_CONTENT_TYPE_VOIP
        elif message_type == "AUDIO":
            return model_im.MESSAGE_CONTENT_TYPE_VOICE
        elif message_type == "VIDEO":
            return model_im.MESSAGE_CONTENT_TYPE_VIDEO
        elif message_type == "PIC":
            return model_im.MESSAGE_CONTENT_TYPE_IMAGE
        elif is_introduction_card == "1" or is_chat_prompt == "1":
            return model_im.MESSAGE_CONTENT_TYPE_SYSTEM
        else:
            return model_im.MESSAGE_CONTENT_TYPE_TEXT

    @staticmethod
    def _read_search_records(search_file):
        tree = BPReader.GetTree(search_file.Data)
        search_array = []
        for i in range(len(tree)):
            if tree[i].Key.startswith("kSearchHistoryArray"):
                search_array.extend(tree[i].Value)
                break
        return search_array

    def _parse_message_body(self, msg_obj, serialized_msg):
        msg = TaoUtils.json_loads(serialized_msg)
        if not msg:
            return

        try:
            sender_id = msg["from"]
            receiver_id = msg["to"]
            msg_ext_info = msg['ext']
            msg_type = self._judge_msg_type(msg_ext_info)

            msg_obj.sender_id = sender_id
            msg_obj.is_sender = 1 if msg_obj.account_id == msg_obj.sender_id else 0
            msg_obj.type = msg_type

            if msg_type == model_im.MESSAGE_CONTENT_TYPE_TEXT:
                msg_obj.content = msg["bodies"][0]['msg']
            elif msg_type == model_im.MESSAGE_CONTENT_TYPE_SYSTEM:
                msg_obj.content = msg["bodies"][0]['msg'] if msg["bodies"][0]['msg'] != "[卡片]" else self._get_card_desc(
                    msg_ext_info)
            elif msg_type == model_im.MESSAGE_CONTENT_TYPE_IMAGE:
                msg_obj.media_path = self._get_image_path(msg_ext_info)
            elif msg_type == model_im.MESSAGE_CONTENT_TYPE_VIDEO:
                msg_obj.media_path = self._get_video_path(msg_ext_info)
            elif msg_type == model_im.MESSAGE_CONTENT_TYPE_VOICE:
                msg_obj.media_path = self._get_audio_path(msg["bodies"][0])
            elif msg_type == model_im.MESSAGE_CONTENT_TYPE_VOIP:
                msg_obj.content = msg["bodies"][0]['msg']

        except Exception as e:
            print(serialized_msg)
            self.logger.error()

    def _query_sender_name(self, account_id, friend_id):
        conversation_id = "".join((account_id + friend_id))
        with self.friend_col as db_col:
            sql = """SELECT signatrue
                        FROM userInfo
                        WHERE converstionID = '{}'""".format(conversation_id)
            reader = db_col.fetch_reader(sql)
            while reader.Read():
                sender_name = db_col.fetch_string(reader, 0)
                return sender_name
            return None

    def _generate_account_table(self):
        """
        创建account table
        """
        account_file = self._search_account_info_file()
        if not account_file:
            return

        file_info = PlistHelper.ReadPlist(account_file)
        if not file_info:
            return

        for account_id in self._account_list:
            try:
                if not account_file:
                    return
                account = model_im.Account()
                account.account_id = account.username = account.nickname = account_id
                account.email = file_info.Get('USER_MAIL' + account_id)
                birth_day = file_info.Get('currentUserBrithday' + account_id)
                if birth_day:
                    account.birthday = TimeHelper.str_to_ts(birth_day.ToString())
                if str(file_info['userInformation'].Get("chatUserID")) == account_id:
                    account.password = file_info['userInformation'].Get("loginUserPassWord")
                    account.telephone = file_info['userInformation'].Get("loginUserPhone")
                    account.signature = file_info["signatureText"]
                    account.username = file_info["userChatUsername"]
                    account.gender = model_im.GENDER_FEMALE if file_info["gender"] == "女" else model_im.GENDER_MALE
                account.nickname = account.signature
                # account.photo = account_info.get("account_info", {}).get("photo_url", None)
                account.source = account_file.PathWithMountPoint
                self.model_im_col.db_insert_table_account(account)
            except Exception as e:
                self.logger.error()
        self.model_im_col.db_commit()

    def _add_friend_record(self):
        with self.friend_col as db_col:
            sql = """SELECT converstionID, 
                            headIcon, 
                            headBG, 
                            alics, 
                            signatrue, 
                            closeLevel, 
                            comeFromStr,
                            mutualFollow 
                        FROM userInfo;"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    friend = model_im.Friend()
                    friend.account_id = self._get_account_id_from_conversation_id(db_col.get_string(0))
                    if not friend.account_id:
                        continue
                    friend.friend_id = db_col.get_string(0)[len(friend.account_id):]
                    friend.signature = db_col.get_string(4)
                    friend.nickname = db_col.get_string(3) if db_col.get_string(3) else friend.signature
                    friend.type = model_im.FRIEND_TYPE_MUTUAL_FOLLOW if db_col.get_string(7) == "1" \
                        else model_im.FRIEND_TYPE_STRANGER
                    friend.source = db_col.db_path
                    self.model_im_col.db_insert_table_friend(friend)
                except Exception as e:
                    self.logger.error()
            self.model_im_col.db_commit()

    def _recover_friend_record(self):
        recover_col = self.friend_recover_col
        if not recover_col.is_valid():
            return
        ts = recover_col.get_table("userInfo", {
            "converstionID": [FieldType.Text, FieldConstraints.NotNull],
            "signatrue": [FieldType.Text, FieldConstraints.NotNull],
            "comeFromStr": [FieldType.Text, FieldConstraints.NotNull],
        })

        for record in recover_col.read_deleted_records(ts):
            try:
                friend = model_im.Friend()
                friend.account_id = self._get_account_id_from_conversation_id(record['converstionID'].Value)
                if not record['converstionID'].Value.startswith(friend.account_id):
                    continue
                friend.friend_id = record['converstionID'].Value[len(friend.account_id):]
                friend.signature = record['signatrue'].Value
                friend.nickname = friend.signature
                friend.type = model_im.FRIEND_TYPE_MUTUAL_FOLLOW if record['mutualFollow'].Value == "1" \
                    else model_im.FRIEND_TYPE_STRANGER
                friend.source = recover_col.db_path
                self.model_im_col.db_insert_table_friend(friend)
            except Exception as e:
                self.logger.error()
        self.model_im_col.db_commit()

    def _generate_friend_table(self):
        """生成friend表的数据"""
        friends_db = self._search_friends_db()

        if not friends_db:
            return

        self.friend_col = ModelCol(friends_db)
        self.friend_recover_col = RecoverTableHelper(friends_db)

        self._add_friend_record()
        self._recover_friend_record()

    def _add_message_record(self, account_id):
        with self.message_col as db_col:
            sql = """SELECT msgid, 
                            msgtime, 
                            msgdirection, 
                            conversation, 
                            isread, 
                            isacked, 
                            isdelivered, 
                            islistened, 
                            status, 
                            msgbody, 
                            msgtype, 
                            bodytype, 
                            servertime 
                        FROM message;"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    message = model_im.Message()
                    message.account_id = account_id
                    message.source = db_col.db_path
                    message.msg_id = db_col.get_string(0)
                    message.send_time = TaoUtils.convert_timestamp(db_col.get_int64(1))
                    message.status = model_im.MESSAGE_STATUS_READ if db_col.get_int64(4) == 1 \
                        else model_im.MESSAGE_STATUS_UNREAD
                    message.talker_type = model_im.CHAT_TYPE_FRIEND
                    message.talker_id = db_col.get_string(3)
                    self._parse_message_body(message, db_col.get_string(9))
                    message.sender_name = message.talker_name = self._query_sender_name(message.account_id, message.sender_id)
                    if message.type == model_im.MESSAGE_CONTENT_TYPE_IMAGE:
                        image_urls = message.media_path[:]
                        for image_url in image_urls:
                            message.media_path = image_url
                            self.model_im_col.db_insert_table_message(message)
                    else:
                        self.model_im_col.db_insert_table_message(message)
                except Exception as e:
                    self.logger.error()

            self.model_im_col.db_commit()

    def _recover_message_record(self, account_id):
        recover_col = self.message_recover_col
        if not recover_col.is_valid():
            return
        ts = recover_col.get_table("message", {
            "msgid": [FieldType.Text, FieldConstraints.NotNull],
            "msgtime": [FieldType.Int, FieldConstraints.NotNull],
            "conversation": [FieldType.Text, FieldConstraints.NotNull],
            "isread": [FieldType.Int, FieldConstraints.NotNull],
            "status": [FieldType.Int, FieldConstraints.NotNull],
            "msgbody": [FieldType.Text, FieldConstraints.NotNull],
            "msgtype": [FieldType.Int, FieldConstraints.NotNull],
            "bodytype": [FieldType.Int, FieldConstraints.NotNull],
            "servertime": [FieldType.Int, FieldConstraints.NotNull],
        })
        for rec in recover_col.read_deleted_records(ts):
            try:
                message = model_im.Message()
                message.account_id = account_id
                message.source = recover_col.db_path
                message.deleted = 1
                message.msg_id = rec['msgid'].Value
                message.send_time = TaoUtils.convert_timestamp(rec['msgtime'].Value)
                message.status = model_im.MESSAGE_STATUS_READ if rec['isread'].Value == 1 \
                    else model_im.MESSAGE_STATUS_UNREAD
                message.talker_type = model_im.CHAT_TYPE_FRIEND
                message.talker_id = rec['conversation'].Value
                self._parse_message_body(message, rec['msgbody'].Value)
                message.sender_name = self._query_sender_name(message.account_id, message.sender_id)
                message.talker_name = message.sender_name

                if message.type == model_im.MESSAGE_CONTENT_TYPE_IMAGE:
                    image_urls = message.media_path[:]
                    for image_url in image_urls:
                        message.media_path = image_url
                        self.model_im_col.db_insert_table_message(message)
                else:
                    self.model_im_col.db_insert_table_message(message)

            except Exception as e:
                self.logger.error()
        self.model_im_col.db_commit()

    def _generate_message_table(self):
        """生成消息的表"""
        for account_id in self._account_list:
            message_db = self._search_message_db(account_id)
            if not message_db:
                continue
            self.message_col = ModelCol(message_db)
            self.message_recover_col = RecoverTableHelper(message_db)

            self._add_message_record(account_id)
            self._recover_message_record(account_id)

    def _add_search_record(self, search_file):
        search_keys = self._read_search_records(search_file)

        for key in search_keys:
            try:
                search = model_im.Search()
                search.key = key.Value
                search.source = search_file.PathWithMountPoint
                self.model_im_col.db_insert_table_search(search)
            except Exception as e:
                self.logger.error()

        self.model_im_col.db_commit()

    def _generate_search_table(self):
        """添加search记录"""
        search_file = next(iter(self.data_node.Search("com.soulapp.cn.plist$")), None)
        if not search_file:
            return
        self._add_search_record(search_file)

    def _update_im_script_version(self, app_version):
        self.model_im_col.db_insert_table_version(model_im.VERSION_KEY_DB, model_im.VERSION_VALUE_DB)
        self.model_im_col.db_insert_table_version(model_im.VERSION_KEY_APP, app_version)
        self.model_im_col.db_commit()

    def _main(self):
        """解析的逻辑主函数"""
        if not self.data_node:
            return

        # 这边会生成出能解析的用户的数字id
        self._account_list = self._search_account_list()
        if not self._account_list:
            return

        self._generate_account_table()
        self._generate_friend_table()
        self._generate_message_table()
        self._generate_search_table()

    def parse(self):
        """程序入口"""
        if self.debug or self.model_im_col.need_parse(self.cache_db, Soul_VERSION):
            self.model_im_col.db_create(self.cache_db)
            self._main()
            self._update_im_script_version(Soul_VERSION)
            self.model_im_col.db_close()

        return self._generate_im_models()


def analyze_Soul(root, extract_deleted, extract_source):
    pr = ParserResults()
    pr.Categories = DescripCategories.Soul
    results = SoulParser(root, extract_deleted, extract_source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("Soul")
    return pr
