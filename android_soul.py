# -*- coding: utf-8 -*-
__author__ = "TaoJianping"

import clr

clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')

try:
    clr.AddReference('model_im')
    clr.AddReference('bcp_im')
    clr.AddReference('ScriptUtils')
except Exception:
    pass

del clr

import model_im
import PA_runtime
from ScriptUtils import TaoUtils, ParserBase, TimeHelper, FieldType, FieldConstraints, BaseModel
import System
from PA_runtime import *
from System.Data.SQLite import *
from System.Xml.Linq import *
from System.Xml.XPath import Extensions as XPathExtensions
from PA.InfraLib.Extensions import PlistHelper

# CONST
Soul_VERSION = 2
DEBUG = False


class SoulParser(ParserBase):
    def __init__(self, root, extract_deleted, extract_source):
        super(SoulParser, self).__init__(
            self._get_root_node(root, times=2),
            extract_deleted,
            extract_source,
            app_name="Soul",
            app_version=Soul_VERSION,
            debug=DEBUG,
        )

        self.model_im_col = self.load_im_model()
        self.data_node = self._copy_root()

        self._friend_array = set()

    def _search_message_db(self):
        target_node = self.data_node.GetByPath("/files/easemobDB")
        nodes = target_node.Search("^/\d+.db$")
        return nodes

    def _search_soul_app_db(self):
        return next(iter(self.data_node.Search("soul_app.db$")), None)

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
        if not msg_ext_list:
            return model_im.MESSAGE_CONTENT_TYPE_TEXT

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

    def _parse_message_body(self, msg_obj, serialized_msg):
        msg = TaoUtils.json_loads(serialized_msg)
        if not msg:
            return

        try:
            sender_id = msg["from"]
            receiver_id = msg["to"]
            msg_ext_info = msg.get("ext", None)
            msg_type = self._judge_msg_type(msg_ext_info)

            msg_obj.sender_id = msg_obj.sender_name = sender_id
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

    def _generate_account_table(self):
        """
        创建account table
        """
        msg_db_col = self.message_col
        account_id = os.path.basename(msg_db_col.db_path).split(".")[0]

        db_col = self.soul_app_col
        table = db_col.get_table("user", {
            "_id": [FieldType.Int, FieldConstraints.NotNull],
            "user": [FieldType.Text, FieldConstraints.NotNull],
        })
        for rec in db_col.read_records(table):
            try:
                if str(rec['_id'].Value) == account_id:
                    account_info = TaoUtils.json_loads(rec['user'].Value)
                    if not account_info:
                        continue

                    account = model_im.Account()
                    account.account_id = rec['_id'].Value
                    account.source = db_col.db_path
                    account.signature = account.username = account.nickname = account_info.get("signature", None)
                    account.gender = model_im.GENDER_FEMALE if account_info.get("gender", None) == "FEMALE" \
                        else model_im.GENDER_MALE
                    account.birthday = TimeHelper.convert_timestamp(account_info.get("birthday", None))
                    account.email = account_info.get('bindMail', None)
                    account.country = "China" if account_info.get('area', None) else None
                    account.deleted = 1 if rec.IsDeleted else 0
                else:
                    account = model_im.Account()
                    account.account_id = account.username = account.nickname = account_id
                    account.source = db_col.db_path
                self.model_im_col.db_insert_table_account(account)
            except Exception as e:
                self.logger.error()
        self.model_im_col.db_commit()

    def _generate_friend_table(self):
        """生成friend表的数据"""
        db_col = self.message_col
        for friend_id in self._friend_array:
            try:
                friend = model_im.Friend()
                friend.account_id = os.path.basename(db_col.db_path).split(".")[0]
                friend.friend_id = friend.nickname = friend.fullname = friend_id
                friend.source = db_col.db_path
                self.model_im_col.db_insert_table_friend(friend)
            except Exception as e:
                self.logger.error()
        self.model_im_col.db_commit()

    def _generate_message_table(self):
        """生成消息的表"""
        db_col = self.message_col
        table = db_col.get_table("message", {
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
        for rec in db_col.read_records(table):
            try:
                message = model_im.Message()
                message.account_id = os.path.basename(db_col.db_path).split(".")[0]
                message.source = db_col.db_path
                message.deleted = 1 if rec.IsDeleted else 0
                message.msg_id = rec['msgid'].Value
                message.send_time = TaoUtils.convert_timestamp(rec['servertime'].Value)
                message.status = model_im.MESSAGE_STATUS_READ if rec['isread'].Value == 1 \
                    else model_im.MESSAGE_STATUS_UNREAD
                message.talker_type = model_im.CHAT_TYPE_SYSTEM if rec['conversation'].Value == "admin" \
                    else model_im.CHAT_TYPE_FRIEND
                message.talker_id = rec['conversation'].Value
                message.talker_name = message.sender_name
                self._parse_message_body(message, rec['msgbody'].Value)
                if rec['conversation'].Value != "admin":
                    self._friend_array.add(message.talker_id)
                if message.type == model_im.MESSAGE_CONTENT_TYPE_IMAGE:
                    image_urls = message.media_path[:]
                    for image_url in image_urls:
                        message.media_path = image_url
                        self.model_im_col.db_insert_table_message(message)
                else:
                    self.model_im_col.db_insert_table_message(message)

            except Exception as e:
                self.logger.error()

    def _generate_search_table(self):
        """添加search记录"""
        db_col = self.soul_app_col
        table = db_col.get_table("search_record_post", {
            "_id": [FieldType.Text, FieldConstraints.NotNull],
            "ts": [FieldType.Int, FieldConstraints.NotNull],
        })
        for record in db_col.read_records(table):
            try:
                s = model_im.Search()
                s.key = record['_id'].Value
                s.create_time = TimeHelper.str_to_ts(record['ts'].Value, _format="%Y-%m-%d %H:%M:%S")
                s.deleted = 1 if record.IsDeleted else 0
                s.source = db_col.db_path
                self.model_im_col.db_insert_table_search(s)
            except Exception as e:
                self.logger.error()
        self.model_im_col.db_commit()

    def _update_im_script_version(self, app_version):
        self.model_im_col.db_insert_table_version(model_im.VERSION_KEY_DB, model_im.VERSION_VALUE_DB)
        self.model_im_col.db_insert_table_version(model_im.VERSION_KEY_APP, app_version)
        self.model_im_col.db_commit()

    def _main(self):
        """解析的逻辑主函数"""
        if not self.data_node:
            return

        soul_app_db = self._search_soul_app_db()
        if not soul_app_db:
            return 
        self.soul_app_col = BaseModel(soul_app_db)
        if self.soul_app_col.is_valid():
            self._generate_search_table()

        for msg_db in self._search_message_db():
            self.message_col = BaseModel(msg_db)
            self._generate_account_table()
            self._generate_message_table()
            self._generate_friend_table()

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
    pr.Categories = DescripCategories.ICQ
    results = SoulParser(root, extract_deleted, extract_source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("Soul")
    return pr
