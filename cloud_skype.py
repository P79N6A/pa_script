# coding=utf-8
__author__ = 'TaoJianping'

import clr

try:
    clr.AddReference('model_nd')
    clr.AddReference('ScriptUtils')
except:
    pass

clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')

del clr
import PA_runtime
import model_im
import json
import System
import hashlib
from PA_runtime import *
from System.Data.SQLite import *
from System.Xml.Linq import *
from System.Xml.XPath import Extensions as XPathExtensions
from ScriptUtils import TimeHelper, YunkanParserBase, PaHtmlParser, TaoUtils

# const
DEBUG = True
SKYPE_VERSION = 1


def print_error():
    if DEBUG:
        TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))


class YunkanSkypePanParser(YunkanParserBase):
    """
    云勘数据 备份解析 -> skype
        1. 好友
        2. 消息
    """

    def __init__(self, node, extract_deleted, extract_source, app_name='YunkanSkype'):
        super(YunkanSkypePanParser, self).__init__(node, extract_deleted, extract_source, app_name)
        self.app_version = SKYPE_VERSION
        self.account_id = self._get_owner_phone(node)
        self.model_im_col = model_im.IM()
        self.debug = DEBUG

    @staticmethod
    def _get_owner_phone(node):
        return 18256078414

    @staticmethod
    def assemble_message_delete_member(executor, target):
        if executor == target:
            return "{} 已离开此对话".format(executor)
        return "{} 已将 {} 从此对话中移除".format(executor, target)

    @staticmethod
    def assemble_message_add_member(executor, target):
        if executor == target:
            return "{} 已加入此对话".format(executor)
        return "{} 已将 {} 添加进此对话".format(executor, target)

    @staticmethod
    def assemble_message_emoji(nodes):
        if not nodes:
            return None
        return "".join(node.data for node in nodes)

    @staticmethod
    def assemble_message_call(account_id, root_node):
        call_type = root_node['partlist'].property['type']
        if call_type == 'missed':
            caller_id = root_node['partlist'].child[0].property['identity']
            if caller_id == account_id:
                return "未接电话"
            return "{} 未接电话".format(root_node['partlist'].child[0]['name'].data)
        elif call_type == 'started':
            return "开始通话"
        elif call_type == 'ended':
            return "通话 {} 秒".format(root_node['partlist'].child[0]['duration'].data)
        return None

    @staticmethod
    def assemble_message_rename_group(root_node):
        changer = root_node['topicupdate']['initiator'].data
        new_name = root_node['topicupdate']['value'].data
        return "{} 已将此对话重命名为“{}”".format(changer, new_name)

    @staticmethod
    def assemble_message_history_closure(root):
        if root['historydisclosedupdate']['value'].data == "true":
            return "{} 已将聊天历史记录隐藏，对新参与者不可见".format(root['historydisclosedupdate']['initiator'].data)
        else:
            return "{} 已将历史聊天记录设为对所有人可见".format(root['historydisclosedupdate']['initiator'].data)

    @staticmethod
    def assemble_message_close_add_memeber(root_node):
        if root_node['joiningenabledupdate']['value'].data == "true":
            return "{} 已启用使用链接加入此对话。转到“组设置”获取邀请其他人加入的链接".format(root_node['joiningenabledupdate']['initiator'].data)
        else:
            return "{} 已禁用加入此对话".format(root_node['joiningenabledupdate']['initiator'].data)

    @staticmethod
    def assemble_message_change_pic_bak(root_node):
        return "{} 已更改对话图片".format(root_node['pictureupdate']['initiator'].data)

    @staticmethod
    def convert_message_content_type(_type):
        if _type == "RichText" or _type == "Text":
            return model_im.MESSAGE_CONTENT_TYPE_TEXT
        elif _type.startswith("ThreadActivity"):
            return model_im.MESSAGE_CONTENT_TYPE_SYSTEM
        elif _type == "RichText/Media_Video":
            return model_im.MESSAGE_CONTENT_TYPE_VIDEO
        elif _type == "RichText/Media_AudioMsg":
            return model_im.MESSAGE_CONTENT_TYPE_VOICE
        elif _type == "RichText/UriObject":
            return model_im.MESSAGE_CONTENT_TYPE_IMAGE
        elif _type == "Event/Call":
            return model_im.MESSAGE_CONTENT_TYPE_VOIP
        else:
            return None

    def convert_message_content(self, content, msg_obj):
        content = content.strip()
        if not (content.startswith("<") and content.endswith(">")):
            return content
        try:
            hp = PaHtmlParser()
            hp.feed(content)
            hp.close()

            tag_name = hp.first_dom.tag_name
            if tag_name == "deletemember":
                executor = hp.root['deletemember']['initiator'].data
                target = hp.root['deletemember']['target'].data
                return self.assemble_message_delete_member(executor, target)
            elif tag_name == "addmember":
                executor = hp.root['addmember']['initiator'].data
                target = hp.root['addmember']['target'].data
                return self.assemble_message_add_member(executor, target)
            elif tag_name == "ss":
                nodes = hp.root.get_all("ss")
                return self.assemble_message_emoji(nodes)
            elif tag_name == "uriobject":
                if hp.root['uriobject'].get("swift", None):
                    encoded_info = hp.root['uriobject']['swift'].property['b64']
                    decoded_info = TaoUtils.decode_base64(encoded_info)
                    attachment = json.loads(decoded_info)["attachments"][0]
                    url = attachment["content"]['images'][0]['url']
                    return url
                return hp.root['uriobject']['originalname'].property['v']
            elif tag_name == 'partlist':
                return self.assemble_message_call(hp.root, msg_obj.account_id)
            elif tag_name == 'topicupdate':
                return self.assemble_message_rename_group(hp.root)
            elif tag_name == 'location':
                address = hp.root['location'].property['address']
                latitude = float(hp.root['location'].property['latitude']) / 1000000
                longitude = float(hp.root['location'].property['longitude']) / 1000000
                ts = TimeHelper.convert_timestamp(hp.root['location'].property.get("timestamp", None))
                location = model_im.Location()
                location.account_id = self.using_account.account_id
                location.address = address
                location.latitude = latitude
                location.longitude = longitude
                location.timestamp = ts
                self.model_im_col.db_insert_table_location(location)
                msg_obj.location_id = location.location_id
                msg_obj.type = model_im.MESSAGE_CONTENT_TYPE_LOCATION
                return None
            elif tag_name == 'historydisclosedupdate':
                return self.assemble_message_history_closure(hp.root)
            elif tag_name == 'joiningenabledupdate':
                return self.assemble_message_close_add_memeber(hp.root)
            elif tag_name == "sms":
                return hp.root['sms']['defaults']['content'].data
            elif tag_name == "pictureupdate":
                return self.assemble_message_change_pic_bak(hp.root)
            else:
                return content
        except Exception as e:
            return content

    def _generate_account_table(self, node):

        account_id = os.path.basename(node.PathWithMountPoint)
        source = node.AbsolutePath

        account = model_im.Account()
        account.account_id = account.username = account.nickname = account.telephone = account_id
        account.source = source

        self.model_im_col.db_insert_table_account(account)
        self.model_im_col.db_commit()

        return account_id

    def _generate_friend_table(self, node, account_id):
        friend_id = node
        source = node.AbsolutePath

        friend = model_im.Friend()
        friend.source = source
        friend.account_id = account_id
        friend.nickname = friend.friend_id = friend_id
        friend.type = model_im.FRIEND_TYPE_FRIEND
        self.model_im_col.db_insert_table_friend(friend)
        self.model_im_col.db_commit()

    def _generate_message_table(self, node, account_id):
        source = node.AbsolutePath
        messages = self._open_json_file(node).get('messages', [])

        for m in messages:
            message = model_im.Message()
            message.source = source
            message.msg_id = m.get("clientmessageid", None)
            message.account_id = account_id
            message.talker_id = m.get("conversationid")
            message.sender_id = message.sender_name = sender_id = m.get('from').split('/')[-1]
            message.is_sender = 1
            content = self.convert_message_content(m.get("content", None), message)
            message.content = content
            send_time = TimeHelper.str_to_ts(m.get('originalarrivaltime', None)[:-2], _format="%Y-%m-%dT%H:%M:%S")
            message.send_time = send_time
            message.type = self.convert_message_content_type(m.get("messagetype", None))
            message.talker_type = model_im.CHAT_TYPE_GROUP if "@" in message.talker_id else model_im.CHAT_TYPE_FRIEND
            # TODO media path 无法添加，案例数据没有
            self.model_im_col.db_insert_table_message(message)
        self.model_im_col.db_commit()

    def _parse_account(self, node):

        if node is None:
            return

        account_id = self._generate_account_table(node)

        if account_id is None:
            return

        for f in node.Children:
            self._generate_friend_table(f, account_id)
            self._generate_message_table(f, account_id)

    def _main(self):
        for node in self.root.Children:
            self._parse_account(node)

    def _update_db_version(self):
        self.model_im_col.db_insert_table_version(model_im.VERSION_KEY_DB, model_im.VERSION_VALUE_DB)
        self.model_im_col.db_insert_table_version(model_im.VERSION_KEY_APP, self.app_version)
        self.model_im_col.db_commit()

    def generate_models(self):
        generate = model_im.GenerateModel(self.cache_db)
        results = generate.get_models()
        return results

    def parse(self):
        if self.debug or self.model_im_col.need_parse(self.cache_db, SKYPE_VERSION):
            self.model_im_col.db_create(self.cache_db)
            self._main()
            self._update_db_version()
            self.model_im_col.db_close()

        return self.generate_models()


def parse_yunkan_skype(root, extract_deleted, extract_source):
    pr = ParserResults()
    pr.Categories = DescripCategories.Skype
    results = YunkanSkypePanParser(root, extract_deleted, extract_source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("Skype")
    return pr