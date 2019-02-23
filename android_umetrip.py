# -*- coding: utf-8 -*-

import clr

__author__ = "TaoJianping"

clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')

try:
    clr.AddReference('unity_c37r')
    clr.AddReference('ScriptUtils')
    clr.AddReference('model_eb')
    clr.AddReference('model_im')
except Exception as e:
    print("debug", e)

import model_eb
import model_im
from ScriptUtils import ParserBase, DataModel, Fields, TimeHelper, TaoUtils
import PA_runtime
import System
from PA_runtime import *
from System.Data.SQLite import *
from System.Xml.Linq import *
from System.Xml.XPath import Extensions as XPathExtensions
from PA.InfraLib.Extensions import PlistHelper
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

del clr

# CONST
Umetrip_VERSION = 1
DEBUG = True


class Message(DataModel):
    __table__ = 'MESSAGE_ENTITY'

    msg_id = Fields.CharField(column_name='MESSAGE_ID')
    send_status = Fields.IntegerField(column_name='SEND_STATUS_TYPE')
    sender_id = Fields.CharField(column_name='FROM_USER_ID')
    content = Fields.CharField(column_name='MESSAGE_STR')
    ts = Fields.IntegerField(column_name='TIMESTAMP')
    msg_type = Fields.IntegerField(column_name='MESSAGE_TYPE')
    talker_id = Fields.CharField(column_name='SESSION_ID')
    media_path = Fields.CharField(column_name='MEDIA_FILE_PATH')


class Friend(DataModel):
    __table__ = 'USER_ENTITY'

    photo = Fields.CharField(column_name='AVATAR')
    nickname = Fields.CharField(column_name='NICKNAME')
    user_id = Fields.IntegerField(column_name='USER_ID')


class FlightTicket(DataModel):
    __table__ = 'UME_ACTIVITY'

    user_id = Fields.CharField(column_name='[uid]')
    ticket_id = Fields.IntegerField(column_name='[pid]')
    departure_code = Fields.CharField(column_name='[startPlace]')
    destination_code = Fields.CharField(column_name='[endPlace]')
    departure_time = Fields.CharField(column_name='[depttimetz]')
    destination_time = Fields.CharField(column_name='[desttimetz]')
    dept_flight_date = Fields.CharField(column_name='[deptdatetz]')
    dest_flight_date = Fields.CharField(column_name='[destdatetz]')
    status = Fields.IntegerField(column_name='[ptktstatus]')
    company = Fields.CharField(column_name='[airline]')


class UmetripParser(ParserBase):

    def __init__(self, root, extract_deleted, extract_source):
        print(extract_deleted)
        super(UmetripParser, self).__init__(
            self._get_root_node(root, times=2),
            extract_deleted,
            extract_source,
            app_name="Umetrip",
            app_version=Umetrip_VERSION,
            debug=DEBUG,
        )

        self.model_eb_col, self.model_im_col = self.load_eb_models(self.cache_db, self.app_version, self.app_name)
        self.master_account = None
        self.history_account_list = []

    @staticmethod
    def _process_ticket_content(dept, dest, dept_time, dest_time):
        return "从{}飞往{}，飞行时间{} - {}".format(dept, dest, dept_time, dest_time)

    @staticmethod
    def _process_ticket_status(status):
        if status == 'OPEN FOR USER':
            return model_eb.TRADE_STATUS_PROCESSING
        elif status == 'USED/FLOWN':
            return model_eb.TRADE_STATUS_FINISHED
        else:
            return model_eb.TRADE_STATUS_CLOSE

    def _generate_account_table(self):

        account_file = self._search_file_simple("mqc_private.xml$")
        if account_file is None:
            return
        info = XElement.Parse(account_file.read())
        es = info.Elements("string")
        ns = info.Elements("boolean")
        self.master_account = account = model_im.Account()
        for rec in ns:
            if rec.Attribute("name") and rec.Attribute("name").Value.startswith("hasEnterTripList"):
                account.account_id = rec.Attribute("name").Value.replace('hasEnterTripList', '')
        for rec in es:
            if rec.Attribute("name") and rec.Attribute("name").Value == "login_phone":
                account.telephone = rec.FirstNode.Value
            elif rec.Attribute("name") and rec.Attribute("name").Value == "HEAD_PHOTO_URL":
                account.photo = rec.FirstNode.Value
            elif rec.Attribute("name") and rec.Attribute("name").Value == "lOGIN_USER_NAME":
                account.nickname = account.username = rec.FirstNode.Value
            elif rec.Attribute("name") and rec.Attribute("name").Value == "flight_number":
                search = rec.FirstNode.Value
                self._generate_search_table(search.split(","))

        account.insert_db(self.model_im_col)
        self.model_im_col.db_commit()

    def _generate_friend_table(self):
        chat_db = self._search_file_simple("chat-db$")
        if not chat_db:
            return
        Friend.connect(chat_db)
        for member in Friend.objects.all:
            try:
                friend = model_im.Friend()
                friend.account_id = self.master_account.account_id
                friend.friend_id = member.user_id
                if friend.friend_id != "":
                    friend.type = model_im.FRIEND_TYPE_SHOP
                friend.fullname = friend.nickname = member.nickname
                friend.photo = member.photo
                friend.deleted = member.deleted
                friend.source = member.source_path
                self.model_im_col.db_insert_table_friend(friend)
            except Exception as e:
                self.logger.error()
        self.model_im_col.db_commit()

    def _generate_message_table(self):
        chat_db = self._search_file_simple("chat-db$")
        if not chat_db:
            return
        Message.connect(chat_db)
        Friend.connect(chat_db)
        name = {member.user_id: member.nickname for member in Friend.objects.all}

        account_id = self.master_account.account_id
        for msg in Message.objects.all:
            try:
                message = model_im.Message()
                message.deleted = msg.deleted
                message.source = msg.source_path
                message.talker_id = msg.talker_id
                message.account_id = message.talker_id.split('##')[-1]
                message.content = msg.content
                message.msg_id = msg.msg_id
                message.sender_id = msg.sender_id
                message.send_time = TaoUtils.convert_timestamp(msg.ts)
                message.is_sender = 1 if int(account_id) == int(msg.sender_id) else 0
                message.sender_name = name.get(int(message.sender_id), None)
                message.talker_type = model_im.CHAT_TYPE_SHOP
                message.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE if msg.msg_type == 1 \
                    else model_im.MESSAGE_CONTENT_TYPE_TEXT
                message.media_path = msg.media_path
                self.model_im_col.db_insert_table_message(message)
            except Exception as e:
                self.logger.error()
        self.model_im_col.db_commit()

    def _generate_search_table(self, search_list):
        for key in search_list:
            try:
                search = model_im.Search()
                search.key = key
                search.account_id = self.master_account.account_id
                self.model_im_col.db_insert_table_search(search)
            except Exception as e:
                self.logger.error()
        self.model_im_col.db_commit()

    def _generate_deal_table(self):
        deal_db = self._search_file_simple('ume1.db$')
        if not deal_db:
            return
        FlightTicket.connect(deal_db)

        for ticket in FlightTicket.objects.all:
            try:
                deal = model_eb.EBDeal()
                deal.set_value_with_idx(deal.account_id, ticket.user_id)
                deal.set_value_with_idx(deal.deleted, ticket.deleted)
                deal.set_value_with_idx(deal.source_file, ticket.source_path)
                deal.set_value_with_idx(deal.begin_time,
                                        TimeHelper.str_to_ts("{} {}".format(ticket.dept_flight_date, ticket.departure_time),
                                                             "%Y-%m-%d %H:%M"))
                deal.set_value_with_idx(deal.end_time,
                                        TimeHelper.str_to_ts("{} {}".format(ticket.dest_flight_date, ticket.destination_time),
                                                             "%Y-%m-%d %H:%M"))
                deal.set_value_with_idx(deal.content, self._process_ticket_content(
                    ticket.departure_code,
                    ticket.destination_code,
                    "{} {}".format(ticket.dept_flight_date, ticket.departure_time),
                    "{} {}".format(ticket.dest_flight_date, ticket.destination_time),
                ))
                deal.set_value_with_idx(deal.status, self._process_ticket_status(ticket.status))
                deal.set_value_with_idx(deal.deal_type, model_eb.EBDEAL_TYPE_REC)
                deal.set_value_with_idx(deal.target, ticket.ticket_id)
                self.model_eb_col.db_insert_table_deal(deal.get_value())
            except Exception as e:
                self.logger.error()
        self.model_eb_col.db_commit()

    def _main(self):
        self._generate_account_table()
        self._generate_friend_table()
        self._generate_message_table()
        self._generate_deal_table()

    def parse(self):
        """程序入口"""
        if self.debug or self.model_eb_col.need_parse:
            if os.path.exists(self.cache_db):
                os.remove(self.cache_db)
            if os.path.exists(self.cache_db + '.im'):
                os.remove(self.cache_db + '.im')
            self.model_eb_col.db_create()
            self._main()
            self._update_eb_script_version(self.model_eb_col, self.app_version)
            self.model_eb_col.db_close()

        return model_eb.GenerateModel(self.cache_db).get_models()


def analyze_Umetrip(root, extract_deleted, extract_source):
    pr = ParserResults()
    pr.Categories = DescripCategories.Umetrip
    results = UmetripParser(root, extract_deleted, extract_source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("Umetrip")
    return pr
