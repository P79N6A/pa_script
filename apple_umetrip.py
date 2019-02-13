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
from ScriptUtils import ParserBase, DataModel, Fields, TimeHelper, TaoUtils, ModelCol
import PA_runtime
import System
from PA_runtime import *
from System.Data.SQLite import *
from System.Xml.Linq import *
from System.Xml.XPath import Extensions as XPathExtensions
from PA.InfraLib.Extensions import PlistHelper

del clr

# CONST
Umetrip_VERSION = 1
DEBUG = True


def create_message_table(account_id):
    class Message(DataModel):
        __table__ = 'UME_ChatMsg' + str(account_id)

        msg_id = Fields.CharField(column_name='[messageId]')
        send_status = Fields.IntegerField(column_name='[sendStatusType]')
        sender_id = Fields.CharField(column_name='[fromUserId]')
        ts = Fields.CharField(column_name='[timeInterval]')
        content = Fields.CharField(column_name='[messageStr]')
        msg_type = Fields.IntegerField(column_name='[messageType]')

    return Message


def create_chat_member_table(account_id):
    class Friend(DataModel):
        __table__ = 'GroupMemberModel' + str(account_id)

        photo = Fields.CharField(column_name='[headImgUrl]')
        nickname = Fields.CharField(column_name='[nickName]')
        user_id = Fields.IntegerField(column_name='[userId]')

    return Friend


class FlightTicket(DataModel):
    __table__ = 'UME_MYJOURNEY'

    ticket_id = Fields.IntegerField(column_name='tktNo')
    is_future = Fields.IntegerField(column_name='isFuture')
    flight_number = Fields.CharField(column_name='flightNo')
    departure_code = Fields.CharField(column_name='deptCityCode')
    destination_code = Fields.CharField(column_name='destCityCode')
    departure_name = Fields.CharField(column_name='deptCityName')
    destination_name = Fields.CharField(column_name='destCityName')
    departure_time = Fields.CharField(column_name='std')
    destination_time = Fields.CharField(column_name='sta')
    dept_flight_date = Fields.CharField(column_name='deptFlightDate')
    dest_flight_date = Fields.CharField(column_name='destFlightDate')
    flight_distance = Fields.CharField(column_name='flyKilo')
    status = Fields.CharField(column_name='tktStatus')


class UmetripParser(ParserBase):

    def __init__(self, root, extract_deleted, extract_source):
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

    def _search_account_file(self):
        file_ = self.root.Search("userinfo.dat$")
        return next(iter(file_), None)

    def _search_chat_db(self):
        file_ = self.root.Search("Chat.sqlite$")
        return next(iter(file_), None)

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

        account_file = self._search_account_file()
        if account_file is None:
            return
        file_data = PlistHelper.ReadPlist(account_file)

        account = model_im.Account()
        account.account_id = file_data.Get('uid')
        account.username = file_data.Get('userName')
        account.country = file_data.Get('country')
        account.gender = file_data.Get('gender')
        account.photo = file_data.Get('headUrl')
        account.signature = file_data.Get('homePageEditDesc')
        account.telephone = file_data.Get('mobile')
        account.nickname = file_data.Get('nickName')

        self.master_account = account
        self.history_account_list.append(account.account_id)
        account.insert_db(self.model_im_col)
        self.model_im_col.db_commit()

    def _generate_history_account(self):
        db = self._search_file("Chat.sqlite$")
        if not db:
            return
        db_col = ModelCol(db)

        with db_col:
            db_col.execute_sql(
                "SELECT name FROM sqlite_master"
            )
            while db_col.has_rest():
                try:
                    name = db_col.get_string(0)
                    if not name.startswith('UME_ChatMsg'):
                        continue
                    account_id = name.replace('UME_ChatMsg', "")
                    if account_id == str(self.master_account.account_id):
                        continue

                    self.history_account_list.append(account_id)

                    account = model_im.Account()
                    account.account_id = account.username = account.nickname = account_id

                    account.insert_db(self.model_im_col)
                except Exception as e:
                    self.logger.error()
            self.model_im_col.db_commit()

    def _generate_friend_table(self):
        chat_db = self._search_file("Chat.sqlite$")
        if not chat_db:
            return

        for account_id in self.history_account_list:
            member_model = create_chat_member_table(account_id)
            member_model.connect(chat_db)

            for member in member_model.objects.all:
                try:
                    friend = model_im.Friend()
                    friend.account_id = account_id
                    friend.friend_id = member.user_id
                    friend.fullname = friend.nickname = member.nickname
                    friend.photo = member.photo
                    self.model_im_col.db_insert_table_friend(friend)
                except Exception as e:
                    self.logger.error()
        self.model_im_col.db_commit()

    def _generate_message_table(self):
        chat_db = self._search_file("Chat.sqlite$")
        if not chat_db:
            return

        for account_id in self.history_account_list:
            msg_model = create_message_table(account_id)
            member_model = create_chat_member_table(account_id)

            member_model.connect(chat_db)
            name = {member.user_id: member.nickname for member in member_model.objects.all}

            msg_model.connect(chat_db)

            for msg in msg_model.objects.all:
                try:
                    message = model_im.Message()
                    message.account_id = account_id
                    message.deleted = msg.deleted
                    message.talker_id = message.sender_id = msg.sender_id
                    message.content = msg.content
                    message.msg_id = msg.msg_id
                    message.send_time = TaoUtils.convert_timestamp(msg.ts)
                    message.is_sender = 1 if account_id == msg.sender_id else 0
                    message.sender_name = name.get(int(message.sender_id), None)
                    message.talker_type = model_im.CHAT_TYPE_FRIEND
                    message.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE if msg.msg_type == 1 \
                        else model_im.MESSAGE_CONTENT_TYPE_TEXT
                    message.media_path = self._search_file('{}$'.format(message.content))
                    self.model_im_col.db_insert_table_message(message)
                except Exception as e:
                    self.logger.error()
            self.model_im_col.db_commit()

    def _generate_search_table(self):
        search_log_by_code = self._search_file("searchFlightByCode.txt$")
        if not search_log_by_code:
            return
        file_data = PlistHelper.ReadPlist(search_log_by_code)
        for d in file_data:
            try:
                search = model_im.Search()
                key = d['flightNo']
                search.key = key
                search.account_id = self.master_account.account_id
                self.model_im_col.db_insert_table_search(search)
            except Exception as e:
                self.logger.error()
        self.model_im_col.db_commit()

    def _generate_deal_table(self):
        deal_db = self._search_file('MyJourneyDB.sqlite$')
        if not deal_db:
            return
        FlightTicket.connect(deal_db)
        account_id = self.master_account.account_id

        for ticket in FlightTicket.objects.all:
            try:
                deal = model_eb.EBDeal()
                deal.set_value_with_idx(deal.account_id, account_id)
                deal.set_value_with_idx(deal.deleted, ticket.deleted)
                deal.set_value_with_idx(deal.source_file, ticket.source_path)
                deal.set_value_with_idx(deal.begin_time,
                                        TimeHelper.str_to_ts("{} {}".format(ticket.dept_flight_date, ticket.departure_time),
                                                             "%Y-%m-%d %H:%M"))
                deal.set_value_with_idx(deal.end_time,
                                        TimeHelper.str_to_ts("{} {}".format(ticket.dest_flight_date, ticket.destination_time),
                                                             "%Y-%m-%d %H:%M"))
                deal.set_value_with_idx(deal.content, self._process_ticket_content(
                    ticket.departure_name,
                    ticket.destination_name,
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
        if self.master_account is None:
            print('没有登陆账号')
            return
        self._generate_history_account()
        self._generate_friend_table()
        self._generate_message_table()
        self._generate_search_table()
        self._generate_deal_table()

    def parse(self):
        """程序入口"""
        if self.debug or self.model_eb_col.need_parse:
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
        pr.Build("Taobao")
    return pr
