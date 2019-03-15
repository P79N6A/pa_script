# coding=utf-8

__author__ = 'TaoJianping'

import clr

try:
    clr.AddReference('ScriptUtils')
    clr.AddReference('model_ticketing')
    clr.AddReference('model_map')
    clr.AddReference('model_im')
except:
    pass

clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')

del clr
import os
import PA_runtime
import model_ticketing
import model_im
import model_map
import json
import System
import hashlib
from PA_runtime import *
from System.Data.SQLite import *
from System.Xml.Linq import *
from System.Xml.XPath import Extensions as XPathExtensions
from ScriptUtils import TimeHelper, YunkanParserBase

# const
DEBUG = False
UMETRIPVERSION = 1


TICKET_STATUS_UNKNOWN = "0"
TICKET_STATUS_USED = "1"
TICKET_STATUS_UNUSE = "2"
TICKET_STATUS_REFUND = "3"
TICKET_STATUS_OTHER = "9"


class YunkanUmetripParser(YunkanParserBase):
    """
    云勘数据 备份解析 -> 纵横航旅
        1. 账户
        2. 订单
        还有很多资料缺失，无法解析
    """

    def __init__(self, node, extract_deleted, extract_source, app_name='YunkanUmetrip'):
        super(YunkanUmetripParser, self).__init__(node, extract_deleted, extract_source, app_name)
        self.app_version = UMETRIPVERSION
        self.csm = model_ticketing.Ticketing()
        self.debug = DEBUG

    def _generate_account_table(self, db):
        account_id = os.path.basename(db.PathWithMountPoint)

        user_info_1 = db.GetByPath('/userinfo1.json')
        user_info_2 = db.GetByPath('/userinfo2.json')

        photo = None
        name = None
        sign = None

        if user_info_1 is not None:
            user_info = self._open_json_file(user_info_1)
            photo = user_info['presp'].get('pdata', {}).get('headUrl', None)

        if user_info_2 is not None:
            user_info = self._open_json_file(user_info_2)
            name = user_info['presp'].get('pdata', {}).get('chnName', None)
            sign = user_info['presp'].get('pdata', {}).get('homePageEditDesc', None)

        account = model_im.Account()
        account.account_id = account.username = account.nickname = account.telephone = account_id
        account.username = name
        account.signature = sign
        account.photo = photo

        self.csm.db_insert_table_account(account)
        self.csm.db_commit()
        return account_id

    def _generate_ticket_table(self, json_file, account_id):

        if json_file is None:
            return

        tickets = self._open_json_file(json_file).get('presp', {}).get('pdata', {}).get('activityList', [])
        for ticket_info in tickets:
            try:
                activity_info = ticket_info.get('activityInfo', {})
                ticket = model_map.LocationJourney()
                ticket.account_id = account_id
                ticket.flightid = activity_info.get('flightNo', None)
                start_time = '{} {}'.format(activity_info.get('deptDateTz', ''), activity_info.get('deptTimeTz', ''))
                ticket.start_time = TimeHelper.str_to_ts(start_time, _format='%Y-%m-%d %H:%M')
                ticket.depart = activity_info.get('deptCityCode', None)
                ticket.depart_address = activity_info.get('deptCityName', None)
                end_time = '{} {}'.format(activity_info.get('destDateTz', ''), activity_info.get('destTimeTz', ''))
                ticket.end_time = TimeHelper.str_to_ts(end_time, _format='%Y-%m-%d %H:%M')
                ticket.destination = activity_info.get('destCityCode', None)
                ticket.destination_address = activity_info.get('destCityName', None)
                ticket.purchase_price = activity_info.get('priceJointWithUnit', None)
                ticket.ticket_status = activity_info.get('tktStatusDesc', None)
                # Nov 16, 2017 8:44:03 PM
                ticket.order_time = TimeHelper.str_to_ts(activity_info.get('createTime', None),
                                                         _format='%b %d, %Y %H:%M:%S %p')
                ticket.latest_mod_time = TimeHelper.str_to_ts(activity_info.get('modifyTime', None),
                                                         _format='%b %d, %Y %H:%M:%S %p')
                self.csm.db_insert_table_journey(ticket)
            except Exception as e:
                print e
        self.csm.db_commit()



    def _generate_message_table(self, db, account_id):
        pass

    def _main(self):
        for node in self.root.Children:
            account_id = self._generate_account_table(node)

            if not account_id:
                return

            msg_db = node.GetByPath('/msg.json')
            if msg_db is not None:
                self._generate_message_table(msg_db, account_id)

            travel_info_db = node.GetByPath('/userTravelInfo.json')
            if travel_info_db is not None:
                self._generate_ticket_table(travel_info_db, account_id)

    def parse(self):
        if DEBUG or self.csm.need_parse(self.cache_db, self.app_version):
            self.csm.db_create(self.cache_db)
            self._main()
            if not canceller.IsCancellationRequested:
                self.csm.db_insert_table_version(model_ticketing.VERSION_KEY_DB, model_ticketing.VERSION_VALUE_DB)
                self.csm.db_insert_table_version(model_ticketing.VERSION_KEY_APP, self.app_version)
                self.csm.db_commit()
            self.csm.db_close()
        models = model_im.GenerateModel(self.cache_db).get_models() + model_map.Genetate(self.cache_db).get_models()
        return models


def parse_yunkan_umetrip(root, extract_deleted, extract_source):
    pr = ParserResults()
    pr.Categories = DescripCategories.Umetrip
    results = YunkanUmetripParser(root, extract_deleted, extract_source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("Umetrip")
    return pr
