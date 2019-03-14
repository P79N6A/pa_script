# coding=utf-8

__author__ = 'YangLiyuan'

import clr

clr.AddReference('PNFA.Common')

from PA_runtime import *

try:
    clr.AddReference('bcp_gis')
    clr.AddReference('ScriptUtils')
    clr.AddReference('model_ticketing')
    clr.AddReference('model_map')
except:
    pass

VERSION_APP_VALUE = 1

import bcp_gis
import model_map
import model_ticketing
from ScriptUtils import tp, exc, base_analyze, parse_decorator, BaseParser

GENDER_TYPE = {
    'M': 1,
    'F': 2,
}


@parse_decorator
def analyze_cloud_12306(node, extract_deleted, extract_source):
    # node = fake_node()
    return base_analyze(Cloud12306Parser,
                        node,
                        bcp_gis.NETWORK_APP_TICKET_12306,
                        VERSION_APP_VALUE,
                        build_name='铁路12306(云勘)',
                        db_name='12306_c')


class Cloud12306Parser(BaseParser):
    def __init__(self, node, db_name):
        super(Cloud12306Parser, self).__init__(node, db_name)
        self.VERSION_VALUE_DB = model_ticketing.VERSION_VALUE_DB
        self.Generate = model_ticketing.GenerateModel
        self.csm = model_ticketing.Ticketing()
        if (node.Children.Count == 1
                and node.Children[0].Name.isalnum()):
            self.root = node.Children[0]
        else:
            self.root = node

        self.account_dict = {}
        self.cur_account_id = 'default_account'

    def parse(self, BCP_TYPE, VERSION_APP_VALUE):

        models = super(Cloud12306Parser, self).parse(BCP_TYPE, VERSION_APP_VALUE)
        map_models = model_map.Genetate(self.cache_db).get_models()
        models.extend(map_models)
        return models

    def parse_main(self):
        # ('userInfo.json
        # ('friends.json                self.parse_friend
        # ('HistoryOrder.json           self.parse_journey
        # ('InsuranceOrder.json
        # ('MyTravel.json
        # ('PaidQueueOrder',
        # ('ProcessedOrder.json
        # ('UnfinishedOrder.json
        # ('UnMoveOrder.json
        # ('UnpayQueueOrder.json                
        _nodes = self.root.Children
        _json_name_list = []

        self.cur_account_id = self.parse_account('userInfo.json')
        self.parse_friend('friends.json')
        self.parse_journey('HistoryOrder.json')

    def parse_account(self, json_path):
        ''' userInfo
        {
            "validateMessagesShowId": "_validatorMessage",
            "status": true,
            "httpstatus": 200,
            "data": {
                "userTypeName": "成人",
                "listEntrySchoolYeasrs": [],
                "country_name": "中国CHINA",
                "listSchoolSystem": [],
                "userDTO": {
                    "loginUserDTO": {
                        "login_id": "E",
                        "agent_contact": "18256078414",
                        "user_type": "1",
                        "user_name": "jt18256078414",
                        "name": "姜停",
                        "id_type_code": "1",
                        "id_type_name": "中国居民身份证",
                        "id_no": "341125199210212013",
                        "member_id": "022205074062",
                        "member_level": "2",
                        "userIpAddress": "116.192.174.3",
                    },
                    "studentInfoDTO": {},
                    "sex_code": "M",
                    "born_date": "1992-10-21 00:00:00",
                    "country_code": "CN",
                    "mobile_no": "18256078414",
                    "phone_no": "",
                    "email": "",
                    "address": "",
                    "postalcode": "",
                    "is_active": "N",
                    "revSm_code": "Y",
                    "last_login_time": "",
                    "user_id": 10000044606660,
                    "phone_flag": "*",
                    "encourage_flag": "*",
                    "user_status": "1",
                    "check_id_flag": "0",
                    "is_valid": "Y",
                    "display_control_flag": "1",
                    "needModifyEmail": "Y",
                    "flag_member": "Y",
                    "pic_control_flag": "",
                    "regist_time": ""
                },
                "listCardType": [],
                "listPassengerTypes": [],
                "bornDateString": "1992-10-21",
                "listCountry": [],
                "listProvince": [],
                "notice": "已通过"
            },
            "messages": [],
            "validateMessages": {}
        }
        '''
        if self._12306_read_json(json_node=self.root.GetByPath(json_path)):
            user_info = self.cur_json_dict.get('userDTO', {})
            try:
                account = model_ticketing.Account()
                _bir = self._convert_strtime_2_ts(user_info.get('born_date'))
                if _bir:
                    account.birthday = _bir
                account.telephone = user_info.get('mobile_no')

                loginUserDTO = user_info.get('loginUserDTO', [])
                account.city = loginUserDTO.get('addressee_city')
                account.account_id = loginUserDTO.get('user_name')
                account.username = loginUserDTO.get('name')
                account.username = loginUserDTO.get('name')
                account.gender = GENDER_TYPE.get(loginUserDTO.get('sex_code'), 0)  # 性别[INT]
                # 身份证 = user_info.get('id_no')
                self.account_dict[account.account_id] = account
                return account.account_id
            except:
                exc()

    def parse_friend(self, json_path):
        '''
            "data": {
                "isNeedAgree": false,
                "can_operate_passenger_days_after": 30,
                "isCanAddAddress": true,
                "address_max_size": 20,
                "addresses": [
                    {
                        "addressee_city": "重庆市",
                        "addressee_county": "沙坪坝区",
                        "addressee_name": "姜停",
                        "addressee_province": "重庆市",
                        "addressee_street": "",
                        "addressee_town": "天星桥街道",
                        "default_address": "1",
                        "deliver_company": "",
                        "deliver_mode": "",
                        "detail_address": "上海市普陀区天地软件园",
                        "invoice_flag": "",
                        "mobile_no": "18256078414",
                        "pay_type": "",
                        "phone_no": "",
                        "receive_date_flag": "",
                        "receive_time": "",
                        "region_code": "",
                        "reserver_grade": "",
                        "service_type": "",
                        "transmit_flag": "",
                        "original_address_name": "姜停",
                        "original_address_province": "重庆市",
                        "original_address_city": "重庆市",
                        "original_address_county": "沙坪坝区",
                        "original_detail_address": "上海市普陀区天地软件园",
                        "original_mobile_no": "18256078414",
                        "original_address_town": "天星桥街道",
                        "original_default_address": "1",
                        "original_address_street": "",
                        "user_name": "jt18256078414",
                        "canEditFlag": "Y"
                    }
                ]
            },
        '''
        if self._12306_read_json(json_node=self.root.GetByPath(json_path)):
            for user_info in self.cur_json_dict.get('addresses', []):
                try:
                    account = self.account_dict.get(user_info.get('user_name'))
                    if not account:
                        continue
                    account.account_id = user_info.get('user_name')
                    account.username = user_info.get('user_name')
                    account.nickname = user_info.get('addressee_name')
                    account.telephone = user_info.get('mobile_no')
                    account.city = user_info.get('addressee_city')
                    account.scource = self.cur_json_source
                    self.csm.db_insert_table_account(account)
                except:
                    exc()
            self.csm.db_commit()

    def parse_journey(self, json_path):
        '''  HistoryOrder.json
        '''
        if self._12306_read_json(json_node=self.root.GetByPath(json_path)):
            for _order in self.cur_json_dict.get('OrderDTODataList', []):
                try:
                    journey = model_map.LocationJourney()
                    journey.account_id = self.cur_account_id
                    journey.order_num = _order.get('sequence_no')
                    journey.order_time = self._convert_strtime_2_ts(_order.get('order_date'))
                    journey.start_time = self._convert_strtime_2_ts(_order.get('start_train_date_page'))
                    journey.depart = _order.get('trip_begin_place')
                    journey.depart_address = _order.get('from_station_name_page')[0]
                    journey.end_time = _order.get('end_time')
                    journey.destination = _order.get('trip_end_place')
                    journey.destination_address = _order.get('to_station_name_page')[0]
                    # journey.destination_above_sealevel =
                    journey.flightid = _order.get('train_code_page')
                    journey.purchase_price = _order.get('ticket_total_price_page')
                    journey.aircom = _order.get('trip_company')
                    journey.order_num = _order.get('sequence_no')
                    # journey.ticket_status    = _order.get('mFlightStatusStr')
                    # journey.order_time       = _order.get('mGMTRealArrivalTime')
                    # journey.latest_mod_time  = _order.get('mUpdateTime')
                    journey.source = self.cur_json_source
                    self.csm.db_insert_table_journey(journey)
                except:
                    exc()
            self.csm.db_commit()

    def _12306_read_json(self, json_node):
        try:
            _json_data = self._read_json(json_node=json_node)
            if (not _json_data
                    or not _json_data.get('status')
                    or _json_data.get('httpstatus') != 200):
                return False
            else:
                self.cur_json_dict = _json_data.get('data', {})
                return True
        except:
            exc()
            return False
