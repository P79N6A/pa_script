# coding=utf-8
__author__ = 'YangLiyuan'

import json

import PA_runtime
from PA_runtime import *
import clr
try:
    clr.AddReference('model_ticketing')
    clr.AddReference('bcp_gis')
    clr.AddReference('ScriptUtils')
    clr.AddReference("model_map")
    clr.AddReference("model_ticketing")
except:
    pass
del clr

import bcp_gis
import model_map
import model_ticketing 
from ScriptUtils import DEBUG, CASE_NAME, exc, tp, base_analyze, parse_decorator, BaseParser


VERSION_APP_VALUE = 1

TRIP_TYPE_TRAIN  = 'train'
TRIP_TYPE_HOTEL  = 'hotel'
TRIP_TYPE_TRAIN  = 'train'
TRIP_TYPE_FLIGHT = 'flight'
TRIP_TYPE_SHIP   = 'ship'     # TODO 轮船 猜的


@parse_decorator
def analyze_smartcare(node, extract_deleted, extract_source):
    return base_analyze(HuaweiSmartcare, 
                        node, 
                        bcp_gis.NETWORK_APP_TICKET_OTHER, 
                        VERSION_APP_VALUE,
                        '华为情景智能',
                        'HUAWEISmartCare_A')


class HuaweiSmartcare(BaseParser):
    def __init__(self, node, db_name):
        super(HuaweiSmartcare, self).__init__(node, db_name)
        self.VERSION_VALUE_DB = model_ticketing.VERSION_VALUE_DB
        self.root = node
        self.csm = model_ticketing.Ticketing()
        self.Generate = model_map.Genetate
        self.accounts = {}

    def parse_main(self):
        ''' smartcare.db

            comhuaweiproviderintelligentdirect_service_tb
            comhuaweiproviderintelligentfact_data_tb
            # 旅途记录
            comhuaweiproviderintelligentintelligent_tb
            comhuaweiproviderintelligent_copy_file_list_info 
            # hiboard_backupfile 附件 
            comhuaweiproviderintelligent_hiboard_backupfile_database_attachment
            comhuaweiproviderintelligent_hiboard_backupfile_shm_database_attachment
            comhuaweiproviderintelligent_hiboard_backupfile_wal_database_attachment

            comhuaweiproviderintelligent_intelligent_backupfile_sharepref_attachment
        '''
        # tp(self.root.AbsolutePath)
        if self._read_db(node=self.root):
            self._parse_intelligentfact_data_tb('comhuaweiproviderintelligentintelligent_tb')
            self._parse_hiboard_backupfile_database_attachment('comhuaweiproviderintelligent_hiboard_backupfile_database_attachment')

    def _parse_hiboard_backupfile_database_attachment(self, table_name):
        pass

    def _parse_intelligentfact_data_tb(self, table_name):
        ''' comhuaweiproviderintelligentintelligent_tb

                trip_begin_place	             TEXT       出发城市
                trip_order_number	             TEXT       火车订机票号码
                hotel_address	                 TEXT       酒店地址
                type	                         TEXT       TRIP_TYPE
                creditcard_repayment_amount_cny	 TEXT
                hotel_event_state	             TEXT       酒店状态, 猜测 1 是已成功预订
                duplicate_code	                 TEXT
                trip_seat	                     TEXT       座位号
                hotel_guest_name	             TEXT
                trip_end_terminal	             TEXT       到达机场的T1 T2航站楼或其他
                trip_end_time	                 TEXT       到达时间
                trip_end_place	                 TEXT       到达城市
                creditcard_repayment_amount_usd	 TEXT       
                creditcard_state	             TEXT
                hotel_tel	                     TEXT       酒店电话
                ume_flight_info	                 TEXT       航班信息
                creditcard_bank_info	         TEXT
                trip_event_number	             TEXT       航班, 车次
                trip_begin_place_address	     TEXT       始发地址车站机场
                hotel_name	                     TEXT       酒店名称
                conference_end_time	             TEXT
                trip_begin_time	                 TEXT       始发时间 13
                conference_topic	             TEXT
                hotel_check_in_time	             TEXT       酒店入住时间
                trip_delay_time	                 TEXT       
                _id	                             TEXT
                conference_address	             TEXT
                trip_check_in_time	             TEXT
                birthday_time	                 TEXT
                creditcard_lowest_repayment_cny	 TEXT
                conference_state	             TEXT
                trip_event_state	             TEXT
                conference_sponsor	             TEXT
                conference_begin_time	         TEXT
                trip_end_place_address	         TEXT       到达地址车站机场
                creditcard_card_number	         TEXT
                trip_begin_terminal	             TEXT       到达机场的T1 T2航站楼或其他
                deleted_flag	                 TEXT
                hotel_sub_name	                 TEXT
                end_time	                     TEXT       结束时间
                begin_time	                     TEXT       开始时间
                main_id	                         TEXT
                creditcard_lowest_repayment_usd	 TEXT
                birthday_format	                 TEXT
                xy_train_station_list	         TEXT
                data_info	                     TEXT
                creditcard_expiration_date	     TEXT
                trip_passenger_name	             TEXT       乘客名称, '、' 分隔
                trip_company	                 TEXT       航空公司
                birthday_contact_id	             TEXT
        '''
        for rec in self._read_table(table_name):
            try:
                if (self._is_empty(rec, '_id') 
                    or rec['type'].Value not in ['train', 'flight']):
                    continue
                journey = model_map.LocationJourney()
                # journey.account_id = 
                journey.start_time    = rec['begin_time'].Value
                journey.depart        = rec['trip_begin_place'].Value
                journey.depart_address= rec['trip_begin_place_address'].Value
                journey.end_time      = rec['end_time'].Value
                journey.destination   = rec['trip_end_place'].Value
                journey.destination_address = rec['trip_end_place_address'].Value
                # journey.destination_above_sealevel = 
                journey.flightid         = rec['trip_event_number'].Value
                # journey.purchase_price   = rec['mGMTRealArrivalTime'].Value
                journey.aircom           = rec['trip_company'].Value
                journey.order_num        = rec['trip_order_number'].Value
                # journey.ticket_status    = rec['mFlightStatusStr'].Value
                # journey.order_time       = rec['mGMTRealArrivalTime'].Value
                # journey.latest_mod_time  = rec['mUpdateTime'].Value
                journey.source = self.cur_db_source
                journey = self._parse_ume_flight_info(journey, rec['ume_flight_info'].Value)
                self.csm.db_insert_table_journey(journey)
                # journey = self._parse_xy_train_station_list(journey, rec['xy_train_station_list'].Value)
            except:
                exc()
        self.csm.db_commit()
            
    def _parse_xy_train_station_list(self, journey, _data):
        ''' 
            0#上海虹桥##09:05#当天@
            1#松江南#09:19#09:24#当天@
            2#嘉兴南#09:42#09:46#当天@
            3#余杭#10:05#10:07#当天@
            4#杭州东#10:18#10:20#当天@
            5#绍兴北#10:39#10:41#当天@
            6#余姚北#11:00#11:02#当天@
            7#宁波#11:23#11:31#当天@
            8#三门县#12:10#12:12#当天@
            9#台州#12:31#12:33#当天@
            10#温岭#12:44#12:46#当天@
            11#绅坊#13:04#13:06#当天@
            12#温州南#13:27#13:29#当天@
            13#鳌江#13:46#13:48#当天@
            14#福鼎#14:10#14:12#当天@
            15#宁德#14:49#14:52#当天@
            16#福州南#15:30#15:35#当天@
            17#莆田#16:05#16:07#当天@
            18#泉州#16:32#16:34#当天@
            19#厦门北#16:59#17:03#当天@
            20#漳州#17:23#17:25#当天@
            21#云霄#17:53#17:55#当天@
            22#饶平#18:15#18:17#当天@
            23#潮汕#18:34#18:37#当天@
            24#葵潭#19:13#19:15#当天@
            25#惠州南#20:11#20:13#当天@
            26#深圳北#20:46##当天
        '''
        pass
        # _station_list = _data.split('@')

    def _parse_ume_flight_info(self, journey, _data):
        ''' 航旅纵横 航班信息
            {
                "mAirlineName": "东方航空",
                "mArrivalAirportName": "双流",
                "mArrivalCity": "成都",
                "mArrivalCnty": "CN",
                "mArrivalCode": "CTU",
                "mArrivalLatitude": "30.581134",
                "mArrivalLongitude": "103.9568",
                "mArrivalTemp": null,
                "mArrivalTerminal": "T2",
                "mArrivalTimeZone": "GMT+08:00",
                "mArrivalWind": null,
                "mArrivalWtherType": null,
                "mBaggageTurn": "17",
                "mBoardingGate": "202",
                "mCheckInTime": "2019-02-21 20:45:00",
                "mCheckcounter": "A01-A50",
                "mDepartureAirportName": "浦东",
                "mDepartureCity": "上海",
                "mDepartureCnty": "CN",
                "mDepartureCode": "PVG",
                "mDepartureTemp": null,
                "mDepartureTerminal": "T1",
                "mDepartureTimeZone": "GMT+08:00",
                "mDepartureWind": null,
                "mDepartureWtherType": null,
                "mFlightNum": "MU5417",
                "mFlightStatusStr": "到达",
                "mFlyMsgs": [
                    {
                        "mMsgId": "305504537496518656",
                        "mPublishTime": "2019-02-22 00:45:57",
                        "mText": "MU5417
                        航班已于00:45(提前25分钟)到达成都双流T2，飞机停在远机位，需转乘摆渡车，请前往17号转盘提取行李。本次实际飞行3小时10分钟，1702公里。成都：晴天，7℃。祝您在成都周末愉快~?",
                        "mTitle": "?航班到达"
                    },
                ],
                "mGMTEstimateArrivalTime": 1550767260000,
                "mGMTEstimateDepartureTime": 1550755800000,
                "mGMTRealArrivalTime":   1550767500000,
                "mGMTRealDepartureTime": 1550756100000,
                "mGMTScheduledArrivalTime": 1550769000000,
                "mGMTScheduledDepartureTime": 1550755800000,
                "mPreStatus": "到达",
                "mScheduledDepartureDate": "2019-02-21",
                "mUpdateTime": 1550768941302
            }
        '''
        try:
            _dict = json.loads(_data)
        except:
            return journey
        if _dict:
            try:
                # journey.account_id = 
                journey.start_time            = _dict.get('mGMTRealDepartureTime')
                journey.depart                = _dict.get('mDepartureCity')
                journey.depart_address        = _dict.get('mDepartureAirportName')
                journey.end_time              = _dict.get('mGMTRealArrivalTime')
                journey.destination           = _dict.get('mArrivalCity')
                journey.destination_address   = _dict.get('mArrivalAirportName')
                journey.destination_longitude = float(_dict.get('mArrivalLongitude'))
                journey.destination_latitude  = float(_dict.get('mArrivalLatitude'))
                # journey.destination_above_sealevel = 
                journey.flightid        = _dict.get('mFlightNum')
                # journey.purchase_price   = _dict.get('mGMTRealArrivalTime')
                journey.aircom          = _dict.get('mAirlineName')
                journey.order_num       = _dict.get('mFlightNum')
                # journey.ticket_status    = _dict.get('mFlightStatusStr')
                # journey.order_time       = _dict.get('mGMTRealArrivalTime')
                journey.latest_mod_time = _dict.get('mUpdateTime')
                journey.source = self.cur_db_source
                # journey.name
                return journey
            except:
                exc()

    # def _parse_passenger(self):
    #     passenger = model_map.Passenger()
        # passenger.sourceFile = self.cur_db_source
        # passenger_info = bplist["cachePassengerKey"]
        # passenger.phone = str(passenger_info["passengerMobile"].Value)
        # passenger.certificate_code = str(passenger_info["SOrderPassengerIDCard"].Value)
        # passenger.name = passenger_info["passengerName"].Value
        # self.qunar_db.db_insert_table_passenger(passenger)            