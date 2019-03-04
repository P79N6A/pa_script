#coding=utf-8

__author__ = "Xu Tao"

import clr
clr.AddReference("System")
clr.AddReference("PNFA.Formats.NextStep")
clr.AddReference('PNFA.InfraLib.Exts')
del clr

import System
from PA.Formats.NextStep import *
from PA.InfraLib.Extensions import PlistHelper

import requests
import json
import re

from PA_runtime import *

'''
herf: http://safe.it168.com/a2016/0913/2916/000002916475.shtml

Radio类型日志是与射频相关的日志,主要包含SIM卡信息,STK信息,无线,通话等信息,手机基站信息就存在于其中
通过提取手机Radio日志,并对其中基站信息进行提取和解析,就可以找到基站地理位置数据,进而可以找到该手机历史地理位置数据
'''

mainfest_path = ds.ProjectState.ProjectDir.FullName + "\\Manifest.pgfd"

HUAWEI_PHONE = "HUAWEI"
VIVO_PHONE = "VIVO"
NEXUS_PHONE = "NEXUS"
ONEPLUS_PHONE = "ONEPLUS"

# 各种安卓机型的基站信息匹配规则
PATTERN_RULES = {
    HUAWEI_PHONE: ".*CellIdentity.*mMcc=(.*) .*mMnc=(.*) .*mLac=(.*) .*mCid=(.*) .*mArfcn.*",
    VIVO_PHONE: ".*CellIdentity.*mMcc=(.*) .*mMnc=(.*) .*mLac=(.*) .*mCid=(.*) .*mPsc.*",
    NEXUS_PHONE: ".*CellIdentityLte.*mMcc=(.*) mMnc=(.*) mCi=(.*) mPci.*mTac=(.*) mEarfcn.*CellIdentityLte.*",
    ONEPLUS_PHONE: ".*CellIdentityLte.*mCi=(.*?) mPci=.*mTac=(.*?) mEarfcn.*mMcc=(.*?) mMnc=(.*?) mAlphaLong.*CellIdentityLte.*CellIdentityLte.*"
}


class Radio(object):

    def __init__(self, node, extract_Deleted, extract_Source):
        self.root = node
        self.extract_Deleted = extract_Deleted
        self.extract_Source = extract_Source

    def parse(self):
        self.create_cache()
        models = []
        plist = NSHelpers.ReadPlist[NSDictionary](mainfest_path)
        if plist is None:
            return
        device = NextStepExts.SafeGetObject[NSDictionary](plist,"Device")
        if device is None:
            return
        Info = NextStepExts.SafeGetObject[NSDictionary](plist,"Info")
        phone_name = NextStepExts.SafeGetString(Info,"Name")
        radio_log  = NextStepExts.SafeGetString(device,"RadioLog")
        if "Xiaomi" in phone_name:
            pass
        elif "vivo" in phone_name:
            models.extend(self.parse_vivo_radio(radio_log, PATTERN_RULES[VIVO_PHONE]))
        elif "huawei" in phone_name:
            models.extend(self.parse_huawei_radio(radio_log, PATTERN_RULES[HUAWEI_PHONE]))
        elif "honor" in phone_name:
            pass
        elif "oppo" in phone_name:
            pass
        elif "LGE" in phone_name:
            models.extend(self.parse_nexus_radio(radio_log, PATTERN_RULES[NEXUS_PHONE]))
        elif "oneplus" in phone_name:
            models.extend(self.parse_oneplus_radio(radio_log, PATTERN_RULES[ONEPLUS_PHONE]))
        elif "zte" in phone_name:
            pass
        elif "meizu" in phone_name:
            pass
        self.close_cache()
        return models

    def create_cache(self):
        '''创建中间数据库'''
        self.cd = bcp_connectdevice.ConnectDeviceBcp()
        cachepath = ds.OpenCachePath("基站信息")
        md5_db = hashlib.md5()
        db_name = 'radio_log'
        md5_db.update(db_name.encode(encoding = 'utf-8'))
        self.db_path = cachepath + '\\' + md5_db.hexdigest().upper() + '.db'
        self.cd.db_create(self.db_path)

    def insert_cache(self, mcc, mnc, lac, ci, latitude, longitude):
        '''插入数据'''
        base = bcp_connectdevice.BasestationInfo()
        base.MCC = mcc
        base.MNC = mnc
        base.LAC = lac
        base.CellID = ci
        base.LATITUDE = latitude
        base.LONGITUDE = longitude
        self.cd.db_insert_table_basestation_information(base)

    def close_cache(self):
        '''关闭数据库,导出bcp'''
        try:
            self.cd.db_commit()
            self.cd.db_close()
            #bcp entry
            temp_dir = ds.OpenCachePath('tmp')
            PA_runtime.save_cache_path(bcp_connectdevice.BASESTATION_INFORMATION, self.db_path, temp_dir)
        except:
            pass
        
    def parse_huawei_radio(self, radio_log, pattern):
        if radio_log is None:
            return
        models = []
        log_list = radio_log.split("\n")
        pattern = re.compile(pattern)
        for line in log_list:
            if line.find("RIL_REQUEST_GET_CELL_INFO_LIST") != -1 and line.find("error") == -1 and len(line) > 153:
                results = re.match(pattern, line)
                if results:
                    if len(results.groups()[0]) != 0 and len(results.groups()[1]) != 0 and len(results.groups()[2]) != 0 and len(results.groups()[3]) != 0:
                        try:
                            if int(results.groups()[1]) in [0,1,11]:
                                celltower = CellTower()
                                mcc,mnc,ci,tac = results.groups()
                                celltower.MNC.Value = mnc
                                celltower.MCC.Value = mcc
                                celltower.LAC.Value = tac
                                celltower.CID.Value = ci
                                models.append(celltower)
                                longitude,latitude = self._get_lbs_data(mcc, mnc, tac, ci)
                                if longitude and latitude:
                                    loc = Location()
                                    coord = Coordinate()
                                    coord.Longitude.Value = longitude
                                    coord.Latitude.Value = latitude
                                    loc.Position.Value = coord
                                    models.append(loc)
                                self.insert_cache(mcc,mnc,lac,ci,latitude,longitude)
                        except Exception as e:
                            pass
        return models

    def parse_vivo_radio(self, radio_log, pattern):
        if radio_log is None:
            return
        models = []
        log_list = radio_log.split("\n")
        pattern = re.compile(pattern)
        for line in log_list:
            if line.find("RIL_REQUEST_GET_CELL_INFO_LIST") != -1:
                results = re.match(pattern, line)
                if results:
                    if len(results.groups()[0]) != 0 and len(results.groups()[1]) != 0 and len(results.groups()[2]) != 0 and len(results.groups()[3]) != 0:
                        try:
                            if int(results.groups()[1]) in [0,1,11]:
                                mcc,mnc,ci,tac = results.groups()
                                celltower = CellTower()
                                mcc,mnc,ci,tac = results.groups()
                                celltower.MNC.Value = mnc
                                celltower.MCC.Value = mcc
                                celltower.LAC.Value = tac
                                celltower.CID.Value = ci
                                models.append(celltower)
                                longitude,latitude = self._get_lbs_data(mcc, mnc, tac, ci)
                                if longitude and latitude:
                                    loc = Location()
                                    coord = Coordinate()
                                    coord.Longitude.Value = longitude
                                    coord.Latitude.Value = latitude
                                    loc.Position.Value = coord
                                    models.append(loc)
                                self.insert_cache(mcc,mnc,lac,ci,latitude,longitude)
                        except Exception as e:
                            pass
        return models

    def parse_nexus_radio(self, radio_log, pattern):
        if radio_log is None:
            return
        models = []
        log_list = radio_log.split("\n")
        pattern = re.compile(pattern)
        for line in log_list:
            if line.find("RIL_REQUEST_GET_CELL_INFO_LIST") != -1:
                results = re.match(pattern, line)
                if results:
                    if len(results.groups()[0]) != 0 and len(results.groups()[1]) != 0 and len(results.groups()[2]) != 0 and len(results.groups()[3]) != 0:
                        try:
                            if int(results.groups()[1]) in [0,1,11]:
                                mcc,mnc,lac,ci = results.groups()
                                celltower = CellTower()
                                celltower.MNC.Value = mnc
                                celltower.MCC.Value = mcc
                                celltower.LAC.Value = lac
                                celltower.CID.Value = ci
                                models.append(celltower)
                                longitude,latitude = self._get_lbs_data(mcc, mnc, lac, ci)
                                if longitude and latitude:
                                    loc = Location()
                                    coord = Coordinate()
                                    coord.Longitude.Value = longitude
                                    coord.Latitude.Value = latitude
                                    loc.Position.Value = coord
                                    models.append(loc)
                                self.insert_cache(mcc,mnc,lac,ci,latitude,longitude)
                        except Exception as e:
                            pass
        return models
        
    def parse_oneplus_radio(self, radio_log, pattern):
        if radio_log is None:
            return
        models = []
        log_list = radio_log.split("\n")
        pattern = re.compile(pattern)
        for line in log_list:
            if line.find("RIL_REQUEST_GET_CELL_INFO_LIST") != -1:
                results = re.match(pattern, line)
                if results:
                    if len(results.groups()[0]) != 0 and len(results.groups()[1]) != 0 and len(results.groups()[2]) != 0 and len(results.groups()[3]) != 0:
                        try:
                            if int(results.groups()[1]) in [0,1]:
                                mci,tac,mcc,mnc = results.groups()
                                celltower = CellTower()
                                celltower.MNC.Value = mnc
                                celltower.MCC.Value = mcc
                                celltower.LAC.Value = tac
                                celltower.CID.Value = mci
                                models.append(celltower)
                                longitude,latitude = self._get_lbs_data(mcc, mnc, tac, mci)
                                if longitude and latitude:
                                    loc = Location()
                                    coord = Coordinate()
                                    coord.Longitude.Value = longitude
                                    coord.Latitude.Value = latitude
                                    loc.Position.Value = coord
                                    models.append(loc)
                                self.insert_cache(mcc,mnc,lac,ci,latitude,longitude)
                        except Exception as e:
                            pass
        return models
    
    def _get_lbs_data(self, mcc, mnc, lac, ci):
        """[summary]
        
        Arguments:
            mcc {[int]} -- [mcc国家代码：中国代码 460]
            mnc {[int]} -- [mnc网络类型：0移动，1联通(电信对应sid)，十进制]
            lac {[int]} -- [lac(电信对应nid)，十进制]
            ci {[int]}  -- [cellid(电信对应bid)，十进制]

        Return:
            longitude {[float]} -- [经度]
            latitude {[float]}  -- [维度]
        """

        payload = {
            "mcc":int(mcc),
            "mnc":int(mnc),
            "lac":int(lac),
            "ci":int(ci),
            "output":"json"
        }

        # 所有免费接口禁止从移动设备端直接访问,请使用固定IP的服务器转发请求
        # 每5分钟限制查询300次,基站接口每日限制查询1000次

        lbs_api = "http://api.cellocation.com:81/cell/"
        try:
            json_data =  requests.get(lbs_api, params=payload, timeout=5)
            longitude, latitude = None, None
            response = json.loads(json_data.content.decode("utf-8"))
            if "lon" in response:
                longitude = response["lon"]
            if "lat" in response:
                latitude = response["lat"]
            return float(longitude),float(latitude)
        except Exception as e:
            return None, None


def analyze_radio(node, extract_Deleted, extract_Source):
    pr = ParserResults()
    results = Radio(node, extract_Deleted, extract_Source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("安卓Radio日志")
    return pr
    