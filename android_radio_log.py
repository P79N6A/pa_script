#coding=utf-8

__author__ = "Xu Tao"

from PA_runtime import *

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

'''
herf: http://safe.it168.com/a2016/0913/2916/000002916475.shtml

Radio类型日志是与射频相关的日志,主要包含SIM卡信息,STK信息,无线,通话等信息,手机基站信息就存在于其中
通过提取手机Radio日志,并对其中基站信息进行提取和解析,就可以找到基站地理位置数据,进而可以找到该手机历史地理位置数据
'''

mainfest_path = ds.ProjectState.ProjectDir.FullName + "\\Manifest.pgfd"

class Radio(object):

    def __init__(self, node, extract_Deleted, extract_Source):
        self.root = node
        self.extract_Deleted = extract_Deleted
        self.extract_Source = extract_Source


    def parse(self):
        models = []
        plist = NSHelpers.ReadPlist[NSDictionary](mainfest_path)
        if plist is None:
            return
        device = NextStepExts.SafeGetObject[NSDictionary](plist,"Device")
        if device is None:
            return
        radio_log  = NextStepExts.SafeGetString(device,"RadioLog")
        if radio_log is None:
            return
        log_list = radio_log.split("\n")
        pattern = re.compile(".*CellIdentity.*mMcc=(.*) .*mMnc=(.*) .*mLac=(.*) .*mCid=(.*) .*mArfcn.*")
        for line in log_list:
            if line.find("RIL_REQUEST_GET_CELL_INFO_LIST") != -1 and line.find("error") == -1 and len(line) > 153:
                results = re.match(pattern, line)
                if results:
                    if len(results.groups()[0]) != 0 and len(results.groups()[1]) != 0 and len(results.groups()[2]) != 0 and len(results.groups()[3]) != 0:
                        try:
                            if int(results.groups()[1]) in [0,1]:
                                mcc,mnc,lac,ci = results.groups()
                                longitude,latitude = self._get_lbs_data(mcc, mnc, lac, ci)
                                if longitude and latitude:
                                    loc = Location()
                                    coord = Coordinate()
                                    coord.Longitude.Value = longitude
                                    coord.Latitude.Value = latitude
                                    loc.Position.Value = coord
                                    models.append(loc)
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
        json_data =  requests.get(lbs_api, params=payload)
        try:
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
    