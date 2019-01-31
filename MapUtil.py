#coding:utf-8

import hashlib
import math
from PA_runtime import *
from PA.InfraLib.ModelsV2.CommonEnum import CoordinateType,LocationSourceType

# 高德像素坐标转换成经纬度
# src：https://github.com/CntChen/tile-lnglat-transform/blob/master/src/transform-class-slippy.js
# 适用地图：高德，Google Map，OSM
def math_sinh(x):
  return (math.exp(x) - math.exp(-x)) / 2

def getMapSize(level):
    return math.pow(2, level)

def pixelXTolng(pixelX, level=20):
    try:
        pixelXToTileAddition = pixelX / 256.0
        lngitude = pixelXToTileAddition / getMapSize(level) * 360 - 180
        if lngitude < 180 and lngitude > 0:
           return lngitude
        else:
           return pixelX
    except Exception as e:
        return pixelX

def pixelYToLat(pixelY, level=20):
    try:
        pixelYToTileAddition = pixelY / 256.0
        latitude = math.atan(math_sinh(math.pi * (1 - 2 * (pixelYToTileAddition) / getMapSize(level)))) * 180.0 / math.pi
        if latitude < 90 and latitude > 0:
            return latitude
        else:
            return pixelY
    except Exception as e:
        print(e)
        return pixelY


def md5(cache_path, node_path):
    m = hashlib.md5()   
    m.update(node_path.encode(encoding = 'utf-8'))
    db_path = cache_path + "\\" + m.hexdigest() + ".db"
    return db_path


def convert_to_timestamp(timestamp):
    if len(str(timestamp)) == 13:
        timestamp = int(str(timestamp)[0:10])
    elif len(str(timestamp)) != 13 and len(str(timestamp)) != 10:
        timestamp = 0
    elif len(str(timestamp)) == 10:
        timestamp = timestamp
    ts = TimeStamp.FromUnixTime(timestamp, False)
    if not ts.IsValidForSmartphone():
        ts = None
    return ts
            

def format_file_path(source_file):
    if source_file:
        return source_file.replace('/', '\\')


def convert_deleted_status(deleted):
    if deleted is None:
        return DeletedState.Unknown
    else:
        return DeletedState.Intact if deleted == 0 else DeletedState.Deleted


def convert_coordinat_type(type_value):
    if type_value == 1:             # GPS坐标
        return CoordinateType.GPS 
    elif type_value == 2:           # GPS米制坐标
        return CoordinateType.GPSmc
    elif type_value == 3:           # GCJ02坐标
        return CoordinateType.Google
    elif type_value == 4:           # GCJ02米制坐标
        return CoordinateType.Googlemc
    elif type_value == 5:           # 百度经纬度坐标
        return CoordinateType.Baidu
    elif type_value == 6:           # 百度米制坐标
        return CoordinateType.Baidumc
    elif type_value == 7:           # mapbar地图坐标
        return CoordinateType.MapBar
    elif type_value == 8:           # 51地图坐标
        return CoordinateType.Map51
    elif type_value == 9:           # 51地图坐标
        return CoordinateType.GPS
