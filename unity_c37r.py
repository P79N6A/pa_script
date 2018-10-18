#coding:utf-8
#
# Fucking code, Do NOT READ ANY OF THEM
#

import shutil
import hashlib
import logging
import re
import math
from PA.InfraLib.Utils import *
import sys
reload(sys)


sys.setdefaultencoding("utf8")

#
# C# Unity
#
import clr
clr.AddReference('System.Data.SQLite')
del clr

import System.Data.SQLite as sql

on_c_sharp_platform  = True

def format_mac_timestamp(mac_time, v = 10):
    """
    from mac-timestamp generate unix time stamp
    """
    date = 0
    date_2 = mac_time
    if mac_time < 1000000000:
        date = mac_time + 978307200
    else:
        date = mac_time
        date_2 = date_2 - 978278400 - 8 * 3600
    s_ret = date if v > 5 else date_2
    return int(s_ret)

#
#   C SHARP GET FUCKING
#   you must ensure that your scripts code have import sqlite already!
#   and reader is a sqlite-reader object!
#
def c_sharp_get_string(reader, idx):
    return reader.GetString(idx) if not reader.IsDBNull(idx) else ""

def c_sharp_get_long(reader, idx):
    return reader.GetInt64(idx) if not reader.IsDBNull(idx) else 0

def c_sharp_get_blob(reader, idx):
    return bytearray(reader.GetValue(idx)) if not reader.IsDBNull(idx) else 0

def c_sharp_get_real(reader, idx):
    return reader.GetDouble(idx) if not reader.IsDBNull(idx) else 0.0

def c_sharp_get_time(reader, idx):
    return reader.GetInt32(idx) if not reader.IsDBNull(idx) else 0

def c_sharp_try_get_time(reader, idx):
    if reader.IsDBNull(idx):
        return 0
    try:
        return reader.GetDouble(idx)
    except:
        pass
    try:
        return reader.GetInt32(idx)
    except:
        pass
    try:
        return reader.GetInt64(idx)
    except:
        return 0

# using shutils to copy file
def mapping_file_with_copy(src, dst):
    if src is None or src is "":
        print('file not copied as src is empty!')
        return False
    if dst is None or dst is "":
        print('file not copied as dst is lost!')
        return False
    shutil.copy(src, dst)

# not implemented right now...
def mapping_file_with_safe_read(src, dst):
    pass

def md5(string):
    return hashlib.md5(string).hexdigest()

def create_connection(src, read_only = True):
    cmd = 'DataSource = {}; ReadOnly = True'.format(src) if read_only else 'DataSource = {}'.format(src)
    conn = sql.SQLiteConnection(cmd)
    conn.Open()
    return conn

def create_logger(path, en_cmd, identifier = 'general'):
    logger = logging.getLogger(identifier)
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(path)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    if en_cmd:
        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    return logger

class SimpleLogger(object):
    def __init__(self, path, en_cmd, id):
        self.logger = create_logger(path, en_cmd, id)
        self.level = 0

    def set_level(self, val):
        self.level = val

    def m_print(self, msg):
        if self.level == 1:
            print(msg)
        self.logger.info(msg)
    
    def m_err(self, msg):
        if self.level == 1:
            print(msg)
        self.logger.error(msg)

# only for small filesystem...
# for escape certain fs_name_check...(like xxxx/twitter.db xxxx/message.db)
def search_for_certain(fs, regx):
    global on_c_sharp_platform
    if not on_c_sharp_platform:
        return list()
    nodes = fs.Search(regx)
    ns = Enumerable.ToList[Node](nodes)
    res = list()
    abs_path = fs.PathWithMountPoint
    res = list()
    for n in ns:
        r = re.search('{}/(.*)'.format(abs_path), fs, re.I | re.M).group(1) # 返回fs的节点，注意node是否实现Search
        res.append(list)
    return res

# correct illegal strings in file names under Windows
def correct_isvilid_path(src_node):
    if src_node is None:
        return
    file_path, file_name = os.path.split(src_node.PathWithMountPoint)
    isvalid_string = ["\/", "\\", ":", "*", "?", "<", ">", "|"]
    if [s for s in isvalid_string if s in file_name]:
        cache = ds.OpenCachePath("Logs")
        des_file = os.path.join(cache, file_name.replace(":","_"))
        f = open(des_file, 'wb+')
        data = src_node.Data
        sz = src_node.Size
        f.write(bytes(data.read(sz)))
        f.close()
        return des_file
    else:
        return src_node.PathWithMountPoint

def get_btree_node_str(b, k, d = ""):
    if k in b.Children and b.Children[k] is not None:
        try:
            return str(b.Children[k].Value)
        except:
            return d
    return d

def get_c_sharp_ts(ts):
    try:
        ts = TimeStamp.FromUnixTime(ts, False)
        if not ts.IsValidFromSmartphone():
            ts = None
        return ts
    except:
        return None

def get_c_sharp_uri(path):
    return ConvertHelper.ToUri(path)


# 坐标转换
earthR = 6378137.0

def outOfChina(lat, lng):
    return not (72.004 <= lng <= 137.8347 and 0.8293 <= lat <= 55.8271)

def transform(x, y):
	xy = x * y
	absX = math.sqrt(abs(x))
	xPi = x * math.pi
	yPi = y * math.pi
	d = 20.0*math.sin(6.0*xPi) + 20.0*math.sin(2.0*xPi)

	lat = d
	lng = d

	lat += 20.0*math.sin(yPi) + 40.0*math.sin(yPi/3.0)
	lng += 20.0*math.sin(xPi) + 40.0*math.sin(xPi/3.0)

	lat += 160.0*math.sin(yPi/12.0) + 320*math.sin(yPi/30.0)
	lng += 150.0*math.sin(xPi/12.0) + 300.0*math.sin(xPi/30.0)

	lat *= 2.0 / 3.0
	lng *= 2.0 / 3.0

	lat += -100.0 + 2.0*x + 3.0*y + 0.2*y*y + 0.1*xy + 0.2*absX
	lng += 300.0 + x + 2.0*y + 0.1*x*x + 0.1*xy + 0.1*absX

	return lat, lng

def delta(lat, lng):
    ee = 0.00669342162296594323
    dLat, dLng = transform(lng-105.0, lat-35.0)
    radLat = lat / 180.0 * math.pi
    magic = math.sin(radLat)
    magic = 1 - ee * magic * magic
    sqrtMagic = math.sqrt(magic)
    dLat = (dLat * 180.0) / ((earthR * (1 - ee)) / (magic * sqrtMagic) * math.pi)
    dLng = (dLng * 180.0) / (earthR / sqrtMagic * math.cos(radLat) * math.pi)
    return dLat, dLng

def wgs2gcj(wgsLat, wgsLng):
    if outOfChina(wgsLat, wgsLng):
        return wgsLat, wgsLng
    else:
        dlat, dlng = delta(wgsLat, wgsLng)
        return wgsLat + dlat, wgsLng + dlng


def gcj2wgs(gcjLat, gcjLng):
    if outOfChina(gcjLat, gcjLng):
        return gcjLat, gcjLng
    else:
        dlat, dlng = delta(gcjLat, gcjLng)
        return gcjLat - dlat, gcjLng - dlng

def gcj2wgs_exact(gcjLat, gcjLng):
    initDelta = 0.01
    threshold = 0.000001
    dLat = dLng = initDelta
    mLat = gcjLat - dLat
    mLng = gcjLng - dLng
    pLat = gcjLat + dLat
    pLng = gcjLng + dLng
    for i in range(30):
        wgsLat = (mLat + pLat) / 2
        wgsLng = (mLng + pLng) / 2
        tmplat, tmplng = wgs2gcj(wgsLat, wgsLng)
        dLat = tmplat - gcjLat
        dLng = tmplng - gcjLng
        if abs(dLat) < threshold and abs(dLng) < threshold:
            return wgsLat, wgsLng
        if dLat > 0:
            pLat = wgsLat
        else:
            mLat = wgsLat
        if dLng > 0:
            pLng = wgsLng
        else:
            mLng = wgsLng
    return wgsLat, wgsLng