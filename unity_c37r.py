#coding:utf-8
#
# Fucking code, Do NOT READ ANY OF THEM
#

__author__ = "chenfeiyang"

import shutil
import hashlib
import logging
import re
import math
from PA.InfraLib.Utils import *
from PA_runtime import *
import sys
reload(sys)
import os
import traceback
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
        return int(reader.GetDouble(idx))
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

def is_md5(string):
    grp = re.search('[a-fA-F0-9]{32,32}', string)
    return True if grp is not None else False

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
        #self.logger = create_logger(path, en_cmd, id)
        self.level = 0

    def set_level(self, val):
        self.level = val

    def m_print(self, msg):
        if self.level == 1:
            print(msg)
        return
        self.logger.info(msg)
    
    def m_err(self, msg):
        if self.level == 1:
            print(msg)
        return
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
        if not ts.IsValidForSmartphone():
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
SQL_TP_TXT  = 0
SQL_TP_INT  = 1
SQL_TP_BLOB = 2
SQL_TP_REAL = 3
#
# 因为C# sqlite 对于类型有强依赖，这样便于写代码时减少错误
# 这个可能不是很全面，因为我还没有收集SQLITE的全部类型。。。。
#
def get_sqlite_tbl_info(cmd, tbl_name):
    res = list()
    cmd.CommandText = '''pragma table_info(%s)''' %tbl_name
    reader = cmd.ExecuteReader()
    while reader.Read():
        tp = c_sharp_get_string(reader, 2)
        if tp == 'text':
            res.append(SQL_TP_TXT)
        elif tp == 'int':
            res.append(SQL_TP_INT)
        elif tp == 'blob':
            res.append(SQL_TP_BLOB)
        elif tp == 'real':
            res.append(SQL_TP_REAL)
    return res

def execute_query(cmd, cmd_text, values):
    cmd.CommandText = cmd_text
    cmd.Parameters.Clear()
    for v in values:
        p = cmd.CreateParameter()
        p.Value = v
        cmd.Parameters.Add(p)
    cmd.ExecuteNonQuery()

#
# copy file....
# note, give node, not db path
# it executes a simple command, then read the results, if any error happens, we copy the sqlite file to cache(C37R)
#
def create_connection_tentatively(db_node, read_only = True):
    cmd = 'DataSource = {}; ReadOnly = {}'
    cmd = cmd.format(db_node.PathWithMountPoint, 'True' if read_only else 'False')
    try:
        conn = None
        dcmd = None
        reader = None
        conn = sql.SQLiteConnection(cmd)
        conn.Open()
        dcmd = sql.SQLiteCommand(conn)
        dcmd.CommandText  = '''
            select * from sqlite_master limit 1 offset 0
        '''
        reader = dcmd.ExecuteReader()
        reader.Read()
        reader.Close()
        dcmd.Dispose()
        return conn
    except:
        traceback.print_exc()
        if reader is not None:
            reader.Close()
        if dcmd is not None:
            dcmd.Dispose()
        if conn is not None:
            conn.Close()
        data = db_node.Data
        sz = db_node.Size
        cache = ds.OpenCachePath('C37R')
        if not os.path.exists(cache):
            os.mkdir(cache)
        cache_db = cache + '/' + md5(db_node.PathWithMountPoint)
        f = open(cache_db, 'wb+')
        f.write(data.read(sz))
        f.close()
        cmd = 'DataSource = {}; ReadOnly = {}'.format(cache_db, 'True' if read_only else 'False')
        conn = sql.SQLiteConnection(cmd)
        conn.Open()
        return conn
#
# MACROS
#
WA_FILE_TEXT = 1
WA_FILE_IMAGE = 2
WA_FILE_AUDIO = 3
WA_FILE_VIDEO = 4
WA_FILE_OTHER = 99

#
# 产生用户类型
#
def create_user_intro(uid, name, photo):
    usr = Common.UserIntro()
    usr.ID.Value = uid
    usr.Name.Value = name
    usr.Photo.Value = get_c_sharp_uri(photo)
    return usr

#
# 所有c37r系列的类型的基类型
#

class C37RBasic(object):
    
    def __init__(self):
        self.idx = -1
        self.res = list()
    
    def set_value_with_idx(self, idx, val):
        if idx >= len(self.res):
            return
        self.res[idx] = val
    
    def get_value_with_idx(self, idx):
        if idx >= len(self.res):
            return None
        return self.res[idx]
    
    def get_values(self):
        return self.res
#
# check db-version is newest version or not
# db_name: sqlite path
# v_key: version key
# v_value: desired version value
# result : false if check version failed, you may delete this sqlite file, then regenerate it.
#

TBL_CREATE_VERSION = '''
    create table if not exists tb_version(v_key text, v_val int)
'''

TBL_INSERT_VERSION = '''
    insert into tb_version values (?,?)
'''

def CheckVersion(db_name, v_key, v_value):
    try:
        if not os.path.exists(db_name):
            return False
        conn = None
        cmd = None
        reader = None
        res = False
        conn = create_connection(db_name)
        cmd = sql.SQLiteCommand(conn)
        cmd.CommandText = '''
            select v_val from tb_version where v_key = '{}'
        '''.format(v_key)
        reader = cmd.ExecuteReader()
        if reader.Read():
            value = c_sharp_get_long(reader, 0)
            if value == v_value:
                res = True
            else:
                res = False
        reader.Close()
        cmd.Dispose()
        conn.Close()
        return res
    except:
        if reader is not None:
            reader.Close()
        if cmd is not None:
            cmd.Dispose()
        if conn is not None:
            conn.Close()
        return False

#
# like unix "strings" command-line tool
# returns readable strings list
#
def py_strings(file):
    chars = r"A-Za-z0-9/\-:.,_$%'()[\]<> "
    shortestReturnChar = 4
    regExp = '[%s]{%d,}' % (chars, shortestReturnChar)
    pattern = re.compile(regExp) # accelerate regularexpression match speed.
    with open(file, 'rb') as f:
        return pattern.findall(f.read())

#
# 检查数据库文件完备性
# input :   sqlite_node(not path)
# cache:    用以copy sqlite 文件的文件夹
# 返回：    如果是正常数据库，则返回原始路径，如果异常，则返回拷贝后的路径
#
def check_sqlite_maturity(sqlite_node, cache):
    pth = sqlite_node.PathWithMountPoint
    ret = 0x0
    if os.path.exists(pth + '-shm'):
        ret |= 0x2
    if os.path.exists(pth + '-wal'):
        ret |= 0x1
    if ret != 0x3:
        hash_code = md5(pth)
        out_file = os.path.join(cache, hash_code)
        mapping_file_with_copy(pth, out_file)
        if ret & 0x1:
            mapping_file_with_copy(pth + '-wal', out_file + '-wal')
        if ret & 0x2:
            mapping_file_with_copy(pth + '-shm', out_file + '-shm')
        return out_file
    return pth

#
# 恢复数据中的相关内容
#
def try_get_rec_value(rec, key, def_val = None):
    try:
        if not rec[key].IsDBNull:
            return rec[key].Value
        else:
            return def_val
    except:
        return def_val