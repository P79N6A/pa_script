# coding=utf-8

import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference('model_im')
    clr.AddReference('model_nd')
    clr.AddReference('model_eb')
except Exception:
    pass
del clr

from PA_runtime import *
from PA.InfraLib.Utils import *
from System.Data.SQLite import *
from System.Xml.Linq import *
from System.Xml.XPath import Extensions as XPathExtensions
from PA.InfraLib.Extensions import PlistHelper

import System.Data.SQLite as sql

import hashlib
import json
import shutil
import logging
import re
import math
import System
import codecs
import os
import plistlib
import sys
reload(sys)
from HTMLParser import HTMLParser
sys.setdefaultencoding("utf8")

import model_im
import model_nd
import model_eb


################################################################################################################
##                                   __author__ = "chenfeiyang"                                               ##
################################################################################################################

#
# C# Unity
#

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



################################################################################################################
##                                   __author__ = "TaoJianping"                                               ##
################################################################################################################


class ModelCol(object):
    def __init__(self, db):
        # TODO 增加db的判定，增加兼容
        db_path = db.PathWithMountPoint
        self.db_path = db_path
        self.conn = System.Data.SQLite.SQLiteConnection(
            'Data Source = {}; Readonly = True'.format(db_path))
        self.cmd = None
        self.is_opened = False
        self.in_context = False
        self.current_reader = None

    def open(self):
        self.conn.Open()
        self.cmd = System.Data.SQLite.SQLiteCommand(self.conn)
        self.is_opened = True

    def close(self):
        if self.current_reader is not None:
            self.current_reader.Close()
        self.cmd.Dispose()
        self.conn.Close()
        self.is_opened = False

    def __enter__(self):
        if self.is_opened is False:
            self.open()
        self.in_context = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        self.in_context = False
        return True

    def __repr__(self):
        return "this db exists in {path}".format(path=self.db_path)

    def __call__(self, sql):
        self.execute_sql(sql)
        return

    def execute_sql(self, sql):
        self.cmd.CommandText = sql
        self.current_reader = self.cmd.ExecuteReader()
        return self.current_reader

    def fetch_reader(self, sql):
        cmd = System.Data.SQLite.SQLiteCommand(self.conn)
        cmd.CommandText = sql
        return cmd.ExecuteReader()

    def has_rest(self):
        return self.current_reader.Read()

    def get_string(self, idx):
        return self.current_reader.GetString(idx) if not self.current_reader.IsDBNull(idx) else ""

    def get_int64(self, idx):
        return self.current_reader.GetInt64(idx) if not self.current_reader.IsDBNull(idx) else 0

    def get_blob(self, idx):
        return self.current_reader.GetValue(idx) if not self.current_reader.IsDBNull(idx) else None

    def get_float(self, idx):
        return self.current_reader.GetFloat(idx) if not self.current_reader.IsDBNull(idx) else 0

    @staticmethod
    def fetch_string(reader, idx):
        return reader.GetString(idx) if not reader.IsDBNull(idx) else ""

    @staticmethod
    def fetch_int64(reader, idx):
        return reader.GetInt64(idx) if not reader.IsDBNull(idx) else 0

    @staticmethod
    def fetch_blob(reader, idx):
        return reader.GetValue(idx) if not reader.IsDBNull(idx) else None

    @staticmethod
    def fetch_float(reader, idx):
        return reader.GetFloat(idx) if not reader.IsDBNull(idx) else 0


# Const
FieldType = SQLiteParser.FieldType
FieldConstraints = SQLiteParser.FieldConstraints


class RecoverTableHelper(object):
    def __init__(self, node):
        self.db = SQLiteParser.Database.FromNode(node, canceller)
        self.db_path = node.PathWithMountPoint

    def get_table(self, table_name, table_config):
        """
        None = 0,
        NotNull = 8,
        Text = 1,   SQLiteParser.FieldType.Text
        Int = 2,    SQLiteParser.FieldType.Int
        Blob = 3,   SQLiteParser.FieldType.Blob
        Float = 4   SQLiteParser.FieldType.Float

        None = 0,
        PrimaryKey = 1,
        NotNull = 2
        :param table_name: 表的名字
        :param table_config: 表的字段的配置
        :return:
        """
        ts = SQLiteParser.TableSignature(table_name)
        for column_name, config in table_config.items():
            field_type = config[0]
            field_constraint = config[1]
            if field_constraint:
                SQLiteParser.Tools.AddSignatureToTable(ts, column_name, field_type, field_constraint)
            else:
                SQLiteParser.Tools.AddSignatureToTable(ts, column_name, field_type)
        return ts

    def is_valid(self):
        return True if self.db else False

    def read_records(self, table, read_delete_records=False, deep_carve=False):
        return self.db.ReadTableRecords(table, read_delete_records, deep_carve)

    def read_deleted_records(self, table, deep_carve=False):
        return self.db.ReadTableDeletedRecords(table, deep_carve)


# 为了不破坏兼容性，只是继承RecoverTableHelper,但功能都是一样的
# ModelCol和BaseModel的区别就是前者能写sql语句,而这个不能
class BaseModel(RecoverTableHelper):
    pass


class TaoUtils(object):
    @staticmethod
    def open_file(file_path, encoding="utf-8"):
        try:
            with codecs.open(file_path, 'r', encoding=encoding) as f:
                return f.read()
        except Exception as e:
            with open(file_path) as f:
                return f.read()

    @staticmethod
    def copy_file(old_path, new_path):
        try:
            shutil.copyfile(old_path, new_path)
            return True
        except Exception as e:
            return False

    @staticmethod
    def copy_dir(old_path, new_path):
        try:
            if os.path.exists(new_path):
                shutil.rmtree(new_path)
            shutil.copytree(old_path, new_path)
            return True

        except Exception as e:
            print(e)
            return False

    @staticmethod
    def list_dir(path):
        return os.listdir(path)

    @staticmethod
    def convert_timestamp(ts):
        try:
            if not ts:
                return None
            ts = str(int(float(ts)))
            if len(ts) > 13:
                return None
            elif float(ts) < 0:
                return None
            elif len(ts) == 13:
                return int(float(ts[:-3]))
            elif len(ts) <= 10:
                return int(float(ts))
            else:
                return None
        except:
            return None

    @staticmethod
    def convert_ts_for_ios(ts):
        try:
            dstart = DateTime(1970, 1, 1, 0, 0, 0)
            cdate = TimeStampFormats.GetTimeStampEpoch1Jan2001(ts)
            return ((cdate - dstart).TotalSeconds)
        except Exception as e:
            return None

    @staticmethod
    def convert_ts_for_mac(mac_time, v=10):
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

    @staticmethod
    def json_loads(data):
        try:
            return json.loads(data)
        except:
            return None

    @staticmethod
    def calculate_file_size(file_path):
        if file_path is None:
            return
        if not os.path.exists(file_path):
            return
        return int(os.path.getsize(file_path))

    @staticmethod
    def hash_md5(words):
        m = hashlib.md5()
        m.update(words)
        return m.hexdigest().upper()

    @staticmethod
    def create_sub_node(node, rpath, vname):
        mem = MemoryRange.CreateFromFile(rpath)
        r_node = Node(vname, Files.NodeType.File)
        r_node.Data = mem
        node.Children.Add(r_node)
        return r_node

    @staticmethod
    def open_plist(file_path):
        try:
            data = plistlib.readPlist(file_path)
        except Exception as e:
            print(e)
            data = None
        return data


class TimeHelper(object):
    @staticmethod
    def str_to_ts(stringify_time, _format="%Y-%m-%d"):
        if not stringify_time:
            return
        time_tuple = time.strptime(stringify_time, _format)
        ts = int(time.mktime(time_tuple))
        return ts

    @staticmethod
    def convert_timestamp(ts):
        try:
            if not ts:
                return None
            ts = str(int(float(ts)))
            if len(ts) > 13:
                return None
            elif float(ts) < 0:
                return None
            elif len(ts) == 13:
                return int(float(ts[:-3]))
            elif len(ts) <= 10:
                return int(float(ts))
            else:
                return None
        except:
            return None

    @staticmethod
    def convert_ts_for_ios(ts):
        try:
            dstart = DateTime(1970, 1, 1, 0, 0, 0)
            cdate = TimeStampFormats.GetTimeStampEpoch1Jan2001(ts)
            return ((cdate - dstart).TotalSeconds)
        except Exception as e:
            return None

    @staticmethod
    def convert_ts_for_mac(mac_time, v=10):
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

    @staticmethod
    def convert_timestamp_for_c_sharp(timestamp):
        """转换成C# 那边的时间戳格式"""
        try:
            ts = TimeStamp.FromUnixTime(timestamp, False)
            if not ts.IsValidForSmartphone():
                ts = None
            return ts
        except Exception as e:
            return None


class Logger(object):
    def __init__(self, debug):
        self.module = None
        self.class_name = None
        self.func_name = None
        self.debug = debug

    def error(self):
        if self.debug:
            TraceService.Trace(TraceLevel.Error, "{module} error: {class_name} {func} ==> {log_info}".format(
                module=self.module,
                class_name=self.class_name,
                func=self.func_name,
                log_info=traceback.format_exc()
            ))

    def info(self, info):
        TraceService.Trace(TraceLevel.Info, "{module} info: {class_name} {func} ==> {log_info}".format(
            module=self.module,
            class_name=self.class_name,
            func=self.func_name,
            log_info=info
        ))


class ParserBase(object):
    """解析类的基类，尽量把基础的函数放在这里，真正的解析类只处理业务逻辑"""

    def __init__(self, root, extract_deleted, extract_source, app_name=None, app_version=1, debug=False):
        self.root = self._get_root_node(root)
        self.app_name = app_name
        self.app_version = app_version
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.cache_db = self._get_cache_db()
        self.logger = Logger(debug)
        self.debug = debug
        self._search_nodes = [self.root, self.root.FileSystem]

    @staticmethod
    def _get_root_node(node, times=0):
        """
        根据传入的节点拿到要检测的根节点
        :param node: 传入的节点
        :param times: 向上返回的次数
        :return: 目标节点
        """
        for i in range(times):
            node = node.Parent
        return node

    def _get_cache_db(self):
        """获取中间数据库的db路径"""
        self.cache_path = ds.OpenCachePath(self.app_name)
        m = hashlib.md5()
        m.update(self.root.AbsolutePath.encode('utf-8'))
        return os.path.join(self.cache_path, m.hexdigest().upper())

    def _copy_root(self):
        """
        因为数据库打开的时候可能会有一些问题，需要把他拷贝出来
        我把它放在了cache目录下
        :return:
        """
        old_root_path = self.root.PathWithMountPoint
        new_root_path = os.path.join(self.cache_path, TaoUtils.hash_md5(old_root_path))
        TaoUtils.copy_dir(old_root_path, new_root_path)
        node = FileSystem.FromLocalDir(new_root_path)
        return node

    def _copy_data(self, *dirs):
        new_data_path_list = [self._copy_data_dir_files(d) for d in dirs]
        return new_data_path_list

    def _copy_data_dir_files(self, data_dir):
        """
        把这个目录下有关的文件夹一般是数据db文件转移到其他地方方便库使用
        :param data_dir:
        :return:
        """
        node = self.root.GetByPath(data_dir)

        if node is None:
            print("not found data")
            return False

        old_dir = node.PathWithMountPoint
        new_dir = os.path.join(self.cache_path, TaoUtils.hash_md5(data_dir))
        TaoUtils.copy_dir(old_dir, new_dir)
        return new_dir

    def _generate_nd_models(self):
        """网盘类应用 => 从中间数据库返回models给C#那边"""
        generate = model_nd.NDModel(self.cache_db)
        nd_results = generate.generate_models()

        generate = model_im.GenerateModel(self.cache_db + ".IM")
        im_results = generate.get_models()

        return nd_results + im_results

    def _generate_im_models(self):
        generate = model_im.GenerateModel(self.cache_db)
        results = generate.get_models()
        return results

    def _add_media_path(self, obj, file_name):
        try:
            searchkey = file_name
            nodes = self.root.FileSystem.Search(searchkey + '$')
            for node in nodes:
                obj.media_path = node.AbsolutePath
                if obj.media_path.endswith('.mp3'):
                    obj.type = model_im.MESSAGE_CONTENT_TYPE_VOICE
                elif obj.media_path.endswith('.amr'):
                    obj.type = model_im.MESSAGE_CONTENT_TYPE_VOICE
                elif obj.media_path.endswith('.slk'):
                    obj.type = model_im.MESSAGE_CONTENT_TYPE_VOICE
                elif obj.media_path.endswith('.mp4'):
                    obj.type = model_im.MESSAGE_CONTENT_TYPE_VIDEO
                elif obj.media_path.endswith('.jpg'):
                    obj.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                elif obj.media_path.endswith('.png'):
                    obj.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                else:
                    obj.type = model_im.MESSAGE_CONTENT_TYPE_ATTACHMENT
                return True
        except Exception as e:
            print (e)
        return False

    @staticmethod
    def load_nd_models(cache_db, app_version):
        """
        初始化并返回网盘类应用需要的 model
        :param cache_db: 中间数据库的地址
        :param app_version: 应用的版本
        :return:
        """
        model_nd_col = model_nd.NetDisk(cache_db, app_version)
        model_im_col = model_nd_col.im
        return model_nd_col, model_im_col

    @staticmethod
    def load_im_model():
        model_im_col = model_im.IM()
        return model_im_col

    @staticmethod
    def load_eb_models(cache_db, app_version, app_name):
        eb = model_eb.EB(cache_db, app_version, app_name)
        im = eb.im
        return eb, im

    def _update_model_version(self, model, app_version):
        model.db_insert_im_version(app_version)
        return True

    @staticmethod
    def _update_eb_script_version(model_eb_col, eb_app_version):
        """当更新数据完成之后，更新version表的内容，方便日后检查"""
        model_eb_col.db_insert_table_version(model_eb.EB_VERSION_KEY, model_eb.EB_VERSION_VALUE)
        model_eb_col.db_insert_table_version(model_eb.EB_APP_VERSION_KEY, eb_app_version)
        model_eb_col.db_commit()
        model_eb_col.sync_im_version()

    def create_sub_node(self, rpath, vname):
        mem = MemoryRange.CreateFromFile(rpath)
        r_node = Node(vname, Files.NodeType.File)
        r_node.Data = mem
        self.root.Children.Add(r_node)
        return r_node

    def create_sub_dir_node(self, rpath):
        d_node = FileSystem.FromLocalDir(rpath)
        self.root.Children.Add(d_node)
        return d_node

    def _search_file(self, file_name):
        """搜索函数"""
        search_nodes = self._search_nodes[:]
        for node in search_nodes:
            results = node.Search(file_name + "$")
            for result in results:
                if os.path.isfile(result.PathWithMountPoint):
                    if result.Parent not in self._search_nodes:
                        self._search_nodes.insert(0, result.Parent)
                    return result
        return None

    def _search_file_simple(self, file_name):
        file_ = self.root.Search(file_name)
        return next(iter(file_), None)


class BaseField(object):
    def __init__(self, column_name, null=True):
        self.name = column_name
        self.constraint = FieldConstraints.NotNull if null is True else FieldConstraints.None


class CharField(BaseField):
    def __init__(self, column_name, null=True):
        super(CharField, self).__init__(column_name, null)
        self.type = FieldType.Text


class IntegerField(BaseField):
    def __init__(self, column_name, null=True):
        super(IntegerField, self).__init__(column_name, null)
        self.type = FieldType.Int


class FloatField(BaseField):
    def __init__(self, column_name, null=True):
        super(FloatField, self).__init__(column_name, null)
        self.type = FieldType.Float


class BlobField(BaseField):
    def __init__(self, column_name, null=True):
        super(BlobField, self).__init__(column_name, null)
        self.type = FieldType.Blob


class DataModelMeta(type):
    instances = {}

    def __new__(mcs, name, bases, attrs):

        if not attrs.get('__table__', None):
            return super(DataModelMeta, mcs).__new__(mcs, name, bases, attrs)

        table_name = attrs['__table__']
        instance = mcs.instances.get(name, None)
        if instance is None:
            config = mcs.get_table_config(table_name, attrs)
            instance = super(DataModelMeta, mcs).__new__(mcs, name, bases, attrs)
            instance.__config__ = config
            instance.__attr_map__ = {k: v for k, v in attrs.items() if isinstance(v, BaseField)}
            mcs.instances[name] = instance
            return instance
        return instance

    @staticmethod
    def get_table_config(table_name, table_config):
        ts = SQLiteParser.TableSignature(table_name)
        for field in table_config.values():
            if isinstance(field, BaseField):
                SQLiteParser.Tools.AddSignatureToTable(ts, field.name, field.type, field.constraint)
        return ts


class QueryObjects(object):
    def __init__(self, node, cls):
        self.db = SQLiteParser.Database.FromNode(node)
        self.source_path = node.PathWithMountPoint
        self._class = cls

    @property
    def all(self):
        for record in self.db.ReadTableRecords(self._class.__config__, True, False):
            ins = self._class()
            for attr, field in self._class.__attr_map__.items():
                val = record[field.name].Value
                if isinstance(val, DBNull):
                    val = None
                setattr(ins, attr, val)
            ins.deleted = record.IsDeleted
            ins.source_path = self.source_path
            yield ins

    def execute_sql(self, sql):
        with ModelCol(self.source_path) as connection:
            return connection.fetch_reader(sql)


class DataModel(object):
    __metaclass__ = DataModelMeta
    __attr_map__ = None
    __config__ = None
    objects = None

    @classmethod
    def connect(cls, node):
        if not node:
            return False
        cls.objects = QueryObjects(node, cls)
        return True if cls.objects.db else False


# 因为只有单文件，没办法，只能放这里面了，不优雅，以后看看
class Fields(object):
    CharField = CharField
    IntegerField = IntegerField
    FloatField = FloatField
    BlobField = BlobField


class SemiXmlParser(object):
    def __init__(self):
        self.root = None

    @staticmethod
    def _get_content_end(raw_data, start_index):
        while 1:
            if start_index >= len(raw_data):
                return start_index
            if raw_data[start_index] == 0x00:
                return start_index
            else:
                start_index += 0x01

    def parse(self, raw_data):
        if not isinstance(raw_data, bytearray):
            raw_data = bytearray(raw_data)
        data_length = len(raw_data)

        field_list = []
        value_list = []
        ptr = 0x0a

        while 1:
            if ptr >= data_length:
                break
            field_len = raw_data[ptr + 0x01] - 1
            start_ptr = ptr + 0x03
            end_ptr = start_ptr + field_len
            raw_field_data = raw_data[start_ptr:end_ptr]
            field_list.append(raw_field_data)
            if raw_data[end_ptr] == 0x00 and raw_data[end_ptr + 0x01] == 0x00:
                ptr = end_ptr + 0x02
                content = ""
            elif raw_data[end_ptr] == 0x00 and raw_data[end_ptr + 0x01] != 0x00:
                offset = 0x03 if raw_data[end_ptr + 0x01] & 0xF0 == 0xc0 else 0x02
                content_start_ptr = end_ptr + offset
                content_end_ptr = self._get_content_end(raw_data, content_start_ptr)
                content = raw_data[content_start_ptr:content_end_ptr]
                ptr = content_end_ptr
            else:
                content = None
            value_list.append(content)
    
        self.feed(field_list, value_list)
        return self.root

    def feed(self, field_list, value_list):
        field_list = [str(f) for f in field_list]
        value_list = [str(v) for v in value_list]
        for k, v in zip(field_list, value_list):
            keys = k.split('.')
            self.add_keys(keys)
            self.set_value(keys, v)

    def add_keys(self, keys):
        if isinstance(keys, str):
            keys = keys.split(".")
        if self.root is None:
            self.root = SemiXmlNode(keys[0])
        node = self.root
        for i in range(len(keys)-1):
            child_name = keys[i+1]
            child_node = node.get_child(child_name)
            if child_node is None:
                node = node.add_child(child_name)
            else:
                node = child_node

    def set_value(self, keys, value):
        if isinstance(keys, str):
            keys = keys.split(".")
        if self.root is None:
            self.root = SemiXmlNode(keys[0])
        node = self.root
        for i in range(len(keys)-1):
            child_name = keys[i+1]
            child_node = node.get_child(child_name)
            if child_node is None:
                raise Exception('key not exists')
            else:
                if i == (len(keys)-2):
                    child_node.value = value.decode('utf-8')
                node = child_node

    def get_keys(self, keys):
        if isinstance(keys, str):
            keys = keys.split(".")
        if self.root is None:
            return None
        node = self.root
        for i in range(len(keys)-1):
            child_name = keys[i+1]
            child_node = node.get_child(child_name)
            if child_node is None:
                return None
            else:
                if i == (len(keys)-2):
                    return child_node
                node = child_node

    def export_items(self):
        count = int(self.get_keys('msg.appmsg.mmreader.category.$count').value)
        ret_nodes = []
        for i in range(count):
            if i == 0:
                key = 'msg.appmsg.mmreader.category.item'
            else:
                key = 'msg.appmsg.mmreader.category.item' + str(i)
            node = self.get_keys(key)
            if node is not None:
                if key == 'msg.appmsg.mmreader.category.item':
                    ret_nodes.insert(0, node)
                else:
                    ret_nodes.append(node)
        return ret_nodes


class SemiXmlNode(object):
    def __init__(self, name):
        self.name = name
        self.children = []
        self.parent = None
        self.value = None

    def add_child(self, key):
        node = SemiXmlNode(key)
        node.parent = self
        self.children.append(node)
        return node

    def get_child(self, key, default=None):
        for i in self.children:
            if i.name == key:
                return i
        return default

    def __getitem__(self, key, default=None):
        return self.get_child(key, default)

    def get(self, key, default=None):
        for i in self.children:
            if i.name == key:
                return i
        return default


class HtmlNode(object):
    def __init__(self, tag):
        self.tag_name = tag
        self.property = {}
        self.child = []
        self.data = None

    def __getitem__(self, item):
        """暂时只支持返回找到的第一个元素"""
        for node in self.child:
            if node.tag_name == item:
                return node

    def get_all(self, tag_name):
        answer = []
        for node in self.child:
            if node.tag_name == tag_name:
                answer.append(node)
        return answer

    def get(self, key, default=None):
        for node in self.child:
            if node.tag_name == key:
                return node
        return default


class PaHtmlParser(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.__inner_node_list = []
        self.body = {}
        self.root = HtmlNode(tag="html")

    def handle_starttag(self, tag, attrs):
        node = HtmlNode(tag)
        node.property = {k: v for k, v in attrs}
        self.__inner_node_list.append(node)

    def handle_data(self, data):
        if not self.__inner_node_list:
            return
        self.__inner_node_list[-1].data = data

    def handle_endtag(self, tag):
        node = self.__inner_node_list.pop()
        if len(self.__inner_node_list) != 0:
            self.__inner_node_list[-1].child.append(node)
        else:
            self.root.child.append(node)

    @property
    def first_dom(self):
        if len(self.root.child) == 0:
            return
        return self.root.child[0]



class YunkanParserBase(object):
    def __init__(self, node, extract_deleted, extract_source, app_name):
        self.root = node
        self.app_name = app_name
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.cache_db = self._get_cache_db()

    def _get_cache_db(self):
        """获取中间数据库的db路径"""
        self.cache_path = ds.OpenCachePath(self.app_name)
        m = hashlib.md5()
        m.update(self.root.AbsolutePath.encode('utf-8'))
        return os.path.join(self.cache_path, m.hexdigest().upper())

    @staticmethod
    def _open_json_file(node):
        if not node:
            return
        try:
            path = node.PathWithMountPoint
            with open(path, 'r') as f:
                data = json.load(f)
                return data
        except Exception as e:
            return {}


################################################################################################################
##                                    __author__ = "Yangliyuan"                                               ##
################################################################################################################


######### SETTING #########
try:
    CASE_NAME = ds.ProjectState.ProjectDir.Name
except:
    CASE_NAME = ''
DEBUG = True
DEBUG = False
DEBUG_RUN_TIME = True
DEBUG_RUN_TIME = False


######### LOG FUNC #########
def exc(e=''):
    ''' Exception log output '''
    try:
        if DEBUG:
            py_name = os.path.basename(__file__)
            msg = 'DEBUG {} New Case:<{}> :'.format(py_name, CASE_NAME)
            TraceService.Trace(TraceLevel.Warning,
                               (msg+'{}{}').format(traceback.format_exc(), e))
    except:
        pass


def tp(*e):
    ''' Highlight log output in vs '''
    if DEBUG:
        TraceService.Trace(TraceLevel.Warning, '{}'.format(e))
    else:
        pass


def print_run_time(func):
    def wrapper(*args, **kw):
        local_time = time.time()
        res = func(*args, **kw)
        if DEBUG_RUN_TIME:
            msg = 'Current Function <{}> run time is {:.2} s'.format(
                func.__name__, time.time() - local_time)
            TraceService.Trace(TraceLevel.Warning, '{}'.format(msg))
        if res is not None:
            return res

    return wrapper


def parse_decorator(func):
    def wrapper(*args, **kw):
        tp('{} is running ...'.format(func.__name__,))
        res = func(*args, **kw)
        tp('{} is finished !'.format(func.__name__,))
        return res
    return wrapper        


######### Base Class #########

def base_analyze(Parser, node, BCP_TYPE, VERSION_APP_VALUE, build_name, db_name):
    '''
    Args:
        Parser (Parser):
        node (node): 
        BCP_TYPE: 
        VERSION_APP_VALUE (int): VERSION_APP_VALUE
        build_name (str): pr.build
        db_name (str): 中间数据库名称
    Returns:
        pr
    '''
    if 'media' in node.AbsolutePath:
        return 
    res = []
    pr = ParserResults()
    try:
        res = Parser(node, db_name).parse(BCP_TYPE, VERSION_APP_VALUE)
    except:
        msg = '{} 解析新案例 <{}> 出错: {}'.format(db_name, CASE_NAME, traceback.format_exc())
        TraceService.Trace(TraceLevel.Debug, msg)
    if res:
        pr.Models.AddRange(res)
        pr.Build(build_name)
    return pr


class BaseParser(object):
    ''' common func:
            _read_db
            _read_table
            _read_json
            _read_xml
            _is_url
            _is_empty
            _is_duplicate
                    
        Instances need to be implemented: 
            attribute:
                self.root
                self.csm        # c sharp model 
                self.Generate   # e.g. model_browser.Generate
                self.VERSION_VALUE_DB
            func:
                parse
                _convert_nodepath
                update_version
    '''
    def __init__(self, node, db_name=''):
        self.root = node
        self.extract_deleted = True
        self.extract_source = False
        self.csm = None
        self.Generate = None
        hash_str = hashlib.md5(node.AbsolutePath.encode('utf8', 'ignore')).hexdigest()[8:-8]
        self.cachepath = ds.OpenCachePath(db_name)
        self.cache_db = self.cachepath + '\\{}_{}.db'.format(db_name, hash_str)
        self.VERSION_KEY_DB = 'db'
        self.VERSION_VALUE_DB = 0
        self.VERSION_KEY_APP = 'app'

    def parse(self, BCP_TYPE, VERSION_APP_VALUE):
        if DEBUG or self.csm.need_parse(self.cache_db, VERSION_APP_VALUE):
            self.csm.db_create(self.cache_db)
            self.parse_main()
            if not canceller.IsCancellationRequested:
                self.csm.db_insert_table_version(self.VERSION_KEY_DB, self.VERSION_VALUE_DB)
                self.csm.db_insert_table_version(self.VERSION_KEY_APP, VERSION_APP_VALUE)
                self.csm.db_commit()
            self.csm.db_close()
            tmp_dir = ds.OpenCachePath('tmp')
            save_cache_path(BCP_TYPE, self.cache_db, tmp_dir)
        models = self.Generate(self.cache_db).get_models()
        # if 'AndroidSMS' in self.cachepath:
        #     from export_bcp import run as export_bcp_run
        #     import bcp_basic
        #     export_bcp_run(target_id='123',
        #                 bcp_path='F:\Caches',
        #                 case_path=self.cachepath.replace('AndroidSMS', ''),
        #                 mountDir='',
        #                 software_type=bcp_basic.BASIC_SMS_INFORMATION)
        return models

    def parse_main(self):
        pass

    def _convert_nodepath(self, raw_path):
        pass

    @staticmethod
    def _convert_ios_time(timestamp):
        ''' convert_ios_time

        :type timestamp: float, Int64  9 digit
        :rtype unixtime: int           13 digit
        '''
        try:
            if timestamp < 0 or len(str(int(timestamp))) != 9:
                return
            dstart = DateTime(1970, 1, 1, 0, 0, 0)
            cdate = TimeStampFormats.GetTimeStampEpoch1Jan2001(timestamp)
            uinixtime = int((cdate - dstart).TotalSeconds)
            return uinixtime
        except:
            exc()

    def _read_db(self, db_path='', node=None):
        ''' and set self.cur_db, self.cur_db_source
        
        Args:
            db_path (str): 

        Returns:
            bool: is valid db
        '''
        try:
            if node is None:
                node = self.root.GetByPath(db_path)
            self.cur_db = SQLiteParser.Database.FromNode(node, canceller)
            if self.cur_db is None:
                return False
            self.cur_db_source = node.AbsolutePath
            return True
        except:
            exc()
            return False

    def _read_table(self, table_name, read_delete=None):
        ''' read_table
        
        Args:
            table_name (str): 
            read_delete (bool):

        Returns:
            (iterable): self.cur_db.ReadTableDeletedRecords(tb, ...)
        '''
        # 每次读表清空并初始化 self._pk_list
        self._pk_list = []
        if read_delete is None:
            read_delete = self.extract_deleted
        try:
            if table_name not in self.cur_db.Tables:
                tp('======= Warnning !!! table: {} not in db: {}'.format(table_name, self.cur_db.FilePath))
                return []
            tb = SQLiteParser.TableSignature(table_name)
            # res = self.cur_db.ReadTableRecords(tb, read_delete, True)
            return self.cur_db.ReadTableRecords(tb, read_delete, True)
        except:
            if self.cur_db and self.cur_db.FilePath:
                exc('db path: ' + self.cur_db.FilePath)
            else:
                exc()
            return []

    def _read_json(self, json_path='', json_node=None):
        ''' read_json set self.cur_json_source

        Args: 
            json_path (str)

        Returns:
            (bool) or json_data: set self.cur_json_source
        '''
        try:
            if json_node is None:
                json_node = self.root.GetByPath(json_path)
            if not json_node:
                return False
            file = json_node.Data.read().decode('utf-8')
            if not file:
                return False
            json_data = json.loads(file, encoding='utf8')
            self.cur_json_source = json_node.AbsolutePath
            return json_data
        except:
            exc()
            return False

    def _read_xml(self, xml_path='', xml_node=None):
        ''' _read_xml, set self.cur_xml_source

        Args: 
            xml_path (str): self.root.GetByPath(xm_path)

        Returns:
            xml_data (XElement)
        '''
        try:
            if xml_node is None:
                xml_node = self.root.GetByPath(xml_path)
            if xml_node and xml_node.Data:
                xml_data = XElement.Parse(xml_node.read())
                self.cur_xml_source = xml_node.AbsolutePath
                return xml_data
            else:
                return False
        except:
            exc()
            return False

    def _is_duplicate(self, rec=None, pk_name='', pk_value=None):
        ''' filter duplicate record
            基于`ReadTableRecords`是先把正常的遍历完(无序)之后再读删除的

        Args:
            rec (record)
            pk_names (tuple(str, ))

        Returns:
            bool: rec[pk_name].Value in self._pk_list
        '''
        try:
            if pk_value == None:
                pk_value = rec[pk_name].Value
            if IsDBNull(pk_value) or pk_value in self._pk_list:
                return True
            self._pk_list.append(pk_value)
            return False
        except:
            exc()
            return True

    @staticmethod
    def _is_empty(rec, *args):
        ''' 过滤 DBNull 空数据, 有一空值就跳过
        
        Args:
            rec (rec): 
            args (str): fields

        Returns:
            book:
        '''
        try:
            for i in args:
                value = rec[i].Value
                if IsDBNull(value) or value in ('', ' ', None, [], {}):
                    return True
                if isinstance(value, str) and re.search(r'[\x00-\x08\x0b-\x0c\x0e-\x1f]', str(value)):
                    return True
            return False
        except:
            exc()
            tp(*args)
            return True

    def _is_contains(self, rec, *keys):
        return False not in [rec.ContainsKey(key) for key in keys]

    @staticmethod
    def _is_url(rec, *args):
        ''' 匹配 URL IP
        
        Args:
            rec (rec): 
            *args (tuple<str>):

        Returns:
            bool: 
        '''
        URL_PATTERN = r'((http|ftp|https)://)(([a-zA-Z0-9\._-]+\.[a-zA-Z]{2,6})|([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}))(:[0-9]{1,4})*(/[a-zA-Z0-9\&%_\./-~-]*)?'
        IP_PATTERN = r'^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$'

        for i in args:
            try:
                match_url = re.match(URL_PATTERN, rec[i].Value)
                match_ip = re.match(IP_PATTERN, rec[i].Value)
                if not match_url and not match_ip:
                    return False
            except:
                exc()
                return False
        return True

    @staticmethod
    def _is_phone_number(rec, args):
        # 验证手机号, 包含 +86, 86 开头
        if 'subscriber_mdn' in args:
            s = rec['subscriber_mdn'].Value
            try:
                reg_str = r'^((\+86)|(86))?(1)\d{10}$'
                match_obj = re.match(reg_str, s)
                if match_obj is None:
                    return False      
            except:
                exc()
                return False    

    @staticmethod
    def _is_email_format(rec=None, key=None, email_str=''):
        """ 匹配邮箱地址 

        Args:
            rec (rec): 
            key (str): 
            email_str (str): 
            
        Returns:
            bool: is valid email address      
        """
        try:
            if email_str is '' and rec is not None:
                if IsDBNull(rec[key].Value) or len(rec[key].Value.strip()) < 5:
                    return False
                email_str = rec[key].Value
            reg_str = r'^\w+([-+.]\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$'
            match_obj = re.match(reg_str, email_str)
            if match_obj is None:
                return False
            return True
        except:
            exc()
            return False

    @staticmethod
    def _convert_strtime_2_ts(format_time):
        ''' "2018/8/15 15:27:20" -> 10位 时间戳 1534318040.0
            
        Args:
            format_time (str)
            
        Returns:
            (int/float): timastamp e.g. 1534318040.0
        '''
        try:
            PATT = {
                15: '%Y{div}%m{div}%d %H:%M',
                16: '%Y{div}%m{div}%d %H:%M',
                18: '%Y{div}%m{div}%d %H:%M:%S',
                19: '%Y{div}%m{div}%d %H:%M:%S',
            }
            if format_time:
                div_str = format_time[4]
                if re.match(r'\d', div_str):
                    div_str = ''
                time_pattren = PATT.get(len(format_time), '').format(div=div_str)
                if time_pattren:
                    ts = time.strptime(format_time, time_pattren)
                    return time.mktime(ts)
            return 0
        except:
            tp('error ts', format_time)
            exc()
            return 0



class BaseAndroidParser(BaseParser):
    def __init__(self, node, db_name):
        super(BaseAndroidParser, self).__init__(node, db_name)
        if node.FileSystem.Name == 'data.tar':
            self.rename_file_path = ['/storage/emulated', '/data/media'] 
        else:
            self.rename_file_path = None


class ProtobufDecoder(object):
    def __init__(self, blob):
        ''' blob (Array[Byte] | zlib.decompress(Array[Byte]))'''
        if isinstance(blob, Array[Byte]):
            blob = str(bytearray(blob))
        self.raw_data = blob
        self.idx = 0
        self.max_size = len(blob)
        self.is_valid = self.max_size != 0

    @property
    def data(self):
        ''' return the rest of data '''
        return self.raw_data[self.idx:]

    def read(self, length=1):
        if not self.is_out_of_range(length):
            res = self.raw_data[self.idx: self.idx+length]
            return res
        return ''

    def ord_read_char(self, length=1):
        '''return ord(self.read())
        
        Returns:
            ord(int):
        '''
        try:
            res = self.read()
            if res:
                return ord(res)
        except:
            return None

    def read_move(self, length=1):
        '''read and move idx 
           length (int, optional): Defaults to 1. [description]
        
        Returns:
            data (str): 
        '''

        res = self.read(length)
        self.idx += length
        return res

    def is_out_of_range(self, length):
        if self.idx + length > self.max_size:
            return True
        return False

    def get_parscal(self):
        ''' x.decode('utf8') '''
        string_length = self.read_move()
        if string_length:
            try:
                res = self.read_move(ord(string_length)).decode('utf8', 'ignore')
            except:
                res = self.read_move(ord(string_length))
            return res
        return None

    def unpack(self, fmt, length):
        ''' struct.unpack
        
        Args:
            fmt (str): 
            length (int): 
        
        Returns:
            pattern (str): hex string
        '''
        return struct.unpack(fmt, self.read_move(length))[0].encode('hex')
    
    def find(self, sub_str):
        ''' Returnthe lowest index in  remaining data  where substring sub is found, like str.find

        Args:
            sub_str (str): '08001a'

        Returns:
            lowest index: (int)
        '''
        sub_str = sub_str.replace(' ', '')
        return self.idx + self.data.find(sub_str.decode('hex'))

    def find_p_after(self, identify):
        '''return parscal string after identify 

        Args:
            identify (str): e.g. '08 01 12' or '080112'
        
        Returns:
            (str): string after identify and length_char
        '''
        self.idx = self.find(identify) + len(identify.replace(' ', ''))/2
        return self.get_parscal()

    def read_before(self, identify):
        '''return raw str before identify 
        
        Args:
            identify (str): '18 00' or '1800'
        
        Returns:
            (str): string before identify
        '''
        end = self.find(identify)
        return self.read_move(end - self.idx)


########## DB ###########

SQL_CREATE_TABLE_VERSION = '''
    create table if not exists version(
        key TEXT primary key,
        version INT)'''

SQL_INSERT_TABLE_VERSION = '''
    insert or REPLACE into version(key, version) values(?, ?)'''


class BaseDBModel(object):
    def __init__(self):
        self.db = None
        self.db_cmd = None
        self.db_trans = None
        self.VERSION_VALUE_DB = 1
        self.create_sql_list = [SQL_CREATE_TABLE_VERSION]

    def db_create(self, db_path):
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except:
                exc()
        self.db = SQLiteConnection('Data Source = {}'.format(db_path))
        self.db.Open()
        self.db_cmd = SQLiteCommand(self.db)
        try:
            self.db_trans = self.db.BeginTransaction()
        except:
            exc()

        self.db_create_table()
        self.db_commit()

    def db_commit(self):
        if self.db_trans is not None:
            self.db_trans.Commit()
        self.db_trans = self.db.BeginTransaction()

    def db_close(self):
        self.db_trans = None
        if self.db_cmd is not None:
            self.db_cmd.Dispose()
            self.db_cmd = None
        if self.db is not None:
            self.db.Close()
            self.db = None

    def db_create_table(self):
        if self.db_cmd is not None:
            for create_sql in self.create_sql_list:
                self.db_cmd.CommandText = create_sql
                self.db_cmd.ExecuteNonQuery()

    def db_insert_table(self, sql, values):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = sql
            self.db_cmd.Parameters.Clear()
            for value in values:
                param = self.db_cmd.CreateParameter()
                param.Value = value
                self.db_cmd.Parameters.Add(param)
            self.db_cmd.ExecuteNonQuery()

    def db_insert_table_version(self, key, version):
        self.db_insert_table(SQL_INSERT_TABLE_VERSION, (key, version))

    '''
    版本检测分为两部分
    如果中间数据库结构改变，会修改db_version
    如果app增加了新的内容，需要修改app_version
    只有db_version和app_version都没有变化时，才不需要重新解析
    '''

    @staticmethod
    def need_parse(cache_db, app_version):
        if not os.path.exists(cache_db):
            return True
        db = sqlite3.connect(cache_db)
        cursor = db.cursor()
        sql = 'select key, version from version'
        row = None
        db_version_check = False
        app_version_check = False
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            pass

        while row is not None:
            if row[0] == 'db' and row[1] == self.VERSION_VALUE_DB:
                db_version_check = True
            elif row[0] == 'app' and row[1] == app_version:
                app_version_check = True
            row = cursor.fetchone()

        if cursor is not None:
            cursor.close()
        if db is not None:
            db.close()
        return not (db_version_check and app_version_check)


class BaseColumn(object):
    def __init__(self):
        self.source = ''
        self.deleted = 0
        self.repeated = 0

    def __setattr__(self, name, value):
        if not IsDBNull(value):
            if isinstance(value, str):
                # 过滤控制字符, 防止断言失败
                value = re.compile('[\\x00-\\x08\\x0b-\\x0c\\x0e-\\x1f]').sub(' ', value)
            self.__dict__[name] = value

    def get_values(self):
        return (self.source, self.deleted, self.repeated)
