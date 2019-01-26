# -*- coding: utf-8 -*-
import codecs
import hashlib
import json
import os
import plistlib
import shutil

import clr

import model_eb

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

import System
from PA_runtime import *
from System.Data.SQLite import *
from System.Xml.Linq import *
from System.Xml.XPath import Extensions as XPathExtensions
from PA.InfraLib.Extensions import PlistHelper
import model_im
import model_nd
import model_eb

__author__ = "TaoJianping"
__all__ = ['ModelCol', 'RecoverTableHelper', 'Logger', 'ParserBase', 'TaoUtils', "TimeHelper", "FieldType",
           "FieldConstraints", "BaseModel", "DataModel", 'Fields']


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
            raise Exception("数据库没有正确链接")
        cls.objects = QueryObjects(node, cls)


# 因为只有单文件，没办法，只能放这里面了，不优雅，以后看看
class Fields(object):
    CharField = CharField
    IntegerField = IntegerField
    FloatField = FloatField
    BlobField = BlobField
