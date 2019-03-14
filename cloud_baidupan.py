# coding=utf-8

__author__ = 'TaoJianping'

import clr

try:
    clr.AddReference('model_nd')
    clr.AddReference('ScriptUtils')
except:
    pass

clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')

del clr
import PA_runtime
import model_nd
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
BAIDUPANVERSION = 1


def print_error():
    if DEBUG:
        TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))


class YunkanBaiduPanParser(YunkanParserBase):
    """
    云勘数据 备份解析 -> 百度网盘
        1. 文件列表
    """

    def __init__(self, node, extract_deleted, extract_source, app_name='YunkanBaiduPan'):
        super(YunkanBaiduPanParser, self).__init__(node, extract_deleted, extract_source, app_name)
        self.app_version = BAIDUPANVERSION
        self.account_id = self._get_owner_phone(node)
        self.model_nd = model_nd.NetDisk(self.cache_db, self.app_version)
        self.debug = DEBUG

    @staticmethod
    def _get_owner_phone(node):
        return 18256078414

    def _get_file_list_file(self, node):
        node = node.GetByPath('/file.json')
        return node

    def _insert_item_to_db(self, obj):
        f = model_nd.NDFileList()
        f.set_value_with_idx(f.account, self.account_id)
        f.set_value_with_idx(f.file_name, obj.get('name', None))
        f.set_value_with_idx(f.file_size, obj.get('Size', None))
        f.set_value_with_idx(f.update_time, TimeHelper.str_to_ts(obj.get('FileModifyedTime', None),
                                                                 _format="%Y-%m-%dT%H:%M:%S"))
        f.set_value_with_idx(f.server_path, obj.get('Path', None))
        self.model_nd.db_insert_filelist(f.get_values())

    def _parse_file_list_item(self, obj):
        item_type = obj['FileItemType']
        if item_type == 1:
            children = obj['Children']
            for c in children:
                self._parse_file_list_item(c)
        elif item_type == 0:
            self._insert_item_to_db(obj)

    def _parse_file_list(self, node):
        node = self._get_file_list_file(node)
        file_data = self._open_json_file(node)

        self._parse_file_list_item(file_data)
        self.model_nd.db_commit()

    def _main(self):
        for node in self.root.Children:
            self._parse_file_list(node)

    def _update_db_version(self):
        self.model_nd.db_insert_version(model_nd.NDDBVersionKey, model_nd.NDDBVersionValue)
        self.model_nd.db_insert_version(model_nd.NDDBApplicationVersionKey, self.app_version)
        self.model_nd.db_insert_im_version(self.app_version)

    def generate_models(self):
        generate = model_nd.NDModel(self.cache_db)
        nd_results = generate.generate_models()
        return nd_results

    def parse(self):
        if self.debug or self.model_nd.need_parse:
            self._main()
            self._update_db_version()
            self.model_nd.db_commit()
            self.model_nd.db_close()

        return self.generate_models()


def parse_yunkan_baidupan(root, extract_deleted, extract_source):
    pr = ParserResults()
    pr.Categories = DescripCategories.BDY
    results = YunkanBaiduPanParser(root, extract_deleted, extract_source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("百度云网盘")
    return pr
