# -*- coding: utf-8 -*-
import clr

__author__ = "TaoJianping"

clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')

try:
    clr.AddReference('ScriptUtils')
    clr.AddReference('model_nd')
    clr.AddReference('model_im')
except Exception as e:
    print("debug", e)

import model_im
import model_nd
from ScriptUtils import ParserBase, DataModel, Fields, TimeHelper, TaoUtils, ModelCol
import PA_runtime
import System
from PA_runtime import *
from System.Data.SQLite import *
from System.Xml.Linq import *
from System.Xml.XPath import Extensions as XPathExtensions
from PA.InfraLib.Extensions import PlistHelper

del clr

# CONST
YunPan360_VERSION = 1
DEBUG = True


class FileList(DataModel):

    __table__ = 'node'

    nid = Fields.CharField(column_name='nid')
    pid = Fields.CharField(column_name='pid')
    file_name = Fields.CharField(column_name='file_name')
    file_size = Fields.IntegerField(column_name='file_size')
    file_hash = Fields.CharField(column_name='file_hash')
    file_type = Fields.IntegerField(column_name='file_type')
    file_category = Fields.IntegerField(column_name='file_category')
    ct = Fields.IntegerField(column_name='create_time')
    mt = Fields.IntegerField(column_name='modify_time')
    owner_qid = Fields.CharField(column_name='owner_qid')


class DownloadFile(DataModel):

    __table__ = 'download'

    nid = Fields.CharField(column_name='nid')
    owner_qid = Fields.CharField(column_name='owner_qid')
    local_path = Fields.CharField(column_name='local_file')
    server_path = Fields.CharField(column_name='remote_file')
    file_size = Fields.CharField(column_name='file_size')
    file_hash = Fields.CharField(column_name='file_hash')
    file_category = Fields.IntegerField(column_name='file_category')
    ct = Fields.CharField(column_name='file_create_time')
    mt = Fields.CharField(column_name='file_modify_time')
    progress = Fields.IntegerField(column_name='display_progress')
    start_time = Fields.IntegerField(column_name='create_time')
    finish_time = Fields.IntegerField(column_name='finish_time')


class YunPan360Parser(ParserBase):

    def __init__(self, root, extract_deleted, extract_source):
        super(YunPan360Parser, self).__init__(
            self._get_root_node(root, times=0),
            extract_deleted,
            extract_source,
            app_name="YunPan360",
            app_version=YunPan360_VERSION,
            debug=DEBUG,
        )

        self.model_nd_col, self.model_im_col = self.load_nd_models(self.cache_db, YunPan360_VERSION)

    def _generate_account_table(self, db):
        if not db:
            return
        account = model_im.Account()
        account.account_id = os.path.basename(db.PathWithMountPoint).replace(".db", "").replace('cloudisk__', "")
        account.username = account.nickname = '360U' + str(account.account_id)
        account.insert_db(self.model_im_col)
        self.model_im_col.db_commit()
        return account

    def _parse_server_path(self, files, file_):
        if file_.file_type == 1:
            return '/'
        if file_.parent is None:
            return '/'
        path = []
        while file_.parent.nid != '0':
            path.insert(0, file_.parent.file_name)
            file_ = file_.parent
        server_path = "/".join(path)
        return '/' + server_path

    def _process_file_type(self, type_):
        if type_ == 1:
            return 2
        elif type_ == 2:
            return 1
        elif type_ == 3:
            return 3
        elif type_ == 4:
            return 4
        else:
            return 0

    def _generate_file_list_table(self, db, account):
        if not db:
            return
        is_connected = FileList.connect(db)
        if not is_connected:
            return

        dir_type = 1
        # TODO: 待优化
        files = {obj.nid: obj for obj in FileList.objects.all}
        root = FileList()
        root.nid = '0'
        root.file_name = ''
        root.file_type = dir_type
        files[root.nid] = root

        for i in files.values():
            i.parent = files.get(i.pid, None)
        for i in files.values():
            i.server_path = self._parse_server_path(files, i) + '/' + i.file_name
            if i.server_path.startswith(r'//'):
                i.server_path = i.server_path[1:]
        for obj in files.values():
            try:
                if obj.file_type == dir_type:
                    continue
                f = model_nd.NDFileList()
                f.set_value_with_idx(f.account, account.account_id)
                f.set_value_with_idx(f.file_name, obj.file_name)
                f.set_value_with_idx(f.file_hash, obj.file_hash)
                f.set_value_with_idx(f.file_size, obj.file_size)
                f.set_value_with_idx(f.create_time, obj.ct)
                f.set_value_with_idx(f.update_time, obj.mt)
                f.set_value_with_idx(f.file_type, self._process_file_type(obj.file_category))
                f.set_value_with_idx(f.server_path, obj.server_path)
                f.set_value_with_idx(f.deleted, obj.deleted)
                self.model_nd_col.db_insert_filelist(f.get_values())
            except Exception as e:
                self.logger.error()
        self.model_nd_col.db_commit()

    def _generate_download_table(self, db, account):
        if not db:
            return
        is_connected_1 = DownloadFile.connect(db)
        is_connected_2 = FileList.connect(db)
        if not all((is_connected_1, is_connected_2)):
            return

        files = {obj.nid: obj for obj in FileList.objects.all}

        for obj in DownloadFile.objects.all:
            try:
                t = model_nd.NDFileTransfer()
                t.set_value_with_idx(t.account, account.account_id)
                t.set_value_with_idx(t.deleted, obj.deleted)
                t.set_value_with_idx(t.server_path, obj.server_path)
                t.set_value_with_idx(t.file_size, obj.file_size)
                t.set_value_with_idx(t.local_path, obj.local_path)
                t.set_value_with_idx(t.file_name, files[obj.nid].fname)
                t.set_value_with_idx(t.hash_code, obj.file_hash)
                if obj.progress == 100:
                    t.set_value_with_idx(t.is_download, model_nd.NDFileDone)
                else:
                    t.set_value_with_idx(t.is_download, model_nd.NDFileProcessing)
                t.set_value_with_idx(t.begin_time, obj.start_time)
                t.set_value_with_idx(t.end_time, obj.finish_time)
                self.model_nd_col.db_insert_transfer(t.get_values())
            except Exception as e:
                self.logger.error()
        self.model_nd_col.db_commit()

    def _main(self):
        databases = self.root.GetByPath('/databases')
        if not databases:
            return
        dbs = databases.Search(r'cloudisk__+\d+\.db$')
        for db in dbs:
            account = self._generate_account_table(db)
            if not account:
                continue
            self._generate_file_list_table(db, account)
            self._generate_download_table(db, account)

    def _db_update_finished_info(self):
        """当更新数据完成之后，更新version表的内容，方便日后检查"""
        self.model_nd_col.db_insert_im_version(YunPan360_VERSION)

    def parse(self):
        """程序入口"""
        if DEBUG or self.model_nd_col.need_parse:
            self._main()
            self._db_update_finished_info()

        generate = model_nd.NDModel(self.cache_db)
        nd_results = generate.generate_models()

        generate = model_im.GenerateModel(self.cache_db + ".IM")
        im_results = generate.get_models()

        return nd_results + im_results


def analyze_YunPan360(root, extract_deleted, extract_source):
    pr = ParserResults()
    pr.Categories = DescripCategories.Pan360
    results = YunPan360Parser(root, extract_deleted, extract_source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("360云盘")
    return pr
