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
    __table__ = 'file_list'

    nid = Fields.CharField(column_name='nid')
    pid = Fields.CharField(column_name='pid')
    fname = Fields.CharField(column_name='fname')
    owner_qid = Fields.CharField(column_name='owner_qid')
    type = Fields.IntegerField(column_name='type')
    fhash = Fields.CharField(column_name='fhash')
    fctime = Fields.IntegerField(column_name='fctime')
    fmtime = Fields.IntegerField(column_name='fmtime')
    category = Fields.IntegerField(column_name='category')
    fsize = Fields.IntegerField(column_name='fsize')


class FileCache(DataModel):
    __table__ = 'file_cache'
    
    nid = Fields.CharField(column_name='nid')
    path = Fields.CharField(column_name='path')
    size = Fields.IntegerField(column_name='size')
    fhash = Fields.CharField(column_name='fhash')
    mtime = Fields.IntegerField(column_name='mtime')


class DownloadingFile(DataModel):

    __table__ = 'download_job'

    nid = Fields.CharField(column_name='nid')
    eid = Fields.CharField(column_name='eid')
    path = Fields.CharField(column_name='path')
    fsize = Fields.IntegerField(column_name='fsize')
    fhash = Fields.CharField(column_name='fhash')
    ctime = Fields.IntegerField(column_name='ctime')
    mtime = Fields.IntegerField(column_name='mtime')


class YunPan360Parser(ParserBase):

    def __init__(self, root, extract_deleted, extract_source):
        super(YunPan360Parser, self).__init__(
            self._get_root_node(root, times=3),
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
        account.account_id = os.path.basename(db.PathWithMountPoint).replace(".db", "")
        account.username = account.nickname = '360U' + str(account.account_id)
        account.insert_db(self.model_im_col)
        self.model_im_col.db_commit()
        return account

    def _parse_server_path(self, files, file_):
        if file_.type == 1:
            return '/'
        if file_.parent is None:
            return '/'
        path = []
        while file_.parent.nid != '0':
            path.insert(0, file_.parent.fname)
            file_ = file_.parent
        server_path = "/".join(path)
        return '/' + server_path

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
        root.fname = ''
        root.type = dir_type
        files[root.nid] = root

        for i in files.values():
            a = i.pid
            i.parent = files.get(i.pid, None)
        for i in files.values():
            i.server_path = self._parse_server_path(files, i) + '/' + i.fname
        for obj in files.values():
            try:
                if obj.type == dir_type:
                    continue
                f = model_nd.NDFileList()
                f.set_value_with_idx(f.account, account.account_id)
                f.set_value_with_idx(f.file_name, obj.fname)
                f.set_value_with_idx(f.file_hash, obj.fhash)
                f.set_value_with_idx(f.file_size, obj.fsize)
                f.set_value_with_idx(f.create_time, obj.fctime)
                f.set_value_with_idx(f.update_time, obj.fmtime)
                f.set_value_with_idx(f.file_type, obj.category)
                f.set_value_with_idx(f.server_path, obj.server_path)
                f.set_value_with_idx(f.deleted, obj.deleted)
                self.model_nd_col.db_insert_filelist(f.get_values())
            except Exception as e:
                self.logger.error()
        self.model_nd_col.db_commit()

    def _generate_download_table(self, db, account):
        if not db:
            return
        is_connected_1 = FileCache.connect(db)
        is_connected_2 = DownloadingFile.connect(db)
        is_connected_3 = FileList.connect(db)
        if not all((is_connected_1, is_connected_2, is_connected_3)):
            return

        files = {obj.nid: obj for obj in FileList.objects.all}

        for obj in FileCache.objects.all:
            try:
                t = model_nd.NDFileTransfer()
                t.set_value_with_idx(t.account, account.account_id)
                t.set_value_with_idx(t.deleted, obj.deleted)
                t.set_value_with_idx(t.server_path, obj.path)
                t.set_value_with_idx(t.file_size, obj.size)
                node = self._search_file(obj.path.split('/')[-1])
                if node is not None:
                    t.set_value_with_idx(t.local_path, node.AbsolutePath)
                t.set_value_with_idx(t.file_name, files[obj.nid].fname)
                t.set_value_with_idx(t.hash_code, obj.fhash)
                t.set_value_with_idx(t.is_download, model_nd.NDFileDone)
                self.model_nd_col.db_insert_transfer(t.get_values())
            except Exception as e:
                self.logger.error()

        for obj in DownloadingFile.objects.all:
            try:
                t = model_nd.NDFileTransfer()
                t.set_value_with_idx(t.account, account.account_id)
                t.set_value_with_idx(t.deleted, obj.deleted)
                t.set_value_with_idx(t.server_path, obj.path)
                t.set_value_with_idx(t.file_size, obj.fsize)
                node = self._search_file(obj.path.split('/')[-1])
                if node is not None:
                    t.set_value_with_idx(t.local_path, node.AbsolutePath)
                t.set_value_with_idx(t.file_name, files[obj.nid].fname)
                t.set_value_with_idx(t.hash_code, obj.fhash)
                t.set_value_with_idx(t.is_download, model_nd.NDFileProcessing)
                self.model_nd_col.db_insert_transfer(t.get_values())
            except Exception as e:
                self.logger.error()

        self.model_nd_col.db_commit()

    def _main(self):
        databases = self.root.GetByPath('/Library/databases')
        if not databases:
            return
        dbs = databases.Search(r'\d+.db')
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
        print(len(results))
        pr.Models.AddRange(results)
        pr.Build("360云盘")
    return pr
