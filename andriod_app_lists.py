# coding:utf-8

import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference('model_applists')
    clr.AddReference('bcp_other')
except:
    pass
del clr

import PA_runtime
import System
from System.IO import Path
from PA_runtime import *
from System.Data.SQLite import *
from System.Xml.XPath import Extensions as XPathExtensions
from System.Xml.Linq import *
from PA.InfraLib.Services import ServiceGetter, IApplicationService

import zipfile
import bcp_other
import pickle
import model_applists
import re
from collections import defaultdict
import os
appService = ServiceGetter.Get[IApplicationService]()
runPath = appService.RunPath
destDir = Path.Combine(runPath, "bin", "aapt.exe")


def GetString(reader, idx):
    return reader.GetString(idx) if not reader.IsDBNull(idx) else ""


class AppLists(object):

    def __init__(self, node, extract_Deleted, extract_Source):
        self.root = node
        self.extract_Deleted = extract_Deleted
        self.extract_Source = extract_Source
        self.cache = ds.OpenCachePath("应用列表")
        self.apps_db = model_applists.Apps()
        self.binds_set = set()

    def parse(self):
        icon_list = []
        app_lists = self.root.Children
        cache_db = self.cache + "\\appinfo.db"
        self.apps_db.db_create(cache_db)
        for app in app_lists:
            try:
                if app.Type == NodeType.Directory:
                    for _node in app.Children:
                        if _node.Name.endswith(".apk"):
                            base_apk_path = _node.PathWithMountPoint
                else:
                    base_apk_path = app.PathWithMountPoint
                tmp_path = " dump badging {0}".format(base_apk_path)
                file_content = os.popen(
                    '"{0}"'.format(destDir) + tmp_path).read()
                icon = self._get_app_data(file_content, base_apk_path)
                if icon:
                    icon_list.append(icon)
            except:
                pass

        other_icon = self.search_packages_xml()
        
        self.apps_db.db_commit()
        self.apps_db.db_close()

        results = model_applists.Generate(cache_db).get_models()
        results.extend(icon_list)
        results.extend(other_icon)
        tmp_dir = ds.OpenCachePath("tmp")
        PA_runtime.save_cache_path(
            bcp_other.BCP_OTHER_APP_INSTALLED, cache_db, tmp_dir)
        return results

    def search_packages_xml(self):
        res = []
        results = self.root.FileSystem.Search("packages.xml")
        if results:
            node = results[0]
            res.extend(self.parse_xml(node))
            self.apps_db.db_commit()
        return res
    
    def parse_xml(self, node):
        icon_list = []
        data = XElement.Load(node.Data)
        if data is None:
            return
        if str(data.Name) == "packages":
            if data.Elements("package"):
                for package in data.Elements("package"):
                    NEED_RUN = True
                    app_info = model_applists.Info()
                    dicts = defaultdict(list)
                    name = package.Attribute("name").Value
                    code_path = package.Attribute("codePath").Value
                    install_time = self._format_time(package.Attribute("it").Value)
                    update_time = self._format_time(package.Attribute("ut").Value)
                    if name in self.binds_set:
                        continue
                    app_info.name = name
                    app_info.bind_id = name
                    app_info.installedPath = code_path
                    app_info.purchaseDate = install_time
                    app_info.deletedDate = update_time

                    path = os.path.join(ds.FileSystem.MountPoint, code_path)
                    if os.path.isdir(path):
                        _paths = os.listdir(path)
                        for files in _paths:
                            if os.path.isfile(os.path.join(path, files)) and files.endswith(".apk"):
                                base_apk_path = os.path.join(path, files)
                                tmp_path = " dump badging {0}".format(base_apk_path)
                                file_content = os.popen(
                                    '"{0}"'.format(destDir) + tmp_path).read()
                                if install_time and update_time:
                                    time_data = [install_time, update_time]
                                else:
                                    time_data = []
                                icon = self._get_app_data(file_content, base_apk_path, time_data)
                                NEED_RUN = False
                                if icon:
                                    icon_list.append(icon)                         
                    if not NEED_RUN:
                        continue
                    perm = package.Element("perms")
                    if perm is None:
                        if app_info.bind_id:
                            self.apps_db.db_insert_table_applists(app_info)
                        continue
                    perm_list = perm.Elements("item")
                    for item in perm_list:
                        name = item.Attribute("name").Value
                        granted = item.Attribute("granted").Value
                        if granted and name and granted == "true":
                            dicts["permission"].append(name)
                    app_info.permission = pickle.dumps(dicts["permission"])
                    if app_info.bind_id:
                        self.apps_db.db_insert_table_applists(app_info)
        return icon_list

    def _get_app_data(self, file_content, base_apk_path, times=[]):
        icon_path = None
        icon_id = None
        icon = KeyValueModel()
        dicts = defaultdict(list)
        if not file_content:
            return
        app_info = model_applists.Info()
        if len(times) == 2:
            app_info.purchaseDate, app_info.deletedDate = times
        app_info.sourceFile = base_apk_path
        app_info.installedPath = base_apk_path
        content_list = file_content.split("\n")
        for line in content_list:
            if line.find("package") != -1:
                reg = re.compile("package:.*name='(.*?)'.*versionName='(.*?)'")
                results = re.match(reg, line)
                if results:
                    try:
                        bind_id, version = results.groups()
                        app_info.bind_id = bind_id
                        self.binds_set.add(bind_id)
                        app_info.version = version
                        icon.Key.Value = bind_id
                    except Exception as e:
                        print(e)

            elif line.find("uses-permission") != -1:
                reg = re.compile(".*name='(.*?)'")
                results = re.match(reg, line)
                if results:
                    try:
                        name = results.group(1)
                        dicts["permission"].append(name)
                    except Exception as e:
                        print(e)

            elif line.find("application-label") != -1:
                reg = re.compile("application-label:'(.*?)'")
                results = re.match(reg, line)
                if results:
                    try:
                        name = results.group(1)
                        app_info.name = name
                    except Exception as e:
                        print(e)

            elif line.find("application-icon-160:") != -1:
                reg = re.compile("application-icon-160:'(.*?)'")
                results = re.match(reg, line)
                if results:
                    try:
                        name = results.group(1)
                        if os.path.isfile(base_apk_path):
                            with zipfile.ZipFile(base_apk_path) as apk:
                                if '{0}'.format(name) in apk.namelist():
                                    export_path = self.cache + "\\" + icon.Key.Value
                                    byte_icon = apk.extract(
                                        '{0}'.format(name), export_path)
                                    icon.Value.Value = export_path + "\\" + name
                    except Exception as e:
                        print(e)

            elif line.find("application-icon-160:") != -1:
                reg = re.compile("application-icon-160:'(.*?)'")
                results = re.match(reg, line)
                if results:
                    try:
                        name = results.group(1)
                        if os.path.isfile(base_apk_path):
                            with zipfile.ZipFile(base_apk_path) as apk:
                                if '{0}'.format(name) in apk.namelist():
                                    export_path = self.cache + "\\" + icon.Key.Value
                                    byte_icon = apk.extract(
                                        '{0}'.format(name), export_path)
                                    icon.Value.Value = export_path + "\\" + name
                    except Exception as e:
                        print(e)

            elif line.find("application-icon-240:") != -1:
                if icon.Value.Value == None:
                    reg = re.compile("application-icon-240:'(.*?)'")
                    results = re.match(reg, line)
                    if results:
                        try:
                            name = results.group(1)
                            if os.path.isfile(base_apk_path):
                                with zipfile.ZipFile(base_apk_path) as apk:
                                    if '{0}'.format(name) in apk.namelist():
                                        export_path = self.cache + "\\" + icon.Key.Value
                                        byte_icon = apk.extract(
                                            '{0}'.format(name), export_path)
                                        icon.Value.Value = export_path + "\\" + name
                        except Exception as e:
                            print(e)

            elif line.find("application-icon-320:") != -1:
                if icon.Value.Value == None:
                    reg = re.compile("application-icon-320:'(.*?)'")
                    results = re.match(reg, line)
                    if results:
                        try:
                            name = results.group(1)
                            if os.path.isfile(base_apk_path):
                                with zipfile.ZipFile(base_apk_path) as apk:
                                    if '{0}'.format(name) in apk.namelist():
                                        export_path = self.cache + "\\" + icon.Key.Value
                                        byte_icon = apk.extract(
                                            '{0}'.format(name), export_path)
                                        icon.Value.Value = export_path + "\\" + name
                        except Exception as e:
                            print(e)

            elif line.find("application-icon-480:") != -1:
                if icon.Value.Value == None:
                    reg = re.compile("application-icon-480:'(.*?)'")
                    results = re.match(reg, line)
                    if results:
                        try:
                            name = results.group(1)
                            if os.path.isfile(base_apk_path):
                                with zipfile.ZipFile(base_apk_path) as apk:
                                    if '{0}'.format(name) in apk.namelist():
                                        export_path = self.cache + "\\" + icon.Key.Value
                                        byte_icon = apk.extract(
                                            '{0}'.format(name), export_path)
                                        icon.Value.Value = export_path + "\\" + name
                        except Exception as e:
                            print(e)

            if "permission" in dicts:
                app_info.permission = pickle.dumps(dicts["permission"])
        if app_info.bind_id:
            self.apps_db.db_insert_table_applists(app_info)
        if icon.Key.Value and icon.Value.Value:
            return icon
        return

    @staticmethod
    def _format_time(string_num):
        timestamp = str(int(string_num.upper(), 16))
        if len(str(timestamp)) == 13:
            timestamp = int(str(timestamp)[0:10])
        elif len(str(timestamp)) != 13 and len(str(timestamp)) != 10:
            timestamp = 0
        elif len(str(timestamp)) == 10:
            timestamp = timestamp
        return timestamp

    def other_parse(self):
        path = self.root.PathWithMountPoint
        cache_db = self.cache + "\\appinfo.db"
        self.apps_db.db_create(cache_db)
        try:
            db = SQLiteParser.Database.FromNode(self.root, canceller)
            if db is None:
                return
            tb = SQLiteParser.TableSignature("apps")
            for rec in db.ReadTableRecords(tb, self.extract_Deleted, True):
                if canceller.IsCancellationRequested:
                    return
                app_info = model_applists.Info()
                app_info.sourceFile = self.root.AbsolutePath
                if "appName" in rec and (not rec["appName"].IsDBNull):
                    app_info.name = rec["appName"].Value
                if "drawable" in rec and (not rec["drawable"].IsDBNull):
                    app_info.imgUrl = rec["drawable"].Value
                if "packageName" in rec and (not rec["packageName"].IsDBNull):
                    app_info.bind_id = rec["packageName"].Value
                if "versionName" in rec and (not rec["versionName"].IsDBNull):
                    app_info.version = rec["versionName"].Value
                if "permissions" in rec and (not rec["permissions"].IsDBNull):
                    app_info.permission = pickle.dumps(
                        rec["permissions"].Value)
                if rec.Deleted == DeletedState.Deleted:
                    app_info.deleted = 1
                if app_info.name or app_info.bind_id or app_info.version or app_info.permission:
                    self.apps_db.db_insert_table_applists(app_info)
        except Exception as e:
            print(e)

        self.apps_db.db_commit()
        self.apps_db.db_close()

        results = model_applists.Generate(cache_db).get_models()
        tmp_dir = ds.OpenCachePath("tmp")
        PA_runtime.save_cache_path(
            bcp_other.BCP_OTHER_APP_INSTALLED, cache_db, tmp_dir)
        return results


def analyze_app_lists(node, extract_Deleted, extract_Source):
    pr = ParserResults()
    if "appinfo" in node.PathWithMountPoint:
        results = AppLists(node, extract_Deleted, extract_Source).other_parse()
    else:
        results = AppLists(node, extract_Deleted, extract_Source).parse()
    print(len(results))
    if results:
        pr.Models.AddRange(results)
        pr.Build("应用列表")
    return pr
