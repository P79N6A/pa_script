#coding:utf-8
import PA_runtime
from PA_runtime import *
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
import os
from collections import defaultdict
import re
import model_applists
import pickle
import System
from System.Xml.Linq import *
from System.Xml.XPath import Extensions as XPathExtensions
from System.Data.SQLite import *
from PA.InfraLib.Services import ServiceGetter,IApplicationService
from System.IO import Path
import bcp_other
import zipfile

appService = ServiceGetter.Get[IApplicationService]()
runPath = appService.RunPath
destDir = Path.Combine(runPath,"bin","aapt.exe")

def GetString(reader, idx):
    return reader.GetString(idx) if not reader.IsDBNull(idx) else ""


class AppLists(object):

    def __init__(self, node, extract_Deleted, extract_Source):
        self.root = node
        self.extract_Deleted = extract_Deleted
        self.extract_Source = extract_Source
        self.cache = ds.OpenCachePath("应用列表")   
        self.apps_db = model_applists.Apps()

    def parse(self):
        icon_list = []
        app_lists = self.root.Children
        cache_db = self.cache + "\\appinfo.db"
        self.apps_db.db_create(cache_db)
        for app in app_lists:
            icon_path = None
            icon_id = None
            icon = KeyValueModel()
            dicts = defaultdict(list)
            try:
                if app.Type == NodeType.Directory:
                    base_apk_path = app.PathWithMountPoint + "\\base.apk"
                else:
                    base_apk_path = app.PathWithMountPoint
                tmp_path = " dump badging {0}".format(base_apk_path)
                file_content = os.popen('"{0}"'.format(destDir) + tmp_path).read()
                if file_content:
                    app_info = model_applists.Info()
                    app_info.sourceFile = app.AbsolutePath + "/base.apk"
                    app_info.installedPath = app.AbsolutePath + "/base.apk"
                    content_list = file_content.split("\n")
                    for line in content_list:
                        if line.find("package") != -1:
                            reg = re.compile("package:.*name='(.*?)'.*versionName='(.*?)'")
                            results = re.match(reg, line)
                            if results:
                                try:
                                    bind_id ,version = results.groups()
                                    app_info.bind_id = bind_id
                                    app_info.version = version
                                    icon.Key.Value = bind_id
                                except Exception as e:
                                    print(e)
                        
                        elif line.find("uses-permission")!= -1:
                            reg = re.compile(".*name='(.*?)'")
                            results = re.match(reg, line)
                            if results:
                                try:
                                    name= results.group(1)
                                    dicts["permission"].append(name)
                                except Exception as e:
                                    print(e)
                        
                        elif line.find("application-label")!= -1:
                            reg = re.compile("application-label:'(.*?)'")
                            results = re.match(reg, line)
                            if results:
                                try:
                                    name= results.group(1)
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
                                                byte_icon = apk.extract('{0}'.format(name),export_path)
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
                                                byte_icon = apk.extract('{0}'.format(name),export_path)
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
                                                    byte_icon = apk.extract('{0}'.format(name),export_path)
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
                                                    byte_icon = apk.extract('{0}'.format(name),export_path)
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
                                                    byte_icon = apk.extract('{0}'.format(name),export_path)
                                                    icon.Value.Value = export_path + "\\" + name
                                    except Exception as e:
                                        print(e)

                        if "permission" in dicts:
                            app_info.permission = pickle.dumps(dicts["permission"])
                    if app_info.bind_id:
                        self.apps_db.db_insert_table_applists(app_info)
                    if icon.Key.Value and icon.Value.Value:
                        icon_list.append(icon)
            except:
                pass

        self.apps_db.db_commit()
        self.apps_db.db_close()

        results = model_applists.Generate(cache_db).get_models()
        results.extend(icon_list)
        tmp_dir = ds.OpenCachePath("tmp")
        PA_runtime.save_cache_path(bcp_other.BCP_OTHER_APP_INSTALLED, cache_db, tmp_dir)
        return results

    
    def other_parse(self):
        path = self.root.PathWithMountPoint
        cache_db = self.cache + "\\appinfo.db"
        self.apps_db.db_create(cache_db)
        try:
            db =SQLiteParser.Database.FromNode(self.root, canceller)
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
                    app_info.permission = pickle.dumps(rec["permissions"].Value)
                if rec.Deleted == DeletedState.Deleted:
                    app_info.deleted = 1
                if app_info.name or app_info.bind_id or app_info.version or app_info.permission:
                    self.apps_db.db_insert_table_applists(app_info)
        except Exception as e:
            print(e)
        # c# read database        
        # connection = System.Data.SQLite.SQLiteConnection('Data Source = {0}'.format(path))
        # try:
        #     connection.Open()
        #     cmd = System.Data.SQLite.SQLiteCommand(connection)
        #     cmd.CommandText = '''
        #         select appName, packageName, versionName, permissions from apps
        #     '''
        #     reader = cmd.ExecuteReader()
        #     while reader.Read():
        #         app_info = model_applists.Info()
        #         app_info.sourceFile = self.root.AbsolutePath
        #         app_info.name = GetString(reader, 0)
        #         app_info.bind_id = GetString(reader, 1)
        #         app_info.version = GetString(reader, 2)
        #         app_info.permission = pickle.dumps(GetString(reader, 3))
        #         self.apps_db.db_insert_table_applists(app_info)
        # except Exception as e:
        #     print(e)

        self.apps_db.db_commit()
        self.apps_db.db_close()

        # if connection != None:
        #     connection.Close()

        results = model_applists.Generate(cache_db).get_models()
        tmp_dir = ds.OpenCachePath("tmp")
        PA_runtime.save_cache_path(bcp_other.BCP_OTHER_APP_INSTALLED, cache_db, tmp_dir)
        return results


def analyze_app_lists(node, extract_Deleted, extract_Source):
    pr = ParserResults()
    if "appinfo" in node.PathWithMountPoint:
        results =  AppLists(node, extract_Deleted, extract_Source).other_parse()
    else:
        results =  AppLists(node, extract_Deleted, extract_Source).parse()
    print(len(results))
    if results:
        pr.Models.AddRange(results)
        pr.Build("应用列表")
    return pr
