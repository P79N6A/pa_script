#coding=utf-8
import PA_runtime
from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference('model_applists')
except:
    pass
del clr
from collections import defaultdict
import json
import re
import time
import datetime
import requests
import model_applists

class InstallationLog(object):

    def __init__(self, node, extract_Deleted, extract_Source):
        self.root = node
        self.extract_Deleted = extract_Deleted
        self.extract_Source = extract_Source
        self.apps_db = model_applists.Apps()
        self.cache = ds.OpenCachePath("applicationLog")
    
    def get_apps_info(self):
        node_lists = self.root.Files
        if node_lists is None:
            return
        dicts = defaultdict(list)
        for node in node_lists:
            with open(node.PathWithMountPoint, "r") as f:
                for line in f:
                    
                    if line.find("ID=") != -1:
                        reg = re.compile('(.*?).\[.*ID=(.*);.*Version=(.*?), ShortVersion=(.*?)>')
                        results = re.match(reg, line)
                        if results:
                            value = results.groups()
                            if value[2] != "(null)" or value[3] != "(null)":
                                dicts[value[1].strip()].append({"install":value})

                        
                    elif line.find("Uninstalling identifier") != -1:
                        reg = re.compile('(.*?).\[.*identifier(.*)')
                        results = re.match(reg, line)
                        if results:
                            value = results.groups()
                            dicts[value[1].strip()].append({"uninstalled":value})
        
        return dicts

    def convert_to_apps_model(self):
        dicts = self.get_apps_info()
        for item in dicts:
            app_info = model_applists.Info()
            app_info.repeated = 1
            app_imgs, app_desc, app_name = self._get_info_by_bundleid(item)
            time.sleep(2)
            install_info, unstalled_info = self._get_install_uninstall_time(dicts[item])
            app_info.bind_id = item
            if app_name:
                app_info.name = app_name
            if app_desc:
                app_info.description = app_desc
            if app_imgs:
                app_info.imgUrl = json.dumps(app_imgs)
            if install_info:
                app_info.purchaseDate = self._convert_to_unixtime(install_info[0])
                app_info.version = install_info[2]
            if unstalled_info:
                app_info.deletedDate = self._convert_to_unixtime(unstalled_info[0])

            self.apps_db.db_insert_table_applists(app_info)
        self.apps_db.db_commit()

    def parse(self):
        db_path = self.cache + "\\log.db"
        self.apps_db.db_create(db_path)
        self.convert_to_apps_model()
        self.apps_db.db_close()

        results = model_applists.Generate(db_path).get_models()
        return results

    @staticmethod
    def _convert_to_unixtime(value):
        try:
             unixtime = time.mktime(datetime.datetime.strptime(value, "%a %b %d %H:%M:%S %Y").timetuple())
             return unixtime
        except Exception as e:
            pass

    @staticmethod
    def _get_info_by_bundleid(value):
        try:
            header = {
                "Referer": "https://www.apple.com/itunes/",
                "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"
            }
            a = requests.get("http://itunes.apple.com/lookup?bundleId={0}".format(value),headers=header)
        except requests.exceptions.ConnectionError:
            print ("Connection refused")
            return
        if a.status_code == 200:
            try:
                response = json.loads(a.content.decode("utf-8"))
                imgs = None
                desc = None
                name = None
                if "results" in response and response["results"]:
                    results = response["results"][0]
                    if "screenshotUrls" in results:
                        imgs = results["screenshotUrls"]
                    if "description" in results:
                        desc = results["description"]
                    if "trackCensoredName" in results:
                        name = results["trackCensoredName"]
                    return imgs,desc,name
            except Exception as e:
                print(e)
        return None,None,None

    @staticmethod
    def _get_install_uninstall_time(value):
        try:
            install_time = None
            uninstall_time = None
            if len(value) == 1:
                if value[0].keys() == ["install"]:
                    install_time = value[0]["install"]
                elif value[0].keys() == ["uninstalled"]:
                    uninstall_time = value[0]["uninstalled"]
            if len(value) > 1:
                for i in value:
                    if i.keys() == ["install"]:
                        install_time = i["install"]
                        break
                for i in value[::-1]:
                    if i.keys() == ["uninstalled"]:
                        uninstall_time = i["uninstalled"]
                        break
            return install_time, uninstall_time
        except Exception as e:
            return None,None

def analyze_Installation_log(node, extract_Deleted, extract_Source):
    pr = ParserResults()
    results = InstallationLog(node, extract_Deleted, extract_Source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("应用安装卸载日志")
    return pr