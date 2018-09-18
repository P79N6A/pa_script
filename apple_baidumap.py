#coding=utf-8
import os
import PA_runtime
import datetime
import time
import json
import math
from PA_runtime import *
import pickle
import clr
try:
    clr.AddReference('model_map')
except:
    pass
del clr
import model_map

APPVERSION = "2.0"

class baiduMapParser(object):

    def __init__(self, node, extract_deleted, extract_source):
        self.root = node
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.cache = ds.OpenCachePath("baiduMap")
        self.baidumap =model_map.Map()
        
    def parse(self):

        path = self.cache + "/baidu_db_1.0.db"
        if self.check_to_update(path, APPVERSION):
            self.baidumap.db_create(path)
            self.account_info()
            self.analyze_search_history()
            self.my_history_address()

        result = model_map.Genetate(path)
        tmpresult = result.get_models()

        self.baidumap.db_close()

        return tmpresult


    def check_to_update(self, path_db, appversion):
        if os.path.exists(path_db) and path_db[-6:-3] == appversion:
            return False
        else:
            return True

    def account_info(self):
        """
        分析百度地图账号信息
        """
        account = model_map.Account()
        account.source = "百度地图:"
        accountNode = self.root.GetByPath("Library/Preferences/com.baidu.map.plist")
        if accountNode is None:
            return 
        else:
            bplist = BPReader.GetTree(accountNode)
            account.sourceFile = accountNode.AbsolutePath
            if bplist == None:
                return
            if bplist["sapi_displayname"]:
                name = bplist["sapi_displayname"].Value
                account.username = name
            if bplist["HEADIMG_URL"]:
                HEADIMG_URL = bplist["HEADIMG_URL"].Value
                account.photo = HEADIMG_URL

            # city id
            city_id = bplist["adsCityId"].Value if bplist["adsCityId"] else None
            account.city = str(city_id)
            # first visit date
            if  "BaiduMobStatTrace" in bplist:
                if "first_visit" in bplist["BaiduMobStatTrace"]:
                    tmpdate1 = bplist["BaiduMobStatTrace"]["first_visit"].Value
                    f_tmp = str(tmpdate1)[:-3]
                    firstVisitDate = TimeStamp.FromUnixTime(int(f_tmp))
                    account.install_time = int(f_tmp)

            # last visit date
            if "BaiduMobStatTrace" in bplist:
                if "last_visit" in bplist["BaiduMobStatTrace"]:
                    tmpdate2 = bplist["BaiduMobStatTrace"]["last_visit"].Value
                    l_tmp = str(int(tmpdate2))[:-3]
                    lastVisitDate = TimeStamp.FromUnixTime(int(l_tmp))
                    account.last_login_time = int(l_tmp)

            # get counts of recent visit
            if "BaiduMobStatTrace" in  bplist:
                if "recent" in bplist["BaiduMobStatTrace"]:
                    recent_array = bplist["BaiduMobStatTrace"]["recent"]
                    dicts = {}
                    for i in recent_array:
                        rdate = i["day"].Value
                        rdatestr = str(rdate)
                        # int to TimeStamp
                        year = int(rdatestr[:4])
                        month = int(rdatestr[4:6])
                        day = int(rdatestr[6:])
                        ts = time.strptime('{0}-{1}-{2}'.format(year,month,day), "%Y-%m-%d")
                        rdate = int(time.mktime(ts))
                        #rdate = TimeStamp(DateTimeOffset(year, month, day,0,0,0,TimeSpan(0)))
                        rcount = i["count"].Value
                        dicts[rdate] = rcount
                    account.recent_visit = pickle.dumps(dicts)
            try:
                self.baidumap.db_insert_table_account(account)
            except Exception as e:
                pass
            self.baidumap.db_commit()   

    def analyze_search_history(self):
        """
        分析搜索历史记录
        """
        dicts = defaultdict(lambda: 'None')
        historyNode = self.root.GetByPath("Documents/his_record.sdb")
        try:
            db = SQLiteParser.Database.FromNode(historyNode)
            if db is None:
                return []
            tb = SQLiteParser.TableSignature('his_record')
            if self.extract_deleted:
                SQLiteParser.Tools.AddSignatureToTable(tb, "key", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                SQLiteParser.Tools.AddSignatureToTable(tb, "value", SQLiteParser.FieldType.Blob, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(tb, self.extract_deleted, True):
                if canceller.IsCancellationRequested:
                    return
                search = model_map.Search()
                if rec.Deleted == DeletedState.Intact:
                    search.deleted = 0
                elif rec.Deleted == DeletedState.Deleted:
                    search.deleted = 1
                search.source = "百度地图:"
                search.sourceFile = historyNode.AbsolutePath
                if "key" in rec:
                    seach_history = rec["key"].Value
                # blob数据类型
                if "value" in rec and (not rec["value"].IsDBNull):
                    x = rec["value"].Value
                    try:
                        b = bytes(x)
                        jsonfile = b.decode("utf-16")
                        dicts = json.loads(jsonfile)
                        search_name = dicts.get("historyMainTitleKey") if dicts.get("historyMainTitleKey") else dicts.get("poiHisValue")
                        search.keyword = search_name
                        search.create_time = dicts.get("addtimesec")
                    except Exception as e:
                        pass
                try:
                    self.baidumap.db_insert_table_search(search)
                except Exception as e:
                    pass
        except Exception as e:
            print(e)
        
        self.baidumap.db_commit()


    # def analyze_ususal_address(self):
    #     """
    #     分析常用地址和家,公司信息
    #     """
    #     ususalAddressNode = self.root.GetByPath("Documents/homeSchool_poi.sdb")
    #     home_company_addressNode = self.root.GetByPath("Documents/aime/udc.db")
    #     try:
    #         db = SQLiteParser.Database.FromNode(ususalAddressNode)
    #         if db is None:
    #             return []
    #         tb = SQLiteParser.TableSignature('homeSchool_poi')
    #         if self.extract_deleted:
    #             pass
    #         for rec in db.ReadTableRecords(tb, self.extract_deleted, True):
    #             seach_history = rec["key"].Value
    #             seach_info = rec["value"].Value
    #     except Exception as e:
    #         pass


    #     db2 = SQLiteParser.Database.FromNode(home_company_addressNode)
    #     if db2 is None:
    #         return []
    #     tb2 = SQLiteParser.TableSignature("sync")
    #     if self.extract_deleted:
    #         pass
    #     for rec2 in db2.ReadTableRecords(tb2, self.extract_deleted, True):
    #         if rec2["key"].Value == "company":
    #             company = rec2["content"]
    #         if rec2["key"].Value == "home":
    #             home = rec2["content"]  


    def my_history_address(self):
        """
        导航记录
        """      
        hsAddressNode = self.root.GetByPath("Documents/routeHis_record.sdb")
        try:
            db = SQLiteParser.Database.FromNode(hsAddressNode)
            if db is None:
                return 
            tb = SQLiteParser.TableSignature('routeHis_record')
            if self.extract_deleted:
                SQLiteParser.Tools.AddSignatureToTable(tb, "value", SQLiteParser.FieldType.Blob, SQLiteParser.FieldConstraints.NotNull)
            
            for rec in db.ReadTableRecords(tb, self.extract_deleted, True):
                if canceller.IsCancellationRequested:
                    return
                routeaddr = model_map.Address() 
                if rec.Deleted == DeletedState.Deleted:
                    routeaddr.deleted = 1
                routeaddr.source = "百度地图:"
                routeaddr.sourceFile = hsAddressNode.AbsolutePath
                seach_history = rec["key"].Value
                if "value" in rec and (not rec["value"].IsDBNull):
                    seach_info = rec["value"].Value
                    try:
                        b = bytes(seach_info)
                        jsonflie = b.decode('utf-16')
                        dicts= json.loads(jsonflie)
                        search_time = dicts.get("addtimesec")
                        routeaddr.create_time = search_time

                        from_name = dicts.get("sfavnode").get("name")
                        from_geoptx = dicts.get("sfavnode").get("geoptx")
                        from_geopty = dicts.get("sfavnode").get("geopty")
                        routeaddr.from_name = from_name
                        routeaddr.from_posX = from_geoptx
                        routeaddr.from_posY = from_geopty

                        to_name = dicts.get("efavnode").get("name")
                        to_geoptx = dicts.get("efavnode").get("geoptx")
                        to_geopty = dicts.get("efavnode").get("geopty")
                        routeaddr.to_name = to_name
                        routeaddr.to_posX = to_geoptx
                        routeaddr.to_posY = to_geopty
                    except Exception as e:
                        pass
                try:
                    self.baidumap.db_insert_table_address(routeaddr)
                except Exception as e:
                    pass
        except Exception as e:
            print(e)
        
        self.baidumap.db_commit()


def analyze_baidumap(root, extract_deleted, extract_source):
    """
    baiduMap
    data source: ["Library/Preferences/com.baidu.map.plist", "Documents/his_record.sdb",  "Documents/routeHis_record.sdb"]
    search rules: ("com.baidu.map", analyze_baidumap, "BaiduMap", "百度地图", DescripCategories.BaiduMap)
    return: Account, SearchHistory, RouteRecord
    """
    pr = ParserResults()
    prResult = baiduMapParser(root, extract_deleted, extract_source).parse()
    if prResult:
        for i in prResult:
            pr.Models.Add(i)
    pr.Build("百度地图")
    return pr

def execute(node, extract_deleted):
    return analyze_baidumap(node, extract_deleted, False)
