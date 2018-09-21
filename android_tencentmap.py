#coding=utf-8
import PA_runtime
import json
from PA_runtime import *
import os
import clr
try:
    clr.AddReference('model_map')
except:
    pass
del clr
import model_map


class TencentMap(object):

    def __init__(self, node, extract_Deleted, extract_Source):
        self.root = node
        self.extract_Deleted = extract_Deleted
        self.extract_Source = extract_Source
        self.tencentdb = model_map.Map()
        self.cache = ds.OpenCachePath("tencentMap")
        
    def parse_route(self):
        """
        导航记录
        """
        try:
            db = SQLiteParser.Database.FromNode(self.root)
            if db is None:
                return
            tbs = SQLiteParser.TableSignature("route_search_history_tab")
            if self.extract_Deleted:
                SQLiteParser.Tools.AddSignatureToTable(tbs, "_keyword", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                SQLiteParser.Tools.AddSignatureToTable(tbs, "from_data", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                SQLiteParser.Tools.AddSignatureToTable(tbs, "end_data", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(tbs, self.extract_Deleted, True):
                if canceller.IsCancellationRequested:
                    return
                route_addr = model_map.Address()
                route_addr.source = "腾讯地图"
                route_addr.sourceApp = "腾讯地图"
                if rec.Deleted == DeletedState.Deleted:
                    route_addr.deleted = 1
                route_addr.sourceFile = self.root.AbsolutePath
                if "_lasted_used" in rec:
                    tmp = rec["_lasted_used"].Value
                    if len(str(tmp)) >= 10:
                        tmpb = str(tmp).strip()[:10]    #转成unix时间
                        route_addr.create_time = int(tmpb)
                if (not rec["from_data"].IsDBNull) and (not rec["end_data"].IsDBNull):
                    try:
                        from_data = json.loads(rec["from_data"].Value)
                    except Exception as e:
                        pass
                    if "name" in from_data:
                        route_addr.from_name = from_data["name"]
                    if "addr" in from_data:
                        route_addr.from_addr = from_data["addr"]
                    if "lon" in from_data:
                        route_addr.from_posX = from_data["lon"]
                    if "lat" in from_data:
                        route_addr.from_posY = from_data["lat"]
                    try:
                        end_data = json.loads(rec["end_data"].Value)
                    except Exception as e:
                        pass
                    if "name" in end_data:
                        route_addr.to_name = end_data["name"]
                    if "addr" in end_data:
                        route_addr.to_addr = end_data["addr"]
                    if "lon" in end_data:
                        route_addr.to_posX = end_data["lon"]
                    if "lat" in end_data:
                        route_addr.to_posY = end_data["lat"]
                    try:
                        self.tencentdb.db_insert_table_address(route_addr)
                    except Exception as e:
                        pass
        except Exception as e:
            print(e)
        self.tencentdb.db_commit()                        

    def parse_search(self):
        """
        搜索记录
        """
        search_node = self.root.Parent.GetByPath("poi_search_history.db")
        try:
            db = SQLiteParser.Database.FromNode(search_node)
            if db is None:
                return
            tbs = SQLiteParser.TableSignature("t_poi_search_history")
            if self.extract_Deleted:
                SQLiteParser.Tools.AddSignatureToTable(tbs, "_keyword", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(tbs, self.extract_Deleted, True):
                if canceller.IsCancellationRequested:
                    return
                search = model_map.Search()
                search.source = "腾讯地图"
                search.sourceApp = "腾讯地图"
                search.sourceFile = search_node.AbsolutePath
                if rec.Deleted == DeletedState.Deleted:
                    search.deleted = 1
                if "_keyword" in rec:
                    search.keyword = rec["_keyword"].Value
                if "_data" in rec and (not rec["_data"].IsDBNull):
                    tmp = rec["_data"].Value
                    try:
                        json_data = json.loads(tmp)
                    except Exception as e:
                        pass
                    if "address" in json_data:
                        search.address = json_data["address"]
                    if "latLng" in json_data:
                        if "a" in json_data["latLng"]:
                            search.pos_x = json_data["latLng"]["a"]
                        if "b" in json_data["latLng"]:
                            search.pos_y = json_data["latLng"]["b"]
                if "_last_used" in rec and (not rec["_last_used"].IsDBNull):
                    tmpa = rec["_last_used"].Value
                    tmpb = str(tmpa)[:10]
                    search.create_time = int(tmpb)
                try:
                    self.tencentdb.db_insert_table_search(search)
                except Exception as e:
                    pass
        except Exception as e:
            print(e)
        self.tencentdb.db_commit()

    def parse_fav_addr(self):
        fav_node = self.root.Parent.GetByPath("favorite_new.db")
        try:
            db = SQLiteParser.Database.FromNode(fav_node)
            if db is None:
                return
            tbs = SQLiteParser.TableSignature("favorite_poi_info")
            if self.extract_Deleted:
                SQLiteParser.Tools.AddSignatureToTable(tbs, "name", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(tbs, self.extract_Deleted, self.extract_Source):
                if canceller.IsCancellationRequested:
                    return
                fav_addr = model_map.Search()
                fav_addr.source = "腾讯地图"
                fav_addr.sourceApp = "腾讯地图"
                fav_addr.sourceFile = fav_node.AbsolutePath
                fav_addr.item_type = 1
                if rec.Deleted == DeletedState.Deleted:
                    fav_addr.deleted = 1
                if "name" in rec:
                    fav_addr.keyword = rec["name"].Value
                if "rawData" in rec and (not rec["rawData"].IsDBNull):
                    try:
                        data = json.loads(rec["rawData"])
                        if "addr" in data:
                            fav_addr.address = rec["addr"].Value
                        if "latLng" in data:
                            if data["latLng"]["b"] is not None:
                                fav_addr.pos_x = data["latLng"]["b"].Value
                            if data["latLng"]["a"] is not None:
                                fav_addr.pos_y = data["latLng"]["a"].Value
                    except Exception as e:
                        pass
                if "lastEditTime" in rec and (not rec["lastEditTime"].IsDBNull):
                    fav_addr.create_time = rec["lastEditTime"].Value
                try:
                    self.tencentdb.db_insert_table_search(fav_addr)
                except Exception as e:
                    pass       
        except Exception as e:
            pass
        self.tencentdb.db_commit()

    def check_to_update(self, path_db, appversion):
        if os.path.exists(path_db) and path_db[-6:-3] == appversion:
            return False
        else:
            return True

    def parse(self):
        db_path = self.cache + "/tencent_db.db"
        self.tencentdb.db_create(db_path)
        self.parse_route()
        self.parse_search()
        self.parse_fav_addr()
        self.tencentdb.db_close()
        
        generate = model_map.Genetate(db_path)   
        tmpresult = generate.get_models()
        return tmpresult        
            
def analyze_tencentmap(node, extract_Deleted, extract_Source):
    pr = ParserResults()
    results = TencentMap(node, extract_Deleted, extract_Source).parse()
    if results:
        for i in results:
            pr.Models.Add(i)
    pr.Build("腾讯地图")
    return pr
    
def execute(node, extract_deleted):
    return analyze_tencentmap(node, extract_deleted, False)