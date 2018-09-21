# -*- coding: utf-8 -*-
import PA_runtime
import json
from PA_runtime import *
import re
import os
import clr
try:
    clr.AddReference('model_map')
except:
    pass
del clr
import model_map


class BaiduMap(object):

    def __init__(self, node, extract_Deleted, extract_Source):
        self.root = node
        self.extract_Deleted = extract_Deleted
        self.extract_Source = extract_Source
        self.baidudb = model_map.Map()
        self.cache = ds.OpenCachePath("baiduMap")  

    def parse_favorites_poi(self):
        if self.root is None:
            return
        try:
            db = SQLiteParser.Database.FromNode(self.root)
            if db is None:
                return
            tbs = SQLiteParser.TableSignature("fav_poi_main")
            if tbs is None:
                return
            if self.extract_Deleted:
                SQLiteParser.Tools.AddSignatureToTable(tbs, "ext_name", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                SQLiteParser.Tools.AddSignatureToTable(tbs, "uid", SQLiteParser.FieldType.Int, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(tbs, self.extract_Deleted, True):
                if canceller.IsCancellationRequested:
                    return
                fav_poi = model_map.Search()
                fav_poi.source = "百度地图"
                fav_poi.sourceApp = "百度地图"
                fav_poi.sourceFile = self.root.AbsolutePath
                fav_poi.item_type = 1 # 表明这是收藏的点
                if rec.Deleted == DeletedState.Deleted:
                    fav_poi.deleted = 1
                if "ext_name" in rec:
                    fav_poi.keyword = rec["ext_name"].Value
                if "addr" in rec and (not rec["addr"].IsDBNull):
                    fav_poi.address = rec["addr"].Value
                if "city_id" in rec and (not rec["city_id"].IsDBNull):
                    fav_poi.adcode = rec["city_id"].Value
                if "ext_geoptx" in rec and (not rec["ext_geoptx"].IsDBNull):
                    fav_poi.pos_x = int(rec["ext_geoptx"].Value)
                if "ext_geopty" in rec and (not rec["ext_geopty"].IsDBNull):
                    fav_poi.pos_y = int(rec["ext_geopty"].Value)
                if "ctime" in rec and (not rec["ctime"].IsDBNull):
                    fav_poi.create_time = rec["ctime"].Value
                try:
                    self.baidudb.db_insert_table_search(fav_poi)
                except Exception as e:
                    print(e)        
        except Exception as e:
            print("node is not exists")
        self.baidudb.db_commit()
        

    def parse_search(self):
        search_node = self.root.Parent.Parent.GetByPath("files/poi_his.sdb")
        if search_node is None:
            return
        try:
            db = SQLiteParser.Database.FromNode(search_node)
            if db is None:
                return 
            tbs = SQLiteParser.TableSignature("poi_his")
            if self.extract_Deleted:
                SQLiteParser.Tools.AddSignatureToTable(tbs, "key", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                SQLiteParser.Tools.AddSignatureToTable(tbs, "value", SQLiteParser.FieldType.Blob, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(tbs,self.extract_Deleted, True):
                if canceller.IsCancellationRequested:
                    return
                search_history = model_map.Search()
                search_history.source = "百度地图"
                search_history.sourceApp = "百度地图"
                search_history.sourceFile = search_node.AbsolutePath
                if rec.Deleted == DeletedState.Deleted:
                    search_history.deleted = 1
                if "value" in rec and (not rec["value"].IsDBNull):
                    tmp = rec["value"].Value
                    b = bytes(tmp)
                    json_data = json.loads(b.decode("utf-16", errors='ignore'))
                    search_history.keyword = json_data["Fav_Content"]
                    if "Fav_Extra" in json_data and (json["Fav_Extra"] is not None):
                        search_history.district = json["Fav_Extra"]
                    if "Fav_Sync" in json_data:
                        if "cityId" in json_data["Fav_Sync"]:
                            search_history.adcode = json_data["Fav_Sync"]["cityId"]
                        if "addtimesec" in json_data["Fav_Sync"]:
                            search_history.create_time = int(json_data["Fav_Sync"]["addtimesec"])
                    try:
                        self.baidudb.db_insert_table_search(search_history)
                    except Exception as e:
                        pass        
        except Exception as e:
            pass
        self.baidudb.db_commit()

    def parse_route(self):
        route_node = self.root.Parent.Parent.GetByPath("files/route_his.sdb")
        if route_node is None:
            return
        try:
            db = SQLiteParser.Database.FromNode(route_node)
            if db is None:
                return
            tbs = SQLiteParser.TableSignature("route_his")
            if self.extract_Deleted:
                SQLiteParser.Tools.AddSignatureToTable(tbs, "key", SQLiteParser.FieldType.NotNull, SQLiteParser.FieldConstraints.NotNull)
                SQLiteParser.Tools.AddSignatureToTable(tbs, "value", SQLiteParser.FieldType.Blob, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(tbs, self.extract_Deleted, True):
                if canceller.IsCancellationRequested:
                    return
                if "value" in rec and (rec["value"] is not None):
                    addr =  model_map.Address()
                    if rec.Deleted == DeletedState.Deleted:
                        addr.deleted = 1
                    addr.source = "百度地图"
                    addr.sourceApp = "百度地图"
                    addr.sourceFile = route_node.AbsolutePath
                    if "value" in rec and (not rec["value"].IsDBNull):
                        data = None
                        tmp = rec["value"].Value
                        try:
                            b = bytes(tmp)
                            jsonfile = b.decode("utf-8",errors='ignore')
                            # data = json.loads(jsonfile)
                        except Exception as e:
                            pass
                        try:
                            data = json.dumps(jsonfile)
                        except Exception as e:
                            continue
                        
                        if "Fav_Content" in data:
                            strings = data.get("Fav_Content")
                            content = json.loads(strings)
                            if "Fav_Sync" in data:
                                addr.create_time = int(data["addtimesec"])
                            if "sfavnode" in content:
                                if "name" in content["sfavnode"]:
                                    addr.from_name = content["sfavnode"]["name"]
                                if "geoptx" in content["sfavnode"]:
                                    addr.from_posX = content["sfavnode"]["geoptx"]
                                if "geopty" in content["sfavnode"]:
                                    addr.from_posY = content["sfavnode"]["geopty"]
                            if "efavnode" in content:
                                if "name" in content["efavnode"]:
                                    addr.from_name = content["efavnode"]["name"]
                                if "geoptx" in content["efavnode"]:
                                    addr.from_posX = content["efavnode"]["geoptx"]
                                if "geopty" in content["efavnode"]:
                                    addr.from_posY = content["efavnode"]["geopty"]                  
                    try:
                        if addr.from_name or addr.from_posX or addr.from_posY or addr.from_addr:
                            self.baidudb.db_insert_table_address(addr)
                    except Exception as e:
                        print(e)           
        except Exception as e:
            print(e)
        self.baidudb.db_commit()



    def parse(self):
        
        db_path = self.cache + "/baidu_db.db"
        self.baidudb.db_create(db_path)
        self.parse_favorites_poi()
        self.parse_search()
        self.parse_route()
        self.baidudb.db_close()
        
        generate = model_map.Genetate(db_path)   
        tmpresult = generate.get_models()
        return tmpresult 

def analyze_baidumap(node, extract_Deleted, extract_Source):
        pr = ParserResults()
        results =  BaiduMap(node, extract_Deleted, extract_Source).parse()
        if results:
            for i in results:
                pr.Models.Add(i)
        pr.Build("百度地图")
        return pr

def execute(node, extract_deleted):
    return analyze_baidumap(node, extract_deleted, False)
