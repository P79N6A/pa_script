#coding=utf-8

'''
The script is based on baidu map version 10.11.0, 
which is theoretically compatible with less than 10.11.0
'''

__author__ = "Xu Tao"
__date__ = "2019-1-24"
__maintainer__ = 'Xu Tao'


from PA_runtime import *
import PA_runtime
import clr
try:
    clr.AddReference('model_map_v2')
    clr.AddReference("MapUtil")
    clr.AddReference("bcp_gis")
except:
    pass
del clr
import model_map_v2 as model_map
import traceback
import MapUtil
import bcp_gis
import json


class baiduMapParser(object):

    def __init__(self, node, extract_deleted, extract_source):
        self.root = node
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.cache = ds.OpenCachePath("baiduMap")
        self.baidumap = model_map.Map()

    def parse(self):
        db_path = MapUtil.md5(self.cache, self.root.AbsolutePath)  # 唯一路径
        self.baidumap.db_create(db_path)
        self.get_search_history()
        self.get_navigation_record()
        self.get_fav_position()
        self.baidumap.db_close()

        tmp_dir = ds.OpenCachePath("tmp")
        PA_runtime.save_cache_path(bcp_gis.NETWORK_APP_MAP_BAIDU, db_path, tmp_dir)

        result = model_map.ExportModel(db_path).get_model()
        return result


    def get_search_history(self):
        """
        search history
        """
        search_node = self.root.Parent.Parent.GetByPath("files/poi_his.sdb")
        if search_node is None:
            return
        try:
            db = SQLiteParser.Database.FromNode(search_node, canceller)
            if db is None:
                return 
            if 'poi_his' not in db.Tables:
                return
            tbs = SQLiteParser.TableSignature("poi_his")
            for rec in db.ReadTableRecords(tbs,self.extract_deleted, True):
                if canceller.IsCancellationRequested:
                    return
                search_history = model_map.Search()
                search_history.sourceApp = "百度地图"
                search_history.sourceFile = search_node.AbsolutePath
                if rec.Deleted == DeletedState.Deleted:
                    search_history.deleted = 1
                if "value" in rec and (not rec["value"].IsDBNull):
                    try:
                        tmp = rec["value"].Value
                        b = bytes(tmp)
                        json_data = json.loads(b.decode("utf-16", errors='ignore'))
                    except Exception as e:
                        continue
                    if json_data:
                        if "Fav_Content" in json_data:
                             search_history.keyword = json_data["Fav_Content"]
                        if "Fav_Sync" in json_data:
                            if "addtimesec" in json_data["Fav_Sync"]:
                                search_history.create_time = int(json_data["Fav_Sync"]["addtimesec"])
                if search_history.keyword:    
                    self.baidumap.db_insert_table_search(search_history)
        except Exception as e:
            pass
        self.baidumap.db_commit()


    def get_navigation_record(self):
        """
        navgation history
        """
        route_node = self.root.Parent.Parent.GetByPath("files/route_his.sdb")
        if route_node is None:
            return
        try:
            db = SQLiteParser.Database.FromNode(route_node, canceller)
            if db is None:
                return
            if 'route_his' not in db.Tables:
                return
            tbs = SQLiteParser.TableSignature("route_his")
            for rec in db.ReadTableRecords(tbs, self.extract_deleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        return
                    
                    routeaddr = model_map.RouteRec()
                    if rec.Deleted == DeletedState.Deleted:
                        routeaddr.deleted = 1
                    routeaddr.sourceApp = "百度地图"
                    routeaddr.sourceFile = route_node.AbsolutePath
                    
                    # seach_history = rec["key"].Value
                    if "value" in rec and (not rec["value"].IsDBNull):
                        seach_info = rec["value"].Value
                        try:
                            b = bytes(seach_info)
                            jsonflie = b.decode('utf-16')
                            dicts = json.loads(jsonflie)
                            if "Fav_Sync" in dicts:
                                routeaddr.create_time = dicts["Fav_Sync"]["addtimesec"]
                            if "Fav_Content" in dicts:
                                data = json.loads(dicts["Fav_Content"])
                                routeaddr.from_name = data.get("sfavnode").get("name")
                                routeaddr.from_posX = data.get("sfavnode").get("geoptx")
                                routeaddr.from_posY = data.get("sfavnode").get("geopty")
                                routeaddr.to_name = data.get("efavnode").get("name")
                                routeaddr.to_posX = data.get("efavnode").get("geoptx")
                                routeaddr.to_posY = data.get("efavnode").get("geopty")
                            routeaddr.type = 6
                        except Exception as e:
                            TraceService.Trace(TraceLevel.Info,"Get model_map.RouteRec() Failed! -{0}".format(e))      
                            continue
                    if routeaddr.from_name:
                        self.baidumap.db_insert_table_routerec(routeaddr)
                except:
                    pass
        except Exception as e:
            TraceService.Trace(TraceLevel.Error,"Get model_map.RouteRec() Failed! -{0}".format(e)) 
        self.baidumap.db_commit()


    def get_fav_position(self):
        """
        favorite history
        """
        if self.root is None:
            return
        try:
            db = SQLiteParser.Database.FromNode(self.root, canceller)
            if db is None:
                return
            if 'fav_poi_main' not in db.Tables:
                return
            tbs = SQLiteParser.TableSignature("fav_poi_main")
            for rec in db.ReadTableRecords(tbs, self.extract_deleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        return
                    fav_poi = model_map.FavPoi()
                    loc = model_map.Location()
                    fav_poi.fav_obj = loc.location_id
                    
                    fav_poi.sourceApp = "百度地图"
                    fav_poi.sourceFile = self.root.AbsolutePath
                    
                    loc.sourceApp = "百度地图"
                    loc.type = 6 # 百度地图米制坐标
                    loc.sourceFile = self.root.AbsolutePath
                    if rec.Deleted == DeletedState.Deleted:
                        fav_poi.deleted = 1
                        loc.deleted = 1
                    if "ext_name" in rec:
                        fav_poi.poi_name = rec["ext_name"].Value
                    if "addr" in rec and (not rec["addr"].IsDBNull):
                        fav_poi.address = rec["addr"].Value
                        loc.address = rec["addr"].Value
                    if "city_id" in rec and (not rec["city_id"].IsDBNull):
                        fav_poi.city_id = rec["city_id"].Value
                    if "ext_geoptx" in rec and (not rec["ext_geoptx"].IsDBNull):
                        loc.longitude = int(rec["ext_geoptx"].Value)
                    if "ext_geopty" in rec and (not rec["ext_geopty"].IsDBNull):
                        loc.latitude = int(rec["ext_geopty"].Value)
                    if "ctime" in rec and (not rec["ctime"].IsDBNull):
                        fav_poi.create_time = rec["ctime"].Value
                        loc.timestamp = rec["ctime"].Value
                    if loc.latitude and loc.longitude:
                        self.baidumap.db_insert_table_location(loc)
                    if fav_poi.poi_name:
                        self.baidumap.db_insert_table_favpoi(fav_poi)
                except:
                    pass
        except Exception as e:
            TraceService.Trace(TraceLevel.Error,"Get model_map.FavPoi() Failed! -{0}".format(e)) 
        self.baidumap.db_commit()

        if 'fav_route_main' not in db.Tables:
                return
        route_tb = SQLiteParser.TableSignature("fav_route_main")
        for route_rec in db.ReadTableRecords(route_tb, self.extract_deleted, True):
            try:
                if canceller.IsCancellationRequested:
                    return
                route_favpoi = model_map.FavPoi()
                route = model_map.FavRoute()
                route_favpoi.fav_obj = route.nav_id
                
                if route_rec.Deleted == DeletedState.Deleted:
                    route_favpoi.deleted = 1
                    route.deleted = 1
                
                route_favpoi.sourceApp = "百度地图"
                route_favpoi.sourceFile = self.root.AbsolutePath
                
                route_favpoi.sourceApp = "百度地图"
                route_favpoi.sourceFile = self.root.AbsolutePath

                if "sfavnode" in route_rec and (not route_rec["sfavnode"].IsDBNull):
                    try:
                        start_data = json.loads(route_rec["sfavnode"].Value)
                        route.from_name = start_data["name"] if "name" in start_data else ""
                        route.from_posX = start_data["geoptx"] if "geoptx" in start_data else 0
                        route.from_posY = start_data["geopty"] if "geopty" in start_data else 0
                    except Exception as e:
                        TraceService.Trace(TraceLevel.Error,"{0}".format(e))
                
                if "efavnode" in route_rec and (not route_rec["efavnode"].IsDBNull):
                    try:
                        end_data = json.loads(route_rec["efavnode"].Value)
                        route.to_name = end_data["name"] if "name" in end_data else ""
                        route.to_posX = end_data["geoptx"] if "geoptx" in end_data else 0
                        route.to_posY = end_data["geopty"] if "geopty" in end_data else 0
                    except:
                        TraceService.Trace(TraceLevel.Error,"{0}".format(e))
                route.type = 6
                if "ctime" in rec and (not rec["ctime"].IsDBNull):
                    route_favpoi.create_time = rec["ctime"].Value
                    route.create_time = rec["ctime"].Value
                if "name" in route_rec and (not route_rec["name"].IsDBNull):
                    route_favpoi.poi_name = route_rec["name"].Value

                if route.from_name and route.to_name:
                    self.baidumap.db_insert_table_favroute(route)
                if route_favpoi.poi_name:
                    self.baidumap.db_insert_table_favpoi(route_favpoi)
            except:
                pass
        self.baidumap.db_commit()


def analyze_baidumap(root, extract_deleted, extract_source):
    TraceService.Trace(TraceLevel.Info,"正在分析安卓百度地图...")
    pr = ParserResults()
    prResult = baiduMapParser(root, extract_deleted, extract_source).parse()
    if prResult:
        pr.Models.AddRange(prResult)
    pr.Build("百度地图")
    TraceService.Trace(TraceLevel.Info,"安卓百度地图分析完成!")
    return pr