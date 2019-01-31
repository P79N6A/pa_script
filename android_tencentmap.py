#coding=utf-8

'''
The script is based on gaode map version 8.3.7.561.1, 
which is theoretically compatible with less than 8.3.7.561.1
'''

__date__ = "2019-1-28"
__author__ = "Xu Tao"
__maintainer__ = "Xu Tao"


import clr
try:
    clr.AddReference('model_map_v2')
    clr.AddReference("MapUtil")
except:
    pass
from PA_runtime import *
import json
import model_map_v2 as model_map
import MapUtil


class TencentMap(object):

    def __init__(self, node, extract_deleted, extract_source):
        self.root = node
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.tencentMap = model_map.Map()
        self.cache = ds.OpenCachePath("tencentMap")


    def parse(self):
        db_path = MapUtil.md5(self.cache, self.root.AbsolutePath)
        self.tencentMap.db_create(db_path)
        self.get_search_history()
        self.get_route_history()
        self.get_fav_point()
        self.tencentMap.db_close()

        results = model_map.ExportModel(db_path).get_model()
        return results

    
    def get_search_history(self):
        """
        搜索记录
        """
        search_node = self.root.Parent.GetByPath("poi_search_history.db")
        db = SQLiteParser.Database.FromNode(search_node, canceller)
        if db is None:
            return
        if 't_poi_search_history' not in db.Tables:
            return
        tbs = SQLiteParser.TableSignature("t_poi_search_history")
        for rec in db.ReadTableRecords(tbs, self.extract_deleted, True):
            try:
                if canceller.IsCancellationRequested:
                    return
                search = model_map.Search()
                search.sourceApp = "腾讯地图"
                search.sourceFile = search_node.AbsolutePath
                if rec.Deleted == DeletedState.Deleted:
                    search.deleted = 1
                if "_keyword" in rec and (not rec["_keyword"].IsDBNull):
                    search.keyword = rec["_keyword"].Value
                if "_data" in rec and (not rec["_data"].IsDBNull):
                    tmp = rec["_data"].Value
                    json_data = json.loads(tmp)
                    if "address" in json_data:
                        search.address = json_data["address"]
                    if "latLng" in json_data:
                        if "a" in json_data["latLng"] and  "b" in json_data["latLng"]:
                            search.pos_x = json_data["latLng"]["a"]
                            search.pos_y = json_data["latLng"]["b"]
                    if "_last_used" in rec and (not rec["_last_used"].IsDBNull):
                        search.create_time = MapUtil.convert_to_timestamp(rec["_last_used"].Value)
                if search.keyword:
                    self.tencentMap.db_insert_table_search(search)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error,"{0}".format(e))
        self.tencentMap.db_commit()


    def get_route_history(self):
        """
        导航记录
        """
        db = SQLiteParser.Database.FromNode(self.root, canceller)
        if db is None:
            return
        if 'route_search_history_tab' not in db.Tables:
            return   
        tbs = SQLiteParser.TableSignature("route_search_history_tab")
        for rec in db.ReadTableRecords(tbs, self.extract_deleted, True):
            try:
                if canceller.IsCancellationRequested:
                    return
                route = model_map.RouteRec()
                route.sourceApp = "腾讯地图"
                route.sourceFile = self.root.AbsolutePath
                if rec.Deleted == DeletedState.Deleted:
                    route.deleted = 1
                if "from_data" in rec and (not rec["from_data"].IsDBNull):
                    try:
                        from_data = json.loads(rec["from_data"].Value)
                        if "name" in from_data:
                            route.from_name = from_data["name"]
                        if "addr" in from_data:
                            route.from_addr = from_data["addr"]
                        if "lon" in from_data:
                            route.from_posX = TencentMap._convert_coordinate(from_data["lon"],1)
                        if "lat" in from_data:
                            route.from_posY = TencentMap._convert_coordinate(from_data["lat"],2)
                    except:
                        pass
                if "end_data" in rec and (not rec["end_data"].IsDBNull):
                    try:
                        end_data = json.loads(rec["end_data"].Value)
                        if "name" in end_data:
                            route.to_name = end_data["name"]
                        if "addr" in end_data:
                            route.to_addr = end_data["addr"]
                        if "lon" in end_data:
                            route.to_posX = TencentMap._convert_coordinate(end_data["lon"],1)
                        if "lat" in end_data:
                            route.to_posY = TencentMap._convert_coordinate(end_data["lat"],2)
                    except Exception as e:
                        TraceService.Trace(TraceLevel.Error,"{0}".format(e))
                if "_lasted_used" in rec and (not rec["_lasted_used"].IsDBNull):
                    route.create_time = MapUtil.convert_to_timestamp(rec["_lasted_used"].Value)
                if route.from_name and route.to_name:
                    self.tencentMap.db_insert_table_routerec(route)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error,"{0}".format(e))
        self.tencentMap.db_commit()


    def get_fav_point(self):
        fav_node = self.root.Parent.GetByPath("favorite_new.db")
        db = SQLiteParser.Database.FromNode(fav_node, canceller)
        if db is None:
            return
        if 'favorite_poi_info' not in db.Tables:
            return  
        tbs = SQLiteParser.TableSignature("favorite_poi_info")
        for rec in db.ReadTableRecords(tbs, self.extract_deleted, True):
            try:
                if canceller.IsCancellationRequested:
                    return
                favpoi = model_map.FavPoi()
                loc = model_map.Location()
                favpoi.fav_obj = loc.location_id
                favpoi.sourceApp = "腾讯地图"
                favpoi.sourceFile = fav_node.AbsolutePath
                loc.sourceApp = "腾讯地图"
                loc.sourceFile = fav_node.AbsolutePath
                if "`name`" in rec and (not rec["`name`"].IsDBNull):
                    favpoi.poi_name = rec["`name`"].Value
                if "`rawData`" in rec and (not rec["`rawData`"].IsDBNull):
                    try:
                        data = json.loads(rec["`rawData`"].Value)
                        if "addr" in data:
                            loc.address = data["addr"]
                        if "point" in data:
                            if "mLatitudeE6" in data["point"]:
                                loc.latitude = TencentMap._convert_coordinate(data["point"]["mLatitudeE6"],2)
                            if "mLongitudeE6" in data["point"]:
                                loc.longitude = TencentMap._convert_coordinate(data["point"]["mLongitudeE6"],1)
                    except Exception as e:
                        TraceService.Trace(TraceLevel.Error,"{0}".format(e))
                if  loc.longitude and loc.latitude:
                    self.tencentMap.db_insert_table_location(loc)
                if favpoi.poi_name:
                    self.tencentMap.db_insert_table_favpoi(favpoi)     
            except Exception as e:
                TraceService.Trace(TraceLevel.Error,"{0}".format(e))
        self.tencentMap.db_commit()

    @staticmethod
    def _convert_coordinate(v, type):
        try:
            if type == 1: # 经度
                return float(str(v)[:3] + "." + str(v)[3:])
            elif type == 2:
                return float(str(v)[:2] +"."+ str(v)[2:])
        except:
            return v  

def analyze_tencentmap(node, extract_deleted, extract_source):
    TraceService.Trace(TraceLevel.Info,"正在分析安卓腾讯地图...")
    pr = ParserResults()
    results = TencentMap(node, extract_deleted, extract_source).parse()
    if results:
        pr.Models.AddRange(results)
    pr.Build("腾讯地图")
    TraceService.Trace(TraceLevel.Info,"安卓腾讯地图分析完成!")
    return pr


def execute(node, extract_deleted):
    return analyze_tencentmap(node, extract_deleted, False)