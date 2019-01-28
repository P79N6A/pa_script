#coding=utf-8

'''
The script is based on gaode map version 8.3.7, 
which is theoretically compatible with less than 8.3.7
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
        self.get_favorites_address()
        self.get_home_company()
        self.get_route_by_bus()
        self.get_route_by_car()
        self.get_route_by_walk()
        self.tencentMap.db_close()

        results = model_map.ExportModel(db_path).get_model()
        return results


    def get_search_history(self):
        """
        搜索记录
        """
        search_node = self.root.GetByPath("Documents/user/basehistoryV2.dat")
        if search_node is None:
            return
        bplist = BPReader(search_node.Data).top
        for uid in bplist['$objects'][1]['NS.objects']:
            if canceller.IsCancellationRequested:
                return
            if uid is None:
                break
            data = self.get_dict_from_search(bplist["$objects"], uid.Value)

            search = model_map.Search()
            search.sourceApp = "腾讯地图"
            search.sourceFile = search_node.AbsolutePath

            if "name" in data:
                search.keyword = data.get("name").Value if "name" in data else ""
            if "addr" in data:
                search.address = data.get("addr").Value if "addr" in data and data.get("addr").Value != "$null" else ""
            if "pointX" in data:
                search.pos_x = data.get("pointX") if "pointX" in data else 0
            if "pointY" in data:
                search.pos_y = data.get("pointY") if "pointY" in data else 0
            if search.keyword:
                self.tencentMap.db_insert_table_search(search)
        self.tencentMap.db_commit()


    def get_dict_from_search(self, bp, dictvalue):
        values = {}
        attrs = dir(bp[dictvalue])
        if "Keys" in attrs:
            for key in bp[dictvalue].Keys:
                # 太多可以采用字典格式判断
                if key in ["index", "accurate", "navPointY", "type", "coType", "pointY", "coor_start", "hasStreetView", "locationType", "pointX", "frequency", "navPointX", "navPointY", "coor_end"]:
                    values[key] = bp[dictvalue][key].Value
                else:
                    tmp = bp[dictvalue][key].Value
                    values[key] = bp[tmp]
        else:
            pass
        return values


    def get_favorites_address(self):
        """
        分析收藏的位置
        """
        favorites_address_node = self.root.GetByPath("Documents/user/favorite.dat")
        if favorites_address_node is None:
            return
        with open(favorites_address_node.PathWithMountPoint, "r") as f: 
            tmp = f.read()
            json_data = json.loads(tmp)
            if "fav_list" in json_data:
                for item in json_data["fav_list"]:
                    fav_point = model_map.FavPoi()
                    loc = model_map.Location()
                    fav_point.fav_obj = loc.location_id
                    loc.sourceApp = "腾讯地图"
                    loc.sourceFile = favorites_address_node.AbsolutePath
                    fav_point.sourceApp = "腾讯地图"
                    fav_point.sourceFile = favorites_address_node.AbsolutePath
                    loc.type = 6
                    if "name" in item:
                        fav_point.poi_name = item["name"]
                    if "content" in item:
                        if "pointx" in item["content"]:
                            loc.longitude = item["content"]["pointx"]
                        if "pointy" in item["content"]:
                            loc.latitude = item["content"]["pointy"]
                        if "addr" in item["content"]:
                            loc.address = item["content"]["addr"]
                    if loc.latitude and loc.longitude:
                            self.tencentMap.db_insert_table_location(loc)
                    if fav_point.poi_name:
                        self.tencentMap.db_insert_table_favpoi(fav_point)
        self.tencentMap.db_commit()


    def get_home_company(self):
        home_company_node = self.root.GetByPath("Documents/user/frequentAddressArray.dat")
        if home_company_node is None:
            return
        bp = BPReader(home_company_node.Data).top
        for uid in bp['$objects'][1]['NS.objects']:
            if canceller.IsCancellationRequested:
                return
            if uid is None:       
                break
            data = self.get_dict_from_bplist(bp["$objects"], uid.Value)

            favpoi = model_map.FavPoi()
            loc = model_map.Location()
            favpoi.fav_obj = loc.location_id
            favpoi.sourceApp = "腾讯地图"
            favpoi.sourceFile = home_company_node.AbsolutePath
            loc.sourceApp = "腾讯地图"   
            loc.sourceFile = home_company_node.AbsolutePath
            
            if "name" in data:
                favpoi.poi_name = data.get("name").Value
            if "address" in data:
                loc.address = data.get("address").Value
            if "longitude" in data:
                loc.longitude = data.get("longitude") if data.get("longitude") else 0
            if "latitude" in data:
                loc.latitude = data.get("latitude") if data.get("latitude") else 0
            if loc.latitude and loc.longitude:
                self.tencentMap.db_insert_table_location(loc)
            if favpoi.poi_name:
                self.tencentMap.db_insert_table_favpoi(favpoi)
        
        self.tencentMap.db_commit()


    def get_dict_from_bplist(self, bp, dictvalue):
        values = {}
        attrs = dir(bp[dictvalue])
        if "Keys" in attrs:
            for key in bp[dictvalue].Keys:
                if key in ["addrtype", "longitude", "latitude"]:
                    values[key] =  bp[dictvalue][key].Value
                else:
                    tmp = bp[dictvalue][key].Value
                    values[key] = bp[tmp]
        else:
            pass
        return values


    def get_route_by_bus(self):
        route_node = self.root.GetByPath("Documents/user/busRouteHistory")
        if route_node is None:
            return 
        bplist = BPReader(route_node.Data).top
        for uid in bplist['$objects'][1]['NS.objects']:
            if canceller.IsCancellationRequested:
                return
            if uid is None:
                break
            self.decode_route(bplist["$objects"], uid.Value, route_node)
        self.tencentMap.db_commit()


    def get_route_by_car(self):
        route_node = self.root.GetByPath("Documents/user/carRouteHistory")
        if route_node is None:
            return 
        bplist = BPReader(route_node.Data).top
        for uid in bplist['$objects'][1]['NS.objects']:
            if canceller.IsCancellationRequested:
                return
            if uid is None:
                break
            self.decode_route(bplist["$objects"], uid.Value, route_node)  
        self.tencentMap.db_commit()             


    def get_route_by_walk(self):
        route_node = self.root.GetByPath("Documents/user/walkCycleRouteHistory")
        if route_node is None:
            return 
        bplist = BPReader(route_node.Data).top
        for uid in bplist['$objects'][1]['NS.objects']:
            if canceller.IsCancellationRequested:
                return
            if uid is None:
                break
            self.decode_route(bplist["$objects"], uid.Value, route_node)
        self.tencentMap.db_commit() 


    def decode_route(self, bp, dictvalue, node):
        start_point, end_point = self.get_route_from_dict(bp, dictvalue)
        if start_point and end_point:
            
            route = model_map.RouteRec()
            route.sourceApp = "腾讯地图"
            route.sourceFile = node.AbsolutePath
            route.type = 9
            if "name" in start_point and "name" in end_point:
                route.from_name = start_point.get("name").Value
                route.to_name = end_point.get("name").Value

            if "addr" in start_point and "addr" in end_point:
                if start_point["addr"].Value != "$null":
                    route.from_addr = start_point.get("addr").Value
                if end_point["addr"].Value  != "$null":
                    route.to_addr = end_point.get("addr").Value    

            if "pointX" in start_point and "pointX" in end_point:
                route.from_posX = start_point.get("pointX") if start_point.get("pointX") else 0
                route.to_posX = end_point.get("pointX") if end_point.get("pointX") else 0
            
            if "pointY" in start_point and "pointY" in end_point:
                route.from_posY = start_point.get("pointY") if start_point.get("pointY") else 0
                route.to_posY = end_point.get("pointY") if end_point.get("pointY") else 0
            
            if route.from_name and route.to_name:
                self.tencentMap.db_insert_table_routerec(route)

   
    def get_route_from_dict(self, bp, dictvalue):
        startvalues = {}
        endvalues = {}
        destPOI = bp[dictvalue]["destPOI"]
        startPOI = bp[dictvalue]["startPOI"]
        fromaddr = bp[startPOI.Value]
        toaddr = bp[destPOI.Value]
        attrs1 = dir(fromaddr)
        if "Keys" in attrs1:
            for key in fromaddr.Keys:
                if key in ["index", "accurate", "navPointY", "type", "coType", "pointY", "coor_start", "hasStreetView", "locationType", "pointX", "navPointX", "coor_end"]:
                    startvalues[key] = fromaddr[key].Value
                else:
                    tmp = fromaddr[key].Value
                    startvalues[key] = bp[tmp]
        attrs2 = dir(toaddr)
        if "Keys" in attrs2:
            for key in toaddr.Keys:
                if key in ["index", "accurate", "navPointY", "type", "coType", "pointY", "coor_start", "hasStreetView", "locationType", "pointX", "navPointX", "coor_end"]:
                    endvalues[key] = toaddr[key].Value
                else:
                    tmp = toaddr[key].Value
                    endvalues[key] = bp[tmp]
        
        return startvalues,endvalues



def analyze_tencentmap(node, extract_deleted, extract_source):
    pr = ParserResults()
    results = TencentMap(node, extract_deleted, extract_source).parse()
    if results:
        pr.Models.AddRange(results)
    pr.Build("腾讯地图")
    return pr


def execute(node, extract_deleted):
    return analyze_gaodemap(node, extract_deleted, False)