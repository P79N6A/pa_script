#coding=utf-8

'''
The script is based on baidu map version 10.11.0, 
which is theoretically compatible with less than 10.11.0
'''

__author__ = "Xu Tao"
__date__ = "2019-1-24"
__maintainer__ = 'Xu Tao'


from PA_runtime import *
import clr
try:
    clr.AddReference('model_map_v2')
    clr.AddReference("MapUtil")
except:
    pass
del clr
import model_map_v2 as model_map
from collections import defaultdict
import traceback
import MapUtil
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

        result = model_map.ExportModel(db_path).get_model()
        return result
    
    
    def get_search_history(self):
        """
        search history
        """
        historyNode = self.root.Parent.Parent.Parent.GetByPath("Documents/his_record.sdb")
        if historyNode is None:
            return
        db = SQLiteParser.Database.FromNode(historyNode, canceller)
        if 'his_record' not in db.Tables:
            return
        tb = SQLiteParser.TableSignature('his_record')
        for rec in db.ReadTableRecords(tb, True):
            try:
                if canceller.IsCancellationRequested:
                    return
                search = model_map.Search()
                if rec.Deleted == DeletedState.Deleted:
                    search.deleted = 1
                    break
                search.sourceApp = "百度地图"
                search.sourceFile = historyNode.AbsolutePath
                if "key" in rec:
                    search.keyword = rec["key"].Value
                if search.keyword:
                    self.baidumap.db_insert_table_search(search)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error,"Get model_map.Search() Failed! -{0}".format(e))
        self.baidumap.db_commit()


    def get_navigation_record(self):
        """
        navgation history
        """      
        hsAddressNode = self.root.Parent.Parent.Parent.GetByPath("Documents/routeHis_record.sdb")
        if hsAddressNode is None:
            return
        try:
            db = SQLiteParser.Database.FromNode(hsAddressNode, canceller)
            if 'routeHis_record' not in db.Tables:
                return
            tb = SQLiteParser.TableSignature('routeHis_record')
            for rec in db.ReadTableRecords(tb, True):
                if canceller.IsCancellationRequested:
                    return
                routeaddr = model_map.RouteRec()
                if rec.Deleted == DeletedState.Deleted:
                    routeaddr.deleted = 1
                routeaddr.sourceApp = "百度地图"
                routeaddr.sourceFile = hsAddressNode.AbsolutePath
                
                seach_history = rec["key"].Value
                if "value" in rec and (not rec["value"].IsDBNull):
                    seach_info = rec["value"].Value
                    try:
                        b = bytes(seach_info)
                        jsonflie = b.decode('utf-16')
                        dicts = json.loads(jsonflie)
                        routeaddr.create_time = dicts.get("addtimesec")
                        routeaddr.from_name = dicts.get("sfavnode").get("name")
                        routeaddr.from_posX = dicts.get("sfavnode").get("geoptx")
                        routeaddr.from_posY = dicts.get("sfavnode").get("geopty")
                        routeaddr.to_name = dicts.get("efavnode").get("name")
                        routeaddr.to_posX = dicts.get("efavnode").get("geoptx")
                        routeaddr.to_posY = dicts.get("efavnode").get("geopty")
                        routeaddr.type = 6
                    except Exception as e:
                        continue
                        TraceService.Trace(TraceLevel.Error,"Get model_map.RouteRec() Failed! -{0}".format(e))      
                if routeaddr.from_name:
                    self.baidumap.db_insert_table_routerec(routeaddr)
        except Exception as e:
            TraceService.Trace(TraceLevel.Error,"Get model_map.RouteRec() Failed! -{0}".format(e)) 
        self.baidumap.db_commit()


    def get_fav_position(self):
        """
        favorite history
        """
        hsAddressNode = self.root.Parent.Parent.Parent.GetByPath("Documents/userCoreData/favCoreDataDB.sqlite")
        if hsAddressNode is None:
            return
        try:
            db = SQLiteParser.Database.FromNode(hsAddressNode, canceller)
            if 'ZBMFAVPOI' not in db.Tables:
                return
            tb = SQLiteParser.TableSignature('ZBMFAVPOI')
            for rec in db.ReadTableRecords(tb, True):
                try:
                    if canceller.IsCancellationRequested:
                        return
                    favpoi = model_map.FavPoi()
                    loc = model_map.Location()
                    favpoi.fav_obj = loc.location_id
                    if rec.Deleted == DeletedState.Deleted:
                        favpoi.deleted = 1
                    favpoi.sourceApp = "百度地图"
                    favpoi.sourceFile = hsAddressNode.AbsolutePath
                    loc.sourceApp = "百度地图"
                    loc.sourceFile = hsAddressNode.AbsolutePath
                    loc.type = 6
                    if "ZNAME" in rec and (not rec["ZNAME"].IsDBNull):
                        favpoi.poi_name = rec["ZNAME"].Value
                    if "ZCITYID" in rec and (not rec["ZCITYID"].IsDBNull):
                        favpoi.city_id = rec["ZCITYID"].Value
                    if "ZCITYNAME" in rec and (not rec["ZCITYNAME"].IsDBNull):
                        favpoi.city_name = rec["ZCITYNAME"].Value
                    if "ZADDRESS" in rec and (not rec["ZADDRESS"].IsDBNull):
                        loc.address = rec["ZADDRESS"].Value
                    if "ZX" in rec and (not rec["ZX"].IsDBNull):
                        loc.longitude = rec["ZX"].Value
                    if "ZY" in rec and (not rec["ZY"].IsDBNull):
                        loc.latitude = rec["ZY"].Value
                    if loc.latitude and loc.longitude:
                        self.baidumap.db_insert_table_location(loc)
                    if favpoi.poi_name:
                        self.baidumap.db_insert_table_favpoi(favpoi)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error,"Get model_map.FavPoi() Failed! -{0}".format(e)) 
        except Exception as e:
            TraceService.Trace(TraceLevel.Error,"Get model_map.FavPoi() Failed! -{0}".format(e)) 
        
        dicts = defaultdict(list)
        try:
            db = SQLiteParser.Database.FromNode(hsAddressNode, canceller)
            if 'ZBMFAVROUTENODE' not in db.Tables:
                return
            tb = SQLiteParser.TableSignature('ZBMFAVROUTENODE')
            for rec in db.ReadTableRecords(tb, True):
                try:
                    if canceller.IsCancellationRequested:
                        return
                    if "ZUSNAMESTRING" in rec and (not rec["ZUSNAMESTRING"].IsDBNull):
                        dicts[rec["ZFAVROUTERS"].Value].append(rec["ZUSNAMESTRING"].Value)
                    if "ZX" in rec and (not rec["ZX"].IsDBNull):
                        dicts[rec["ZFAVROUTERS"].Value].append(rec["ZX"].Value)
                    if "ZY" in rec and (not rec["ZY"].IsDBNull):
                        dicts[rec["ZFAVROUTERS"].Value].append(rec["ZY"].Value)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error,"Get model_map.FavPoi() Failed! -{0}".format(e))
            
            for key,value in dicts.items():
                route_favpoi = model_map.FavPoi()
                route_favpoi.sourceApp = "百度地图"
                route_favpoi.sourceFile = hsAddressNode.AbsolutePath
                route = model_map.FavRoute()
                route.sourceApp = "百度地图"
                route.sourceFile = hsAddressNode.AbsolutePath
                route_favpoi.fav_obj = route.nav_id
                route.from_name = value[0]
                route.from_posX = value[1]
                route.from_posY = value[2]
                route.to_name = value[3]
                route.to_posX = value[4]
                route.to_posY = value[5]
                route.type = 6
                self.baidumap.db_insert_table_favroute(route)
                self.baidumap.db_insert_table_favpoi(route_favpoi)

        except Exception as e:
            TraceService.Trace(TraceLevel.Error,"Get model_map.FavPoi() Failed! -{0}".format(e)) 
        self.baidumap.db_commit()


def analyze_baidumap(root, extract_deleted, extract_source):
    pr = ParserResults()
    prResult = baiduMapParser(root, extract_deleted, extract_source).parse()
    if prResult:
        pr.Models.AddRange(prResult)
    pr.Build("百度地图")
    return pr