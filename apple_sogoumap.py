#coding:utf-8

'''
The script is based on sogou map version 8.3.7.561.1, 
which is theoretically compatible with less than 8.3.7.561.1
'''

__date__ = "2019-1-30"
__author__ = "Xu Tao"
__maintainer__ = "Xu Tao"

import clr
try:
    clr.AddReference('model_map_v2')
    clr.AddReference("MapUtil")
    clr.AddReference("bcp_gis")
except:
    pass
from PA_runtime import *
import json
import model_map_v2 as model_map
import MapUtil
import bcp_gis


class SogouMap(object):

    def __init__(self, node, extract_deleted, extract_source):
        self.root = node
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.sogouMap = model_map.Map()
        self.db_cache = ds.OpenCachePath("sogouMap")


    def parse(self):
        db_path = MapUtil.md5(self.db_cache, self.root.AbsolutePath)
        self.sogouMap.db_create(db_path)
        self.get_search_history_v1()
        self.get_favpoi_v1()
        # self.get_route_v1()
        self.sogouMap.db_close()
        
        tmp_dir = ds.OpenCachePath("tmp")
        PA_runtime.save_cache_path(bcp_gis.NETWORK_APP_MAP_SOGOU, db_path, tmp_dir)

        results = model_map.ExportModel(db_path).get_model()
        return results


    def get_search_history_v1(self):
        search_node = self.root.Parent.Parent.Parent.GetByPath("Documents/MapData.data")
        if search_node is None:
            return
        mapdb = SQLiteParser.Database.FromNode(search_node, canceller)
        if "ZSGMHISTORYENTITY" not in mapdb.Tables:
            return
        tb = SQLiteParser.TableSignature("ZSGMHISTORYENTITY")
        for rec in mapdb.ReadTableRecords(tb, self.extract_deleted, self.extract_source):
            try:
                if canceller.IsCancellationRequested:
                    return
                search = model_map.Search()
                search.sourceApp = "搜狗地图"
                search.sourceFile = search_node.AbsolutePath
                if rec.Deleted == DeletedState.Deleted:
                    search.deleted = 1
                if "ZCAPTION" in rec and (not rec["ZCAPTION"].IsDBNull):
                    search.keyword = rec["ZCAPTION"].Value
                if "ZADDRESS" in rec and (not rec["ZADDRESS"].IsDBNull):
                    search.address = rec["ZADDRESS"].Value
                if "ZDESC" in rec and (not rec["ZDESC"].IsDBNull):
                    search.address = rec["ZDESC"].Value
                if "ZDATE" in rec and (not rec["ZDATE"].IsDBNull):
                    search.create_time = SogouMap._format_mac_timestamp((rec["ZDATE"].Value))
                if search.keyword:
                    self.sogouMap.db_insert_table_search(search)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error,"{0}".format(e))
        self.sogouMap.db_commit()


    def get_favpoi_v1(self):
        fav_node = self.root.Parent.Parent.Parent.GetByPath("Documents/MapData.data")
        if fav_node is None:
            return
        mapdb = SQLiteParser.Database.FromNode(fav_node, canceller)
        if "ZSGMFAVORITEENTITY" not in mapdb.Tables:
            return
        tb = SQLiteParser.TableSignature("ZSGMFAVORITEENTITY")
        for rec in mapdb.ReadTableRecords(tb, self.extract_deleted, self.extract_source):
            try:
                if canceller.IsCancellationRequested:
                    return
                favpoi = model_map.FavPoi()
                favpoi.sourceApp = "搜狗地图"
                favpoi.sourceFile = fav_node.AbsolutePath
                if rec.Deleted == DeletedState.Deleted:
                    favpoi.deleted = 1
                if "ZCAPTION" in rec and (not rec["ZCAPTION"].IsDBNull):
                    favpoi.poi_name = rec["ZCAPTION"].Value
                if "ZCREATETIME" in rec and (not rec["ZCREATETIME"].IsDBNull):
                    favpoi.create_time = SogouMap._format_mac_timestamp(rec["ZCREATETIME"].Value)
                if favpoi.poi_name:
                    self.sogouMap.db_insert_table_favpoi(favpoi)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error,"{0}".format(e))
        self.sogouMap.db_commit()


    def get_route_v1(self):
        route_node = self.root.Parent.Parent.Parent.GetByPath("Documents/BusHistory.plist")
        if route_node is None:
            return
        bplist = BPReader(route_node.Data).top
        for uid in bplist["$objects"][1]["NS.objects"]:
            if uid is None:
                break
            data = self.get_route_from_dict(bplist["$objects"], uid.Value)
            print(data) 
            route = model_map.RouteRec()
            route.sourceApp = "搜狗地图"
            route.sourceFile = route_node.AbsolutePath
            if "bus.history.startname" in data:
                route.from_name = data.get("bus.history.startname").Value
            if "bus.history.endname" in data:
                route.to_name = data.get("bus.history.endname").Value
            if "bus.history.detail" in data:
                pass
            if "bus.history.date" in data:
                pass
            if route.from_name and route.to_name:
                self.sogouMap.db_insert_table_routerec(route)
        self.sogouMap.db_commit()


    def get_route_from_dict(self, bp, uid):
        values = {}
        attrs = dir(bp[uid])
        if "Keys" in attrs:
            for key in bp[uid].Keys:
                if key in ["bus.history.type"]:
                    values[key] =  bp[uid][key].Value
                else:
                    tmp = bp[uid][key].Value
                    values[key] = bp[tmp]
        else:
            pass
        return values

    @staticmethod
    def _format_mac_timestamp(mac_time, v = 10):
        """
        from mac-timestamp generate unix time stamp
        """
        date = 0
        date_2 = mac_time
        if mac_time < 1000000000:
            date = mac_time + 978307200
        else:
            date = mac_time
            date_2 = date_2 - 978278400 - 8 * 3600
        s_ret = date if v > 5 else date_2
        return int(s_ret)


def analyze_sogoumap(node, extract_deleted, extract_source):
    TraceService.Trace(TraceLevel.Info,"正在分析苹果搜狗地图...")
    pr = ParserResults()
    results = SogouMap(node, extract_deleted, extract_source).parse()
    if results:
        pr.Models.AddRange(results)
    pr.Build("搜狗地图")
    TraceService.Trace(TraceLevel.Info,"苹果搜狗地图分析完成!")
    return pr


def execute(node, extract_deleted):
    return analyze_sogoumap(node, extract_deleted, False)