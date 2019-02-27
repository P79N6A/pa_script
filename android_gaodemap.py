#coding=utf-8

'''
The script is based on gaode map version 8.85.0.2275, 
which is theoretically compatible with less than 8.85.0.2275
'''

__author__ = "Xu Tao"
__date__ = "2019-1-25"
__maintainer__ = "Xu Tao"


import clr
try:
    clr.AddReference('model_map_v2')
    clr.AddReference("MapUtil")
    clr.AddReference('System.Data.SQLite')
    clr.AddReference("bcp_gis")
except:
    pass
from PA_runtime import *
from System.Security.Cryptography import *
from System.IO import File
from System import *
import model_map_v2 as model_map
import System.Data.SQLite as SQLite
import json
import MapUtil
import bcp_gis
import os
import PA_runtime
from PA.InfraLib.ModelsV2 import *


class gaodeMap(object):

    def __init__(self, node, extract_deleted, extract_source):
        self.root = node
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.cache = ds.OpenCachePath("gaodeMap")
        self.gaodemap = model_map.Map()
        self.decrypt_db_path = None
        self.decrypt_node = None

    
    def parse(self):

        self.decrypt_db_path = self.cache + "/girf_sync_decode.db"
        db_path = MapUtil.md5(self.cache, self.root.AbsolutePath)

        if not os.path.exists(self.decrypt_db_path):  # 如果已经解密过db,就跳过解密步骤
            self.decode_db() 
        self.decrypt_node = self.create_memory_node(self.root.Parent, self.cache + "/girf_sync_decode.db", "girf_sync_decode.db")
        self.gaodemap.db_create(db_path)
        account_list = self.get_account_list()
        self.get_search_history(account_list)
        self.get_fav_point_history(account_list)
        self.get_fav_route_history(account_list)
        self.get_route_history(account_list)

        tmp_dir = ds.OpenCachePath("tmp")
        PA_runtime.save_cache_path(bcp_gis.NETWORK_APP_MAP_GAODE, db_path, tmp_dir)

        results = model_map.ExportModel(db_path).get_model()
        return results
        

    def get_route_history(self, account_list):
        db = SQLiteParser.Database.FromNode(self.decrypt_node, canceller)
        route_tables = map(lambda x:"ROUTE_HISTORY_V2_SNAPSHOT"+x, account_list)
        for route_table in route_tables:
            if route_table not in db.Tables:
                return
            tbs = SQLiteParser.TableSignature(route_table)
            for rec in db.ReadTableRecords(tbs, self.extract_deleted, True):
                try:
                    route = model_map.RouteRec()
                    route.sourceApp = "高德地图"
                    route.sourceFile = self.root.AbsolutePath
                    route.type = 9
                    if "data" in rec and (not rec["data"].IsDBNull):
                        try:
                            data = json.loads(rec["data"].Value)
                            if "from_poi_json" in data:
                                from_json = data["from_poi_json"]
                                if "mName" in from_json:
                                    route.from_name = from_json["mName"]
                                if "mX" in from_json:
                                    route.from_posX = float(from_json["mX"])
                                if "mY" in from_json:
                                    route.from_posY = float(from_json["mY"])
                            if "to_poi_json" in data:
                                to_json = data["to_poi_json"]
                                if "mName" in to_json:
                                    route.to_name = to_json["mName"]
                                if "mX" in to_json:
                                    route.to_posX = float(to_json["mX"])
                                if "mY" in to_json:
                                    route.to_posY = float(to_json["mY"])
                                if "mAddr" in to_json:
                                    route.to_addr = to_json["mAddr"]
                        except:
                            pass
                    if route.from_name and route.to_name:
                        self.gaodemap.db_insert_table_routerec(route)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error,"{0}".format(e))
        self.gaodemap.db_commit()


    def get_fav_point_history(self, account_list):
        """
        收藏的是一个点
        """
        db = SQLiteParser.Database.FromNode(self.decrypt_node, canceller)
        poi_tables = map(lambda x:"POI_SNAPSHOT"+x, account_list)
        for poi_table in poi_tables:
            if poi_table not in db.Tables:
                return
            tbs = SQLiteParser.TableSignature(poi_table)
            for rec in db.ReadTableRecords(tbs, self.extract_deleted, True):
                try:
                    favpoi = model_map.FavPoi()
                    loc = model_map.Location()
                    favpoi.fav_obj = loc.location_id
                    favpoi.sourceApp = "高德地图"
                    favpoi.sourceFile = self.root.AbsolutePath
                    
                    loc.sourceApp = "高德地图"
                    loc.sourceFile = self.root.AbsolutePath
                    loc.type = 9
                    if rec.Deleted == DeletedState.Deleted:
                        favpoi.deleted = 1
                        loc.deleted = 1

                    # 坐标类型还未赋值
                    if "name" in rec and (not rec["name"].IsDBNull):
                        favpoi.poi_name = rec["name"].Value
                    if "address" in rec and (not rec["address"].IsDBNull):
                        loc.address = rec["address"].Value
                    if "point_x" in rec and (not rec["point_x"].IsDBNull):
                        loc.longitude = rec["point_x"].Value
                    if "point_y" in rec and (not rec["point_y"].IsDBNull):
                        loc.latitude = rec["point_y"].Value
                    if "create_time" in rec and (not rec["create_time"].IsDBNull):
                        loc.timestamp = MapUtil.convert_to_timestamp(rec["create_time"].Value)
                        favpoi.create_time = MapUtil.convert_to_timestamp(rec["create_time"].Value)

                    if loc.latitude and loc.longitude:
                            self.gaodemap.db_insert_table_location(loc)
                    if favpoi.poi_name:
                        self.gaodemap.db_insert_table_favpoi(favpoi)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error,"{0}".format(e))
        self.gaodemap.db_commit()


    def get_fav_route_history(self, account_list):
        """
        收藏的是路线
        """
        db = SQLiteParser.Database.FromNode(self.decrypt_node, canceller)
        route_tables = map(lambda x:"ROUTE_SNAPSHOT"+x, account_list)
        for route_table in route_tables:
            if route_table not in db.Tables:
                return
            tbs = SQLiteParser.TableSignature(route_table)
            for rec in db.ReadTableRecords(tbs, self.extract_deleted, True):
                try:
                    route_favpoi = model_map.FavPoi()
                    fav_route = model_map.FavRoute()
                    route_favpoi.fav_obj = fav_route.nav_id
                    fav_route.type = 9
                    route_favpoi.sourceApp = "高德地图"
                    route_favpoi.sourceFile = self.root.AbsolutePath

                    fav_route.sourceApp = "高德地图"
                    fav_route.sourceFile = self.root.AbsolutePath
                    
                    if rec.Deleted == DeletedState.Deleted:
                        route_favpoi.deleted = 1
                        fav_route.deleted = 1
                    # 坐标类型还未赋值
                    if "route_name" in rec and (not rec["route_name"].IsDBNull):
                        route_favpoi.poi_name = rec["route_name"].Value
                    if "from_name" in rec and (not rec["from_name"].IsDBNull):
                        fav_route.from_name = rec["from_name"].Value
                    if "to_name" in rec and (not rec["to_name"].IsDBNull):
                        fav_route.to_name = rec["to_name"].Value
                    if "update_time" in rec and (not rec["update_time"].IsDBNull):
                        route_favpoi.create_time = MapUtil.convert_to_timestamp(float(rec["update_time"].Value))
                        fav_route.create_time = MapUtil.convert_to_timestamp(float(rec["update_time"].Value))
                    
                    self.gaodemap.db_insert_table_favroute(fav_route)
                    self.gaodemap.db_insert_table_favpoi(route_favpoi)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error,"{0}".format(e))
        self.gaodemap.db_commit()

    
    def get_search_history(self, account_list):
        """
        search history
        """
        db = SQLiteParser.Database.FromNode(self.decrypt_node, canceller)
        search_tables = map(lambda x:"SEARCH_SNAPSHOT"+x, account_list)
        for search_table in search_tables:
            if search_table not in db.Tables:
                return
            tbs = SQLiteParser.TableSignature(search_table)
            for rec in db.ReadTableRecords(tbs, self.extract_deleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        return
                    search = model_map.Search()
                    if rec.Deleted == DeletedState.Deleted:
                        search.deleted = 1
                    search.sourceApp = "高德地图"
                    search.sourceFile = self.root.AbsolutePath
                    if "data" in rec and (not rec["data"].IsDBNull):
                        try:
                            data = json.loads(rec["data"].Value)
                            if "name" in data:
                                search.keyword = data["name"]
                            if "addres" in data:
                                search.address = data["address"]
                            if "x" in data:
                                search.pos_x = float(data["x"])
                            if "y" in data:
                                search.pos_y = float(data["y"])
                            if "update_time" in data:
                                search.create_time = MapUtil.convert_to_timestamp(data["update_time"])
                        except:
                            pass
                    if search.keyword:
                        self.gaodemap.db_insert_table_search(search)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error,"Get gaodeMap model_map.Search() Failed! -{0}".format(e))
        self.gaodemap.db_commit()


    def get_account_list(self):
        conn = SQLite.SQLiteConnection("Data Source = {0}; ReadOnly = True".format(self.decrypt_db_path))
        try:
            account_list = []
            conn.Open()
            cmd = SQLite.SQLiteCommand(conn)
            cmd.CommandText = '''
                select tbl_name from sqlite_master where tbl_name like '%POI_SNAPSHOT%' and type ='table'
            '''
            reader = cmd.ExecuteReader()
            while reader.Read():
                try:
                    a_id = reader.GetString(0) if not reader.IsDBNull(0) else ""
                    # if len(a_id) != len("POI_SNAPSHOT"):
                    account_list.append(a_id[len("POI_SNAPSHOT"):])
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error,"{0}".format(e))
            reader.Close()
            conn.Close()
            return account_list
        except:
            pass
        

    def decode_db(self):
        """
        decrypt sqlite....
        AES.ECB with key = a4a11bb9ef4b2f4c
        """
        node = self.root
        if node is None:
            return
        f_dest = File.OpenWrite(self.cache + "/girf_sync_decode.db")
        data = Encoding.UTF8.GetBytes("SQLite format 3\0")
        f_dest.Write(data,0,data.Length)
        nodes = node.Data
        nodes.Seek(0,SeekOrigin.Begin)
        bts = Array[Byte](range(16))

        rm = RijndaelManaged()
        rm.Key = Encoding.UTF8.GetBytes("a4a11bb9ef4b2f4c")
        rm.Mode = CipherMode.ECB
        rm.Padding = PaddingMode.None
        tr = rm.CreateDecryptor()
        stm=CryptoStream(f_dest,tr,CryptoStreamMode.Write)

        count = 0
        nodes.Seek(8,SeekOrigin.Begin)
        count = nodes.Read(bts,0,8)
        stm.Write(bts,0,8)
        nodes.Seek(24,SeekOrigin.Begin)
        while True:
            count = nodes.Read(bts,0,bts.Length)
            if count!=0:
                stm.Write(bts,0,count)
                stm.Flush()
            else:
                break
        stm.Close()
        f_dest.Close()


    def create_memory_node(self, parent, rfs_path, vfs_name):
        """
            rfs_path:REAL FILE SYSTEM FILE PATH(ABSOLUTE)
            vfs_name:file_name in virtual file system
            ret:node which compact with vfs
        """
        mem_range = MemoryRange.CreateFromFile(rfs_path)
        r_node = Node(vfs_name, Files.NodeType.Embedded)
        r_node.Data = mem_range
        parent.Children.Add(r_node)
        return r_node


def analyze_gaodemap(node, extract_deleted, extract_source):
    TraceService.Trace(TraceLevel.Info,"正在分析安卓高德地图...")
    pr = ParserResults()
    prResult = gaodeMap(node, extract_deleted, extract_source).parse()
    if prResult:
        pr.Models.AddRange(prResult)
    pr.Build("高德地图")
    TraceService.Trace(TraceLevel.Info,"苹果安卓地图分析完成!")
    return pr

def execute(node, extract_deleted):
    return analyze_gaodemap(node, extract_deleted, False)