#coding=utf-8
import PA_runtime
import base64
from PA_runtime import *
from System.Security.Cryptography import *
from System.IO import File
from System import *
from sqlite3 import *
import json
import os
import clr
try:
    clr.AddReference('model_map')
except:
    pass
del clr

import model_map

POI_SNAPSHOT = 12
APPVERSION = "1.0"

class gaodeMap(object):

    def __init__(self, node, extract_deleted, extract_source):
        self.root = node
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.cache = ds.OpenCachePath("gaodeMap")
        self.gaodemap = model_map.Map()
        self.sourcefile = None

    def decode_db(self):
        """
        decrypt sqlite....
        AES.ECB with key = a4a11bb9ef4b2f4c
        """
        #node = self.root.GetByPath('/Documents/cloundSyncData/girf_sync.db')
        node = self.root
        self.sourcefile = node.AbsolutePath
        f_dest = File.OpenWrite(self.cache + "/girf_sync_decode.db")
        data=Encoding.UTF8.GetBytes("SQLite format 3\0")
        f_dest.Write(data,0,data.Length)
        nodes=node.Data
        nodes.Seek(0,SeekOrigin.Begin)
        bts =Array[Byte](range(16))

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
        self.decoded = False


    def entrance(self):
        path = (self.cache + "/girf_sync_decode.db")
        conn = connect(path)
        cursor = conn.cursor()
        find_user_sql = "select tbl_name from sqlite_master where tbl_name like '%POI_SNAPSHOT%' and type ='table'"
        cursor.execute(find_user_sql)
        result = cursor.fetchall() 
        for res in result:
            if canceller.IsCancellationRequested:
                return
            table_name = res[0]
            if len(table_name) == POI_SNAPSHOT:
                # 没有登录账户
                SEARCH_SQL = "select * from SEARCH_SNAPSHOT"     
                POSITION_SQL = "select * from POI_SNAPSHOT"   # home company collection address
                ROUTE_SQL = "select * from ROUTE_HISTORY_V2_SNAPSHOT"
                try:
                    cursor.execute(SEARCH_SQL)
                    search_results = cursor.fetchall()
                    for row in search_results:
                        a = row
                        # 0 item_id  1 data 2 history_type 3 adcode 4 update_time 5 deleted 6 stale
                        no_login_search = model_map.Search()
                        no_login_search.source = "高德地图"
                        no_login_search.sourceApp = "高德地图"
                        no_login_search.sourceFile = self.sourcefile
                        no_login_search.create_time = row[4]
                        no_login_search.adcode = row[3]
                        no_login_search.deleted = row[5]
                        if row[1]:
                            no_login_search = self.parse_search_json(no_login_search, row[1])
                        try:
                            self.gaodemap.db_insert_table_search(no_login_search)
                        except Exception as e:
                            pass
                except Exception as e:
                    print(e)
                
                    print("TABLE----SEARCH_SHANPSHOT is not exists!")

                try:
                    cursor.execute(ROUTE_SQL)
                    route_results = cursor.fetchall()
                    for row in route_results:
                        # 0 item id  1 type 2 route name 3 update_time 4 data 5 deleted 6 stale
                        no_login_route = model_map.Address()
                        no_login_route.source = "高德地图"
                        no_login_route.sourceApp = "高德地图"
                        no_login_route.sourceFile = self.sourcefile
                        no_login_route.create_time = row[3]
                        no_login_route.deleted = row[5]
                        if row[4]:
                            self.parse_route_json(no_login_route, row[4])
                        try:
                            self.gaodemap.db_insert_table_address(no_login_route)
                        except Exception as e:
                            pass
                except Exception as e:
                    print("TABLE----ROUTE_HISTORY_V2_SNAPSHOT is not exists!")
                
                try:
                    cursor.execute(POSITION_SQL)
                    poi_results = cursor.fetchall()
                    for row in poi_results:
                        # 0 id 1 item_id 2 name 3 point_x 4 point_y 5 top_time 6 create_time 7 city code 8 tag 9 new type 10 classifitciton
                        # 11 common name 12 custom_name 13 address 14 type 15 city_name 16 deleted 17 poiid 18 data 19 poiid_parsed
                        no_login_poi = model_map.Search()
                        no_login_poi.source = "高德地图"
                        no_login_poi.sourceApp = "高德地图"
                        no_login_poi.sourceFile = self.sourcefile
                        no_login_poi.item_type = 1
                        if row[6]:
                            no_login_poi.create_time = row[6]
                        no_login_poi.keyword = row[2]
                        no_login_poi.address = row[13]
                        if row[3]:
                            no_login_poi.pos_x = row[3]
                        if row[4]:
                            no_login_poi.pos_y = row[4]
                        no_login_poi.adcode = row[7]
                        no_login_poi.deleted = row[16]
                        try:
                            self.gaodemap.db_insert_table_search(no_login_poi)
                        except Exception as e:
                            pass
                except Exception as e:
                    print("POI_SNAPSHOT is not exists!")

            else:
                # 账户名
                user_id = table_name[POI_SNAPSHOT:]
                sql = "select * from {0}".format("USER"+str(user_id))
                cursor.execute(sql)
                records = cursor.fetchall()
                # 0 type  1  id  2 data  3 payload  4 ts
                for rec in records:
                    types = str(rec[0])
                    if types.startswith("10"):
                        # parse POI_SNAPSHOT
                        user_id_poi = model_map.Search()
						user_id_poi.account_id = str(user_id)
                        user_id_poi.source = "高德地图"
                        user_id_poi.sourceApp = "高德地图"
                        user_id_poi.sourceFile = self.sourcefile
                        user_id_poi.item_type = 1
                        user_id_poi.create_time = rec[4]
                        if rec[2]:
                            data_json = json.loads(rec[2])
                            user_id_poi.keyword = data_json.get("name")
                            user_id_poi.address = data_json.get("address")
                            user_id_poi.adcode = data_json.get("city_code")
                            user_id_poi.pos_x = data_json.get("point_x")
                            user_id_poi.pos_y = data_json.get("point_y")
                            try:
                                self.gaodemap.db_insert_table_search(user_id_poi)
                            except Exception as e:
                                pass

                    elif types.startswith("301"):
                        # parse SEARCH_SNAPSHOT
                        user_id_search = model_map.Search()
                        user_id_search.item_type = 0
						user_id_search.account_id = str(user_id)
                        user_id_search.source = "高德地图"
                        user_id_search.sourceApp = "高德地图"
                        user_id_search.sourceFile = self.sourcefile
                        user_id_search.account_id = user_id
                        user_id_search.create_time = rec[4]
                        if rec[2]:
                            user_id_search = self.parse_search_json(user_id_search, rec[2])
                        try:
                            self.gaodemap.db_insert_table_search(user_id_search)
                        except Exception as e:
                            pass

                    elif types.startswith("30"):
                        # parse ROUTE_HISTORY_V2_SNAPSHOT
                        user_id_route = model_map.Address()
						user_id_route.account_id = str(user_id)
                        user_id_route.source = "高德地图"
                        user_id_route.sourceApp = "高德地图"
                        user_id_route.sourceFile = self.sourcefile
                        user_id_route.account_id = user_id
                        user_id_route.create_time = rec[4]
                        if rec[2]:
                            user_id_route = self.parse_route_json(user_id_route, rec[2])
                        try:
                            self.gaodemap.db_insert_table_address(user_id_route)
                        except Exception as e:
                            pass   
                # parse have been deleted data
                try:
                    SEARCH_DELETED_SQL = "select * from SEARCH_SHANPSHOT" + user_id
                    search_deleted_results = cursor.fetchall()
                    for row in search_deleted_results:
                        # 0 item_id  1 data 2 history_type 3 adcode 4 update_time 5 deleted 6 stale
                        if row[5] == 1:
                            user_login_search = model_map.Search()
							user_login_search.account_id = str(user_id)
                            user_login_search.source = "高德地图"
                            user_login_search.sourceApp = "高德地图"
                            user_login_search.sourceFile = self.sourcefile
                            user_login_search.create_time = row[4]
                            user_login_search.adcode = row[3]
                            user_login_search.deleted = row[5]
                            if row[1]:
                                user_login_search = self.parse_search_json(user_login_search, row[1])
                            try:
                                self.gaodemap.db_insert_table_search(user_login_search)
                            except Exception as e:
                                pass

                except Exception as e:
                    tmpa = "SEARCH_SHANPSHOT{0}".format(user_id)
                    print(tmpa + "is not exists!")

                try:
                    POSITION_DELETED_SQL = "select * from POI_SNAPSHOT" + user_id
                    cursor.execute(POSITION_DELETED_SQL)
                    poi_deleted_results = cursor.fetchall()
                    for row in poi_deleted_results:
                        # 0 id 1 item_id 2 name 3 point_x 4 point_y 5 top_time 6 create_time 7 city code 8 tag 9 new type 10 classifitciton
                        # 11 common name 12 custom_name 13 address 14 type 15 city_name 16 deleted 17 poiid 18 data 19 poiid_parsed
                        if row[16] == 1:
                            user_login_poi = model_map.Search()
							user_login_poi.account_id = str(user_id)
                            user_login_poi.source = "高德地图"
                            user_login_poi.sourceApp = "高德地图"
                            user_login_poi.sourceFile = self.sourcefile
                            user_login_poi.item_type = 1
                            if row[6]:
                                user_login_poi.create_time = row[6]
                            user_login_poi.keyword = row[2]
                            user_login_poi.address = row[13]
                            if row[3]:
                                user_login_poi.pos_x = row[3]
                            if row[4]:
                                user_login_poi.pos_y = row[4]
                            user_login_poi.adcode = row[7]
                            user_login_poi.deleted = row[16]
                            try:
                                self.gaodemap.db_insert_table_search(user_login_poi)
                            except Exception as e:
                                pass
                except Exception as e:
                    tmpb = "POI_SNAPSHOT{0}".format(user_id)
                    print(tmpb + "is not exists!")

                try:
                    ROUTE_DELETED_SQL = "select * from ROUTE_HISTORY_V2_SNAPSHOT" + user_id
                    cursor.execute(ROUTE_DELETED_SQL)
                    route_deleted_results = cursor.fetchall()
                    for row in route_deleted_results:
                        # 0 item id  1 type 2 route name 3 update_time 4 data 5 deleted 6 stale
                        if row[5] == 1:
                            no_login_route = model_map.Address()
							no_login_route.account_id = str(user_id)
                            no_login_route.source = "高德地图"
                            no_login_route.sourceApp = "高德地图"
                            no_login_route.sourceFile = self.sourcefile
                            no_login_route.create_time = row[3]
                            no_login_route.deleted = row[5]
                            if row[4]:
                                self.parse_route_json(no_login_route, row[4])
                            try:
                                self.gaodemap.db_insert_table_address(no_login_route)
                            except Exception as e:
                                pass
                except Exception as e:
                    tmpc = "ROUTE_HISTORY_V2_SNAPSHOT{0}".format(user_id)
                    print(tmpc + "is not exists!")


        self.gaodemap.db_commit()
        self.gaodemap.db_close()


    def parse_search_json(self, models, data):
        """
        return models
        parse json data to models
        """
        json_data = json.loads(data)
        if "name" in json_data:
            models.keyword = json_data.get("name")
        if "adcode" in json_data:
            models.adcode = json_data.get("adcode")
        if "address" in json_data:
            models.address = json_data.get("address")
        if "district" in json_data:
            models.district = json_data.get("district")
        if "x" in json_data:
            models.pos_x = json_data.get("x")
        if "y" in json_data:
            models.pos_y = json_data.get("y")
        return models
        
    def parse_route_json(self, models, data):
        """
        return models
        parse json data to models
        city_code is available in gaodemap but no add in models 
        """
        json_data = json.loads(data)
        if "from_poi_json" in json_data:
            models.from_name = json_data.get("from_poi_json").get("mName")
            models.from_addr = json_data.get("from_poi_json").get("mAddr")
            models.from_posX = json_data.get("from_poi_json").get("mX")
            models.from_posY = json_data.get("from_poi_json").get("mY")
        if "to_poi_json" in json_data:
            models.to_name = json_data.get("to_poi_json").get("mName")
            models.to_addr = json_data.get("to_poi_json").get("mAddr")
            models.to_posX = json_data.get("to_poi_json").get("mX")
            models.to_posY = json_data.get("to_poi_json").get("mY")
        return models


    def check_to_update(self, path_db, appversion):
        if os.path.exists(path_db) and path_db[-6:-3] == appversion:
            return False
        else:
            return True


    def parse(self):
        
        decode_db_path = self.cache + "/girf_sync_decode.db"
        db_path = self.cache + "/gaode_db_1.0.db"
        
        if not os.path.exists(decode_db_path):
            self.decode_db()

        if self.check_to_update(db_path, APPVERSION):
            self.gaodemap.db_create(db_path)
            self.entrance()
            self.gaodemap.db_close()
        
        generate = model_map.Genetate(db_path)
        tmpresult = generate.get_models()
        return tmpresult
      

def analyze_gaodemap(node, extract_deleted, extract_source):
    
    pr = ParserResults()
    prResult = gaodeMap(node, extract_deleted, extract_source).parse()
    if prResult:
        for i in prResult:
            pr.Models.Add(i)
    pr.Build("高德地图")
    return pr

def execute(node, extract_deleted):
    return analyze_gaodemap(node, extract_deleted, False)