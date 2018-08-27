#coding=utf-8
import os
import PA_runtime
from PA_runtime import *
import model_map
import json


APPVERSION = "1.0"

class SogouMap(object):

    def __init__(self, node, extract_deleted, extract_source):
        self.root = node
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.sogoudb = model_map.Map()
        self.db_cache = ds.OpenCachePath("sogouMap")
    

    def parse_history_data(self):
        history_node = self.root.GetByPath("Documents/MapData.data")
        try:
            mapdb = SQLiteParser.Database.FromNode(history_node)
            if mapdb is None:
                print("Documents/MapData.data is not exists!")
                return 

            tb = SQLiteParser.TableSignature("ZSGMHISTORYENTITY")
            if self.extract_deleted:
                SQLiteParser.Tools.AddSignatureToTable(tb, "ZCAPTION", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            
            for rec in mapdb.ReadTableRecords(tb, self.extract_deleted, self.extract_source):
                search = model_map.Search()
                search.source = "搜狗地图:"
                search.sourceApp = "搜狗地图"
                search.sourceFile = history_node.AbsolutePath
                if rec.Deleted == DeletedState.Deleted:
                    search.deleted = 1
                elif rec.Deleted == DeletedState.Intact:
                    search.deleted = 0
                if "ZCAPTION" in rec and (not rec["ZCAPTION"].IsDBNull):
                    search.keyword = rec["ZCAPTION"].Value
                if "ZADDRESS" in rec and (not rec["ZADDRESS"].IsDBNull):
                    search.address = rec["ZADDRESS"].Value
                if rec["ZADDRESS"].IsDBNull and (not rec["ZDESC"].IsDBNull):
                    search.address = rec["ZDESC"].Value
                if rec["ZPOINTX"] in rec and (not rec["ZPOINTX"].IsDBNull):
                    search.pos_x = int(rec["ZPOINTX"].Value)
                if rec["ZPOINTY"] in rec and (not rec["ZPOINTY"].IsDBNull):
                    search.pos_y = int(rec["ZPOINTY"].Value)
                try:
                    self.sogoudb.db_insert_table_search(search)
                except Exception as e:
                    pass    
        except Exception as e:
            pass
        finally:
            self.sogoudb.db_commit()
    
    def parse_user_data(self):
        user_node = self.root.GetByPath("Documents/MapData.data")
        try:
            mapdb = SQLiteParser.Database.FromNode(user_node)
            if mapdb is None:
                print("Documents/MapData.data is not exists!")
                return
            tb = SQLiteParser.TableSignature("ZSGMUSERDATAENTITY")
            if self.extract_deleted:
                pass
            for rec in mapdb.ReadTableRecords(tb, self.extract_deleted, self.extract_source):
                user = model_map.Account()
                user.sourceApp = "搜狗地图"
                user.sourceFile = user_node.AbsolutePath
                if rec.Deleted == DeletedState.Deleted:
                    user.deleted = 1
                elif rec.Deleted == DeletedState.Intact:
                    user.deleted = 0
                if "ZNAME" in rec and (not rec["ZNAME"].IsDBNull):
                    user.nickname = rec["ZNAME"].Value
                if "ZSEX" in rec and (not rec["ZSEX"].IsDBNull):
                    user.gender = rec["ZSEX"].Value
                if "ZUSERID" in rec and (not rec["ZUSERID"].IsDBNull):
                    user.account_id = rec["ZUSERID"].Value
                    user.source = "搜狗地图:" + rec["ZUSERID"].Value
                try:
                    self.sogoudb.db_insert_table_account(user)
                except Exception as e:
                    pass
        except Exception as e:
            pass
        finally:
            self.sogoudb.db_commit()

    def parse_favorites_data(self):
        addr_node = self.root.GetByPath("Documents/MapData.data")
        try:
            mapdb = SQLiteParser.Database.FromNode(addr_node)
            if mapdb is None:
                return
            tbs = SQLiteParser.TableSignature("ZSGMFAVORITEENTITY")
            if self.extract_deleted:
                SQLiteParser.Tools.AddSignatureToTable(tbs, "ZCAPTION", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in mapdb.ReadTableRecords(tbs, self.extract_deleted, self.extract_source):
                search = model_map.Search()
                search.sourceApp = "搜狗地图"
                search.sourceFile = addr_node.AbsolutePath
                search.item_type = 1  # 类型是收藏的点
                if rec.Deleted == DeletedState.Deleted:
                    search.deleted = 1
                elif rec.Deleted == DeletedState.Intact:
                    search.deleted = 0
                if "ZCAPTION" in rec and (not rec["ZCAPTION"].IsDBNull) and rec["ZTYPE"].Value == 105:
                    search.keyword = rec["ZCAPTION"].Value
                if "ZUSERID" in rec and (not rec["ZUSERID"].IsDBNull) and rec["ZTYPE"].Value == 105:
                    search.account_id = rec["ZUSERID"].Value
                    search.source = "搜狗地图:" + rec["ZUSERID"].Value
                if "ZATTRIBUTES" in rec and (not rec["ZATTRIBUTES"].IsDBNull):
                    b = bytes(rec["ZATTRIBUTES"].Value)
                    jsonfile = b.decode("utf-8")
                    ztime =  json.loads(jsonfile)
                    if ztime and "localVerAtt" in ztime:
                        search.create_time = int(ztime["localVerAtt"][0:10])
                try:
                    self.sogoudb.db_insert_table_search(search)
                except Exception as e:
                    print(e)
        except Exception as e:
            print(e)
        finally:
            self.sogoudb.db_commit()

    def parse_route_data(self):
        route_addr =  self.root.GetByPath("Documents/BusHistory.plist")
        if route_addr is None:
            return 
        bplist = BPReader(route_addr.Data).top
        for uid in bplist["$objects"][1]["NS.objects"]:
            if uid is None:
                break
            self.decode_route(bplist["$objects"], uid.Value, route_addr)


    def decode_route(self, bp, uid, roure_addr):
        values = self.get_route_from_dict(bp, uid)
        if values:
            pass

    def get_route_from_dict(self, bp, uid):
        values = {}
        # if bp[dictvalue].__getattribute__("Keys"):
        attrs = dir(bp[dictvalue])
        if "Keys" in attrs:
            for key in bp[dictvalue].Keys:
                if key in ["bus.history.type"]:
                    values[key] =  bp[dictvalue][key].Value
                else:
                    tmp = bp[dictvalue][key].Value
                    values[key] = bp[tmp]
        else:
            pass
        return values


    def parse(self):

        self.parse_history_data()
        self.parse_user_data()
        self.parse_favorites_data()
        #self.parse_route_data()

    
def analyze_sogoumap(node, extract_deleted, extract_source):
    pr = ParserResults()
    results = SogouMap(node, extract_deleted, extract_source).parse()
    if results:
        for i in results:
            pr.Models.Add(i)
    return pr