#coding=utf-8
import os
import PA_runtime
from PA_runtime import *
import json
import clr
try:
    clr.AddReference('model_map')
    clr.AddReference("bcp_gis")
except:
    pass
del clr
import model_map
import bcp_gis
import hashlib

class SogouMap(object):

    def __init__(self, node, extract_deleted, extract_source):
        self.root = node
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.sogoudb = model_map.Map()
        self.db_cache = ds.OpenCachePath("sogouMap")
        self.tmp_dir = ds.OpenCachePath("tmp")
    
    # 历史搜索记录
    def parse_history_data(self):
        history_node = self.root
        # history_node = self.root.GetByPath("Documents/MapData.data")
        if history_node is None:
            return
        try:
            mapdb = SQLiteParser.Database.FromNode(history_node, canceller)
            if mapdb is None:
                return
            if "ZSGMHISTORYENTITY" not in mapdb.Tables:
                return
            tb = SQLiteParser.TableSignature("ZSGMHISTORYENTITY")
            if self.extract_deleted:
                SQLiteParser.Tools.AddSignatureToTable(tb, "ZCAPTION", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in mapdb.ReadTableRecords(tb, self.extract_deleted, self.extract_source):
                try:
                    if canceller.IsCancellationRequested:
                        return
                    search = model_map.Search()
                    search.source = "搜狗地图"
                    search.sourceApp = "搜狗地图"
                    search.sourceFile = history_node.AbsolutePath
                    if rec.Deleted == DeletedState.Deleted:
                        search.deleted = 1
                    # elif rec.Deleted == DeletedState.Intact:
                    #     search.deleted = 0
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
                    if search.keyword or search.address or search.pos_x or search.pos_y:
                        self.sogoudb.db_insert_table_search(search)
                except Exception as e:
                    pass    
        except Exception as e:
            pass
        self.sogoudb.db_commit()
    
    def parse_user_data(self):
        # user_node = self.root.GetByPath("Documents/MapData.data")
        user_node = self.root
        if user_node is None:
            return
        try:
            mapdb = SQLiteParser.Database.FromNode(user_node, canceller)
            if mapdb is None:
                return
            if "ZSGMUSERDATAENTITY" not in mapdb.Tables:
                return
            tb = SQLiteParser.TableSignature("ZSGMUSERDATAENTITY")
            for rec in mapdb.ReadTableRecords(tb, self.extract_deleted, self.extract_source):
                try:
                    if canceller.IsCancellationRequested:
                        return
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
                        user.source = "搜狗地图" 
                    if user.account_id or user.nickname or user.gender:
                        self.sogoudb.db_insert_table_account(user)
                except Exception as e:
                    pass
        except Exception as e:
            pass
        finally:
            self.sogoudb.db_commit()

    def parse_favorites_data(self):
        # addr_node = self.root.GetByPath("Documents/MapData.data")
        addr_node = self.root
        if addr_node is None:
            return
        try:
            mapdb = SQLiteParser.Database.FromNode(addr_node, canceller)
            if mapdb is None:
                return
            if "ZSGMFAVORITEENTITY" not in mapdb.Tables:
                return
            tbs = SQLiteParser.TableSignature("ZSGMFAVORITEENTITY")
            if self.extract_deleted:
                SQLiteParser.Tools.AddSignatureToTable(tbs, "ZCAPTION", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in mapdb.ReadTableRecords(tbs, self.extract_deleted, self.extract_source):
                try:
                    if canceller.IsCancellationRequested:
                        return
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
                        search.source = "搜狗地图"
                    # if "ZATTRIBUTES" in rec and (not rec["ZATTRIBUTES"].IsDBNull):
                    #     try:
                    #         dates = rec["ZATTRIBUTES"].Value
                    #         dstart = DateTime(1970,1,1,0,0,0)
                    #         cdate = TimeStampFormats.GetTimeStampEpoch1Jan2001(rec["ZATTRIBUTES"].Value)
                    #         search.create_time = TimeStamp.FromUnixTime(int((cdate - dstart).TotalSeconds), False)
                    #     except Exception as e:
                    #         pass
                    if search.keyword or search.account_id or search.create_time:
                        self.sogoudb.db_insert_table_search(search)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error,"{0}".format(e))
        except Exception as e:
            TraceService.Trace(TraceLevel.Error,"{0}".format(e))
        finally:
            self.sogoudb.db_commit()

    def parse_route_data(self):
        route_addr =  self.root.Parent.GetByPath("BusHistory.plist")
        if route_addr is None:
            return 
        bplist = BPReader(route_addr.Data).top
        for uid in bplist["$objects"][1]["NS.objects"]:
            if uid is None:
                break
            self.decode_route(bplist["$objects"], uid.Value, route_addr)
        self.sogoudb.db_commit()

    def decode_route(self, bp, uid, roure_addr):
        values = self.get_route_from_dict(bp, uid)
        if values:
            route_address = model_map.Address()
            route_address.source = "搜狗地图"
            route_address.sourceApp = "搜狗地图"
            route_address.sourceFile = roure_addr.AbsolutePath
            if "bus.history.startname" in values:
                route_address.from_name = values.get("bus.history.startname").Value
            if "bus.history.endname" in values:
                route_address.to_name = values.get("bus.history.endname").Value
            try:
                if route_address.from_name or route_address.to_name:
                    self.sogoudb.db_insert_table_address(route_address)
            except Exception as e:
                pass

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

    def md5(self, cache_path, node_path):
        m = hashlib.md5()   
        m.update(node_path.encode(encoding = 'utf-8'))
        db_path = cache_path + "\\" + m.hexdigest() + ".db"
        return db_path

    def parse(self):
        
        db_path = self.md5(self.db_cache, self.root.AbsolutePath)

        self.sogoudb.db_create(db_path)
        self.parse_history_data()
        self.parse_user_data()
        self.parse_favorites_data()
        self.parse_route_data()
        self.sogoudb.db_close()

        generate = model_map.Genetate(db_path)   
        tmpresult = generate.get_models()
        PA_runtime.save_cache_path(bcp_gis.NETWORK_APP_MAP_SOGOU,db_path, self.tmp_dir)
        return tmpresult

    
def analyze_sogoumap(node, extract_deleted, extract_source):
    pr = ParserResults()
    results = SogouMap(node, extract_deleted, extract_source).parse()
    if results:
        for i in results:
            pr.Models.Add(i)
    pr.Build("搜狗地图")
    return pr


def execute(node, extract_deleted):
    return analyze_sogoumap(node, extract_deleted, False)