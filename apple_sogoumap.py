#coding=utf-8
import os
import PA_runtime
from PA_runtime import *
import json
import clr
try:
    clr.AddReference('model_map')
except:
    pass
del clr
import model_map

class SogouMap(object):

    def __init__(self, node, extract_deleted, extract_source):
        self.root = node
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.sogoudb = model_map.Map()
        self.db_cache = ds.OpenCachePath("sogouMap")
    

    def parse_history_data(self):
        history_node = self.root.GetByPath("Documents/MapData.data")
        if history_node is None:
            return
        try:
            mapdb = SQLiteParser.Database.FromNode(history_node, canceller)
            if mapdb is None:
                print("Documents/MapData.data is not exists!")
                return 
            tb = SQLiteParser.TableSignature("ZSGMHISTORYENTITY")
            if self.extract_deleted:
                SQLiteParser.Tools.AddSignatureToTable(tb, "ZCAPTION", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            
            for rec in mapdb.ReadTableRecords(tb, self.extract_deleted, self.extract_source):
                if canceller.IsCancellationRequested:
                    return
                search = model_map.Search()
                search.source = "搜狗地图"
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
        if user_node is None:
            return
        try:
            mapdb = SQLiteParser.Database.FromNode(user_node, canceller)
            if mapdb is None:
                print("Documents/MapData.data is not exists!")
                return
            tb = SQLiteParser.TableSignature("ZSGMUSERDATAENTITY")
            if self.extract_deleted:
                pass
            for rec in mapdb.ReadTableRecords(tb, self.extract_deleted, self.extract_source):
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
        if addr_node is None:
            return
        try:
            mapdb = SQLiteParser.Database.FromNode(addr_node, canceller)
            if mapdb is None:
                return
            tbs = SQLiteParser.TableSignature("ZSGMFAVORITEENTITY")
            if self.extract_deleted:
                SQLiteParser.Tools.AddSignatureToTable(tbs, "ZCAPTION", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in mapdb.ReadTableRecords(tbs, self.extract_deleted, self.extract_source):
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
                #     b = bytes(rec["ZATTRIBUTES"].Value)
                #     try:
                #         jsonfile = b.decode("utf-8")
                #         ztime =  json.loads(jsonfile)
                #     except Exception as e:
                #         pass
                #     if "localVerAtt" in ztime:
                #         search.create_time = int(ztime["localVerAtt"][0:10])
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
                self.sogoudb.db_insert_table_address(route_address)
            except Exception as e:
                print(e)

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

    # def check_to_update(self, path_db, appversion):
    #     if os.path.exists(path_db) and path_db[-6:-3] == appversion:
    #         return False
    #     else:
    #         return True


    def parse(self):
        
        db_path = self.db_cache + "/sogou_db_1.0.db"
        # if self.check_to_update(db_path, APPVERSION):
        self.sogoudb.db_create(db_path)
        self.parse_history_data()
        self.parse_user_data()
        self.parse_favorites_data()
        self.parse_route_data()
        self.sogoudb.db_close()

        generate = model_map.Genetate(db_path)   
        tmpresult = generate.get_models()
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