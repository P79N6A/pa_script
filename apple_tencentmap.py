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

# 想重新分析并生成数据库 在这里修改一下版本号
# APPVERSION = "1.0"

class TencentMap(object):

    def __init__(self, node, extract_deleted, extract_source):
        self.root = node
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.tencentMap = model_map.Map()
        self.db_cache = ds.OpenCachePath("tencentMap")
        self.account_id = None

    def get_account_info(self):
        """
        分析账号信息
        """
        accountNode = self.root.GetByPath("Library/Preferences/com.tencent.sosomap.plist")
        if accountNode is None:
            #raise Exception("can not find account node")
            return 
        account_bplist = BPReader.GetTree(accountNode)
        if account_bplist is None:
            return 
        account = model_map.Account()
        account.sourceFile = accountNode.AbsolutePath
        if "loginKeysDictionary_OpenQQAPI" in account_bplist:
            if "fullName" in account_bplist["loginKeysDictionary_OpenQQAPI"]:
                account.username = account_bplist["loginKeysDictionary_OpenQQAPI"]["fullName"].Value
            if "faceUrl" in account_bplist["loginKeysDictionary_OpenQQAPI"]:
                account.photo = account_bplist["loginKeysDictionary_OpenQQAPI"]["faceUrl"].Value
            if "userID" in account_bplist["loginKeysDictionary_OpenQQAPI"]:
                self.account_id = account_bplist["loginKeysDictionary_OpenQQAPI"]["userID"].Value
                account.account_id = self.account_id
                account.source = "腾讯地图"
            try:
                self.tencentMap.db_insert_table_account(account)
            except Exception as e:
                pass
            self.tencentMap.db_commit()
    
    def get_search_data(self):
        """
        分析搜索记录
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
            self.decode_search_data(bplist["$objects"], uid.Value, search_node)
        self.tencentMap.db_commit()

    def decode_search_data(self, bp, dictvalue, search_node):
        search = model_map.Search()
        search.source = "腾讯地图"
        search.sourceFile = search_node.AbsolutePath
        values = self.get_dict_from_search(bp, dictvalue)
        if "name" in values:
            search.keyword = values.get("name").Value
        if "address" in values:
            search.address = values.get("address").Value
        if "pointX" in values:
            search.pos_x = values.get("pointX") if values.get("pointX") else 0
        if "pointY" in values:
            search.pos_y = values.get("pointY") if values.get("pointY") else 0
        try:
            self.tencentMap.db_insert_table_search(search)
        except Exception as e:
            pass
        

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
        f = open(favorites_address_node.PathWithMountPoint,"r")
        tmp = f.read()
        json_data = json.loads(tmp)
        if json_data.get("fav_list"):
            favorite_search = model_map.Search()
            favorite_search.source = "腾讯地图"
            favorite_search.sourceFile = favorites_address_node.AbsolutePath
            favorite_search.item_type = 1
            for rs in json_data.get("fav_list"):
                favorite_search.keyword = rs.get("name").decode("utf-8")
                if "last_edit_time" in rs:
                    favorite_search.create_time = rs.get("last_edit_time")
                if "pointx" in rs.get("content"):
                    favorite_search.pos_x = rs.get("content").get("pointx")
                if "pointy" in rs.get("content"):
                    favorite_search.pos_y = rs.get("content").get("pointy")
                if "addr" in rs.get("content"):
                    favorite_search.address = rs.get("content").get("addr").decode("utf-8")
                try:
                    self.tencentMap.db_insert_table_search(favorite_search)
                except Exception as e:
                    print(favorite_search)
        f.close()
        self.tencentMap.db_commit()

    def get_home_company(self):
        """
        分析家和公司信息
        """
        a = self.root
        x = 5
        home_company_node = self.root.GetByPath("Documents/user/frequentAddressArray.dat")
        if home_company_node is None:
            return
        bp = BPReader(home_company_node.Data).top
        for uid in bp['$objects'][1]['NS.objects']:
            if canceller.IsCancellationRequested:
                return
            if uid is None:       
                break
            self.decode_home_company(bp["$objects"], uid.Value, home_company_node)
        self.tencentMap.db_commit()

    def decode_home_company(self, bp, dictvalue, home_company_node):        
        values = self.get_dict_from_bplist(bp, dictvalue)
        if values:
            home_company = model_map.Search()
            home_company.source = "腾讯地图"
            home_company.sourceFile = home_company_node.AbsolutePath
            home_company.item_type = 1
            if "name" in values:
                home_company.keyword = values.get("name").Value
            if "address" in values:
                home_company.address = values.get("address").Value
            if "longitude" in values:
                home_company.pos_x = values.get("longitude") if values.get("longitude") else 0
            if "latitude" in values:
                home_company.pos_y = values.get("latitude") if values.get("latitude") else 0
            try:
                if values.get("name") is not None:
                    self.tencentMap.db_insert_table_search(home_company)
            except Exception as e:
                pass

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

    def decode_route(self, bp, dictvalue, route):
        startvalues, endvalues = self.get_route_from_dict(bp, dictvalue)
        if startvalues and endvalues:
            route_address = model_map.Address()
            route_address.source = "腾讯地图"
            route_address.sourceFile = route.AbsolutePath
            if "name" in startvalues and "name" in endvalues:
                route_address.from_name = startvalues.get("name").Value
                route_address.to_name = endvalues.get("name").Value
            if "addr" in startvalues and "addr" in endvalues:
                if startvalues["addr"] != "$null":
                    route_address.from_addr = startvalues.get("addr").Value
                if endvalues["addr"] != "$null":
                    route_address.to_addr = endvalues.get("addr").Value
            if "pointX" in startvalues and "pointX" in endvalues:
                route_address.from_posX = startvalues.get("pointX") if startvalues.get("pointX") else 0
                route_address.to_posX = endvalues.get("pointX") if endvalues.get("pointX") else 0
            if "pointY" in startvalues and "pointY" in endvalues:
                route_address.from_posY = startvalues.get("pointY") if startvalues.get("pointY") else 0
                route_address.to_posY = endvalues.get("pointY") if endvalues.get("pointY") else 0
            if "cityName" in startvalues and "cityName" in endvalues:
                if startvalues["cityName"] != "$null":
                    route_address.city_name = startvalues.get("cityName").Value
                if endvalues["cityName"] != "$null":
                    route_address.city_name = endvalues.get("cityName").Value
            try:
                self.tencentMap.db_insert_table_address(route_address)
            except Exception as e:
                print(e)

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

    # def check_to_update(self, path_db, appversion):
    #     if os.path.exists(path_db) and path_db[-6:-3] == appversion:
    #         return False
    #     else:
    #         return True
            
    def parse(self):
        db_path = self.db_cache + "/tencent_db_1.0.db"
        self.tencentMap.db_create(db_path)
        self.get_account_info() 
        self.get_favorites_address()    # 得到收藏夹地址
        self.get_search_data()          # 得到搜索记录
        self.get_home_company()         # 得到公司和家地址
        self.get_route_by_bus()         # 得到导航记录通过公交
        self.get_route_by_car()         # 得到导航记录通过汽车
        self.get_route_by_walk()        # 得到导航记录通过步行
        self.tencentMap.db_close()
        generate = model_map.Genetate(db_path)   
        tmpresult = generate.get_models()
        return tmpresult
        

def analyze_tencentmap(node, extract_deleted, extract_source):
    """
    tencentMap
    data source: ["Library/Preferences/com.tencent.sosomap.plist","Documents/user/.*.dat",]
    search rules: ("com.tencent.sosomap", analyze_tencentmap, "TencentMap", "腾讯地图", DescripCategories.TencentMap)
    return: account, RouteRecord, PoiRecord
    """
    pr = ParserResults()
    results = TencentMap(node, extract_deleted, extract_source).parse()
    if results:
        for i in results:
            pr.Models.Add(i)
    pr.Build("腾讯地图")
    return pr


def execute(node, extract_deleted):
    return analyze_tencentmap(node, extract_deleted, False)