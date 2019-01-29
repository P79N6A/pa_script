#coding:utf-8

import clr
from PA_runtime import *
try:
    clr.AddReference('System.Data.SQLite')
    clr.AddReference('MapUtil')
except:
    pass
import System.Data.SQLite as SQLite
from PA.InfraLib.ModelsV2 import *
from PA.InfraLib.ModelsV2.Map import *
from PA.InfraLib.ModelsV2.CommonEnum import CoordinateType,LocationSourceType
import sqlite3
import MapUtil
import os


LOCATION_TYPE_OTHERR = 0    #其他地址
LOCATION_TYPE_HOME = 1      #家庭地址
LOCATION_TYPE_COMPANY = 2   #公司地址


LOCATION_TYPE_GPS = 1  # GPS坐标
LOCATION_TYPE_GPS_MC = 2  # GPS米制坐标
LOCATION_TYPE_GOOGLE = 3  # GCJ02坐标
LOCATION_TYPE_GOOGLE_MC = 4  # GCJ02米制坐标
LOCATION_TYPE_BAIDU = 5  # 百度经纬度坐标
LOCATION_TYPE_BAIDU_MC = 6  # 百度米制坐标
LOCATION_TYPE_MAPBAR = 7  # mapbar地图坐标
LOCATION_TYPE_MAP51 = 8  # 51地图坐标


SQL_CREATE_TABLE_SEARCH = '''
    create table if not exists search(
        account_id TEXT, 
        keyword TEXT,
        create_time INT,
        address TEXT,
        pos_x REAL,
        pos_y REAL,
        type INT,
        sourceApp TEXT,
        sourceFile TEXT,
        deleted INT DEFAULT 0
        )'''

SQL_INSERT_TABLE_SEARCH = '''
    insert into search(account_id,keyword,create_time,address,pos_x,pos_y,type,sourceApp,sourceFile,deleted)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

SQL_CREATE_TABLE_FAVPOI = '''
    create table if not exists favpoi(
        account_id TEXT, 
        poi_name TEXT,
        city_id INT,
        city_name TEXT,
        pos_type INT DEFAULT 0,
        create_time INT,
        fav_obj TEXT,
        sourceApp TEXT,
        sourceFile TEXT,
        deleted INT DEFAULT 0
        )'''

SQL_INSERT_TABLE_FAVPOI = '''
    insert into favpoi(account_id,poi_name,city_id,city_name,pos_type,create_time,fav_obj,sourceApp,sourceFile,deleted)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''


SQL_CREATE_TABLE_ROUTEREC = '''
    create table if not exists routerec(
        account_id TEXT,
        from_name TEXT,
        from_posX REAL,
        from_posY REAL,
        from_addr TEXT,
        to_name TEXT,
        to_posX REAL,
        to_posY REAL,
        to_addr TEXT,
        create_time INT,
        nav_id TEXT,
        type INT,
        sourceApp TEXT,
        sourceFile TEXT,
        deleted INT DEFAULT 0
        )'''

SQL_INSERT_TABLE_ROUTEREC = '''
    insert into routerec(account_id, from_name, from_posX, from_posY, from_addr, to_name, to_posX,
     to_posY, to_addr, create_time, nav_id, type, sourceApp, sourceFile, deleted)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

SQL_CREATE_TABLE_LOCATION = '''
    create table if not exists location(
        location_id TEXT,
        latitude REAL,
        longitude REAL,
        elevation REAL,
        address TEXT,
        timestamp INT,
        type INT,
        sourceApp TEXT,
        sourceFile TEXT, 
        deleted INT DEFAULT 0)'''

SQL_INSERT_TABLE_LOCATION = '''
    insert into location(location_id, latitude, longitude, elevation, address, timestamp, type, sourceApp, sourceFile, deleted) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''


SQL_CREATE_TABLE_FAVROUTE = '''
    create table if not exists favroute(
        account_id TEXT,
        from_name TEXT,
        from_posX REAL,
        from_posY REAL,
        from_addr TEXT,
        to_name TEXT,
        to_posX REAL,
        to_posY REAL,
        to_addr TEXT,
        create_time INT,
        nav_id TEXT,
        type INT,
        sourceApp TEXT,
        sourceFile TEXT,
        deleted INT DEFAULT 0
        )'''

SQL_INSERT_TABLE_FAVROUTE = '''
    insert into favroute(account_id, from_name, from_posX, from_posY, from_addr, to_name, to_posX,
     to_posY, to_addr, create_time, nav_id, type, sourceApp, sourceFile, deleted)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''


class Map(object):
    
    def __init__(self):
        self.db = None
        self.db_command = None
        self.db_trans = None

    def db_create(self, db_path):
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except Exception as e:
                print("{0} remove failed!".format(db_path))
        
        self.db = SQLite.SQLiteConnection("Data Source = {0}".format(db_path))
        self.db.Open()
        self.db_command = SQLite.SQLiteCommand(self.db)
        self.db_trans = self.db.BeginTransaction()

        self.db_create_table()
        self.db_commit()

    def db_close(self):
        self.db_trans = None
        if self.db_command is not None:
            self.db_command.Dispose()
            self.db_command = None
        if self.db is not None:
            self.db.Close()
            self.db = None

    def db_commit(self):
        if self.db_trans is not None:
            try:
                self.db_trans.Commit()
            except Exception as e:
                self.db_trans.RollBack()
        self.db_trans = self.db.BeginTransaction()

    def db_create_table(self):
        if self.db_command is not None:
            self.db_command.CommandText = SQL_CREATE_TABLE_SEARCH
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = SQL_CREATE_TABLE_FAVPOI
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = SQL_CREATE_TABLE_ROUTEREC
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = SQL_CREATE_TABLE_LOCATION
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = SQL_CREATE_TABLE_FAVROUTE
            self.db_command.ExecuteNonQuery()

    def db_insert_table(self, sql, values):
        if self.db_command is not None:
            self.db_command.CommandText = sql
            self.db_command.Parameters.Clear()
            for value in values:
                param = self.db_command.CreateParameter()
                param.Value = value
                self.db_command.Parameters.Add(param)
            self.db_command.ExecuteNonQuery()

    def db_insert_table_search(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_SEARCH, column.get_values())

    def db_insert_table_favpoi(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_FAVPOI, column.get_values())

    def db_insert_table_routerec(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_ROUTEREC, column.get_values())

    def db_insert_table_location(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_LOCATION, column.get_values())
    
    def db_insert_table_favroute(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_FAVROUTE, column.get_values())



nav_id = 1
loc_id = 2

class Column(object):

    def __init__(self):
        self.sourceApp = ""
        self.sourceFile = ""
        self.deleted = 0

    # def __setattr__(self, name, value):
    #     if not IsDBNull(value):
    #         if isinstance(value, str):
    #             # 过滤控制字符, 防止断言失败
    #             value = re.compile('[\\x00-\\x08\\x0b-\\x0c\\x0e-\\x1f]').sub(' ', value)               
    #         self.__dict__[name] = value

    def get_values(self):
        return self.sourceApp, self.sourceFile, self.deleted


class Search(Column):
    
    def __init__(self):
        super(Search, self).__init__()
        self.account_id = None      # 账户id [TEXT]
        self.keyword = None         # 搜索关键词 [TEXT]
        self.create_time = None     # 搜索时间 [INT]
        self.address = None         # 搜索地址 [TEXT]
        self.pos_x = None           # 搜索经度 [REAL]
        self.pos_y = None           # 搜索纬度 [REAL]
        self.type = LOCATION_TYPE_GPS

    def get_values(self):
        return (self.account_id, self.keyword, self.create_time, self.address,
        self.pos_x, self.pos_y, self.type) + super(Search, self).get_values()


class FavPoi(Column):

    def __init__(self):
        super(FavPoi, self).__init__()
        self.account_id = None      # 账户id [TEXT]
        self.poi_name = None        # 搜索关键词 [TEXT]
        self.city_id = None         # 城市id [INT]
        self.city_name = None       # 城市名称 [TEXT]
        self.pos_type = LOCATION_TYPE_OTHERR  # 位置类型 [INT]
        self.create_time = None     # 收藏时间 [INT]
        self.fav_obj = None
        # self.address = None         # 收藏地址 [TEXT]
        # self.pos_x = None           # 经度 [REAL]
        # self.pos_y = None           # 维度 [REAL]

    def get_values(self):
        return (self.account_id, self.poi_name, self.city_id, self.city_name,
        self.pos_type, self.create_time, self.fav_obj) + super(FavPoi, self).get_values()


class RouteRec(Column):

    def __init__(self):
        super(RouteRec, self).__init__()
        self.account_id = None      # 账户id [TEXT]
        self.from_name = None       # 起点 [TEXT]
        self.from_posX = None       # 起点经度 [REAL]
        self.from_posY = None       # 起点纬度 [REAL]
        self.from_addr = None       # 起点地址 [TEXT]
        self.to_name = None         # 目的地 [TEXT]
        self.to_posX = None         # 目的地经度 [REAL]
        self.to_posY = None         # 目的地纬度 [REAL]
        self.to_addr = None         # 目的地地址 [TEXT]
        self.create_time = None     # 目的地搜索时间 [INT]
        self.nav_id = None
        self.type = LOCATION_TYPE_GPS 


    def get_values(self):
        return (self.account_id,self.from_name,self.from_posX,self.from_posY,
        self.from_addr,self.to_name,self.to_posX,self.to_posY,self.to_addr,self.create_time,self.nav_id,self.type) + super(RouteRec, self).get_values()
    

# 某坐标信息
class Location(Column):
    def __init__(self):
        super(Location, self).__init__()
        global loc_id
        self.location_id = loc_id # 地址ID[TEXT]
        loc_id += 2
        self.latitude = None  # 纬度[REAL]
        self.longitude = None  # 经度[REAL]
        self.elevation = None  # 海拔[REAL]
        self.address = None  # 地址名称[TEXT]
        self.timestamp = None  # 时间戳[INT]
        self.type = LOCATION_TYPE_GPS  # 坐标系[INT]  LOCATION_TYPE

    def get_values(self):
        return (self.location_id, self.latitude, self.longitude, self.elevation, self.address, 
                self.timestamp, self.type) + super(Location, self).get_values()


class FavRoute(Column):

    def __init__(self):
        super(FavRoute, self).__init__()
        global nav_id
        self.account_id = None      # 账户id [TEXT]
        self.from_name = None       # 起点 [TEXT]
        self.from_posX = None       # 起点经度 [REAL]
        self.from_posY = None       # 起点纬度 [REAL]
        self.from_addr = None       # 起点地址 [TEXT]
        self.to_name = None         # 目的地 [TEXT]
        self.to_posX = None         # 目的地经度 [REAL]
        self.to_posY = None         # 目的地纬度 [REAL]
        self.to_addr = None         # 目的地地址 [TEXT]
        self.create_time = None     # 目的地搜索时间 [INT]
        self.nav_id = nav_id
        nav_id += 2
        self.type = LOCATION_TYPE_GPS 


    def get_values(self):
        return (self.account_id,self.from_name,self.from_posX,self.from_posY,
        self.from_addr,self.to_name,self.to_posX,self.to_posY,self.to_addr,self.create_time,self.nav_id,self.type) + super(FavRoute, self).get_values()
 


class ExportModel(object):

    def __init__(self, db_path):
        self.db_path = db_path

    def get_model(self):
        models = []

        self.db = sqlite3.connect(self.db_path)
        self.cursor = self.db.cursor()

        models.extend(self._get_search_models())
        models.extend(self._get_favpoi_models())
        models.extend(self._get_routerec_models())

        self.cursor.close()
        self.db.close()
        return models


    def _get_search_models(self):
        models = []

        sql = """
            select account_id,keyword,create_time,address,pos_x,pos_y,type,sourceApp,sourceFile,deleted from search
            """

        row = None
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except:
            pass
        while row is not None:
            try:
                if canceller.IsCancellationRequested:
                    return
                search = Base.SearchRecord()
                if row[1]:
                    search.Keyword = row[1]
                if row[2]:
                    search.CreateTime = MapUtil.convert_to_timestamp(row[2])
                if row[3]:
                    pass
                if row[4]:
                    pass
                if row[5]:
                    pass
                if row[6]:
                    pass
                if row[8]:
                    search.SourceFile = MapUtil.format_file_path(row[8])

                search.Deleted = MapUtil.convert_deleted_status(row[9])
                
                models.append(search)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error,"Get Base.SearchRecord() Failed! -{0}".format(e))
            row = self.cursor.fetchone()
        return models
    

    def _get_favpoi_models(self):
        models = []

        sql = """
            select favpoi.account_id,favpoi.poi_name,favpoi.city_id,favpoi.city_name,favpoi.pos_type,favpoi.create_time,favpoi.fav_obj,
                favpoi.sourceApp,favpoi.sourceFile,favpoi.deleted,location.latitude,location.longitude,
                location.elevation,location.address,location.timestamp,location.type,favroute.from_name,favroute.from_posX,
                favroute.from_posY,favroute.from_addr,favroute.to_name,favroute.to_posX,favroute.to_posY,favroute.to_addr,favroute.type
                from favpoi  
                left join location on favpoi.fav_obj = location.location_id
                left join favroute on favpoi.fav_obj = favroute.nav_id
            """

        row = None
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except:
            pass
        while row is not None:
            try:
                if canceller.IsCancellationRequested:
                    return
                favpoi = Favorites()
                loc = Base.Location()
                
  
                account_id = row[0] if row[0] else ''
                poiName = row[1] if row[1] else ''
                cityId = row[2] if row[2] else 0
                cityName = row[3] if row[3] else ''
                # poiType = row[4] if row
                create_time = row[5] if row[5] else 0
                fav_obj = row[6] if row[6] else ''
                sourceApp = row[7] if row[7] else ''
                sourceFile = row[8] if row[8] else ''
                deleted = row[9] if row[9] else 0
                
                latitude = row[10] if row[10] else 0
                longitude = row[11] if row[11] else 0
                elevation = row[12] if row[12] else 0
                address = row[13] if row[13] else ''
                timestamp = row[14] if row[14] else 0
                loc_type = row[15] if row[15] else 0
                
                from_name = row[16] if row[16] else ''
                from_posX = row[17] if row[17] else 0
                from_posY = row[18] if row[18] else 0
                from_addr = row[19] if row[19] else ''
                to_name = row[20] if row[20] else ''
                to_posX = row[21] if row[21] else 0
                to_posY = row[22] if row[22] else 0
                to_addr = row[23] if row[23] else ''
                routerec_type = row[24] if row[24] else 1
                if fav_obj:
                    if latitude != None and longitude != None:   # 收藏的是一个点
                        loc.AddressName = address
                        loc.SourceType = LocationSourceType.App
                        loc.Deleted = MapUtil.convert_deleted_status(deleted)
                        loc.SourceFile = MapUtil.format_file_path(sourceFile)
                        # if loc_type == 6:
                        #     loc.Coordinate = Base.Coordinate(longitude,latitude,MapUtil.convert_coordinat_type(loc_type))
                        if loc_type == 9:
                            loc.Coordinate = Base.Coordinate(MapUtil.pixelXTolng(longitude),MapUtil.pixelYToLat(latitude),MapUtil.convert_coordinat_type(loc_type))
                        else:
                            loc.Coordinate = Base.Coordinate(longitude,latitude,MapUtil.convert_coordinat_type(loc_type))
                        models.append(loc)
                        locationContent = Base.Content.LocationContent(favpoi)
                        locationContent.Value = loc
                        favpoi.Content = locationContent
                    else:  # 收藏的是一个导航路线
                        route = Base.Navigation()
                        start_loc = Base.Location()
                        end_loc = Base.Location()
                        # if sourceApp == "百度地图":
                        #     start_loc.Coordinate = Base.Coordinate(from_posX,from_posY,CoordinateType.Baidumc)
                        # if sourceApp == "百度地图":
                        #     end_loc.Coordinate = Base.Coordinate(to_posX,to_posY,CoordinateType.Baidumc)
                        start_loc.AddressName = from_name
                        end_loc.AddressName = to_name
                        
                        start_loc.SourceType = LocationSourceType.App
                        start_loc.Deleted = MapUtil.convert_deleted_status(deleted)
                        start_loc.SourceFile = MapUtil.format_file_path(sourceFile)
                        
                        end_loc.SourceType = LocationSourceType.App
                        end_loc.Deleted = MapUtil.convert_deleted_status(deleted)
                        end_loc.SourceFile = MapUtil.format_file_path(sourceFile)
                        # if routerec_type == 6:
                        #     start_loc.Coordinate = Base.Coordinate(from_posX,from_posY, MapUtil.convert_coordinat_type(routerec_type))
                        #     end_loc.Coordinate = Base.Coordinate(to_posX,to_posY,MapUtil.convert_coordinat_type(routerec_type))
                        if routerec_type == 9:
                            start_loc.Coordinate = Base.Coordinate(MapUtil.pixelXTolng(from_posX),MapUtil.pixelXTolng(from_posY), MapUtil.convert_coordinat_type(routerec_type))
                            end_loc.Coordinate = Base.Coordinate(MapUtil.pixelXTolng(to_posX),MapUtil.pixelXTolng(to_posY),MapUtil.convert_coordinat_type(routerec_type))
                        else:
                            start_loc.Coordinate = Base.Coordinate(from_posX,from_posY, MapUtil.convert_coordinat_type(routerec_type))
                            end_loc.Coordinate = Base.Coordinate(to_posX,to_posY,MapUtil.convert_coordinat_type(routerec_type))
                        models.append(start_loc)
                        models.append(end_loc)
                        
                        route.StartLocation = start_loc
                        route.EndLocation = end_loc
                        navContent = Base.Content.NavigationContent(favpoi)
                        navContent.Value = route
                        favpoi.Content = navContent

                favpoi.PoiName = poiName
                favpoi.CityName = cityName
                favpoi.Deleted = MapUtil.convert_deleted_status(deleted)
                favpoi.SourceFile = MapUtil.format_file_path(sourceFile)
                favpoi.CreateTime = MapUtil.convert_to_timestamp(create_time)

                # if row[4]:
                #     if row[4] == 0:
                #         pass
                models.append(favpoi)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error,"Get Map.Favorites() Failed! -{0}".format(e))
            row = self.cursor.fetchone()
        return models


    def _get_routerec_models(self):
        models = []

        sql = """
            select account_id, from_name, from_posX, from_posY, from_addr, to_name, to_posX,
                to_posY, to_addr, create_time, nav_id, type, sourceApp, sourceFile, deleted from routerec
            """

        row = None
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except:
            pass
        while row is not None:
            try:
                if canceller.IsCancellationRequested:
                    return
                routerec = Navigation()
                start_loc = Base.Location()
                end_loc = Base.Location()
                start_coord = Base.Coordinate()
                end_coord = Base.Coordinate()
                # if row[0]:
                #     routerec.account_id = row[0]

                from_name = row[1] if row[1] else ""
                from_posX = row[2] if row[2] else 0
                from_posY = row[3] if row[3] else 0
                from_addr = row[4] if row[4] else ""

                to_name = row[5] if row[5] else ""
                to_posX = row[6] if row[6] else 0
                to_posY = row[7] if row[7] else 0
                to_addr = row[8] if row[8] else ""

                create_time = row[9] if row[9] else 0
                routerec_type = row[11]

                start_loc.PoiName = from_name
                start_loc.AddressName = from_addr
                
                if routerec_type == 9:
                    start_loc.Coordinate = Base.Coordinate(MapUtil.pixelXTolng(from_posX),MapUtil.pixelXTolng(from_posY), MapUtil.convert_coordinat_type(routerec_type))
                    end_loc.Coordinate = Base.Coordinate(MapUtil.pixelXTolng(to_posX),MapUtil.pixelXTolng(to_posY),MapUtil.convert_coordinat_type(routerec_type))
                else:
                    start_loc.Coordinate = Base.Coordinate(from_posX,from_posY, MapUtil.convert_coordinat_type(routerec_type))
                    end_loc.Coordinate = Base.Coordinate(to_posX,to_posY,MapUtil.convert_coordinat_type(routerec_type))

                end_loc.PoiName = to_name
                end_loc.AddressName = to_addr
                
                routerec.StartTime = MapUtil.convert_to_timestamp(create_time)

                routerec.SourceFile = MapUtil.format_file_path(row[13])
                start_loc.SourceFile = MapUtil.format_file_path(row[13])
                end_loc.SourceFile = MapUtil.format_file_path(row[13])
                start_coord.SourceFile = MapUtil.format_file_path(row[13])
                end_coord.SourceFile = MapUtil.format_file_path(row[13])

                routerec.Deleted = MapUtil.convert_deleted_status(row[14])
                start_loc.Deleted = MapUtil.convert_deleted_status(row[14])
                end_loc.Deleted = MapUtil.convert_deleted_status(row[14])
                start_coord.Deleted = MapUtil.convert_deleted_status(row[14])
                end_coord.Deleted = MapUtil.convert_deleted_status(row[14])
                
                start_loc.SourceType = LocationSourceType.App
                end_loc.SourceType = LocationSourceType.App
                routerec.StartLocation = start_loc
                routerec.EndLocation = end_loc
                models.append(routerec)
                models.append(start_loc)
                models.append(end_loc)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error,"Get Map.Navigation() Failed! -{0}".format(e))
            row = self.cursor.fetchone()
        return models
