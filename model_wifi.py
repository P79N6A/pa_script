#coding=utf-8

from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions
from PA.InfraLib.Utils import *
import System.Data.SQLite as SQLite

import os
import sqlite3
import json

SQL_CREATE_TABLE_WIFILOG = """
    create table if not exists wifilog(
        IP_Address TEXT,
        Router_IP_Address TEXT,
        Default_Gateway_IP_Address TEXT,
        Router_MAC_Address TEXT,
        Longitude REAL,
        Latitude REAL,
        Network TEXT,
        Time float,
        Type TEXT,
        Source TEXT,
        SourceFile TEXT,
        Deleted INT DEFAULT 0,
        Repeated INT DEFAULT 0
    )
"""

SQL_INSERT_TABLE_WIFILOG = """
    insert into wifilog(IP_Address, Router_IP_Address, Default_Gateway_IP_Address, Router_MAC_Address, Longitude, Latitude, Network, Time, Type, Source, SourceFile, Deleted, Repeated)
    values(?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


class WIFILog(object):
    
    def __init__(self):
        self.db = None
        self.cursor = None

    def db_create(self, db_path):
        if os.path.exists(db_path):
            os.remove(db_path)
        
        self.db = sqlite3.connect(db_path)
        self.cursor = self.db.cursor()
        self.db_create_table()
    
    def db_close(self):
        if self.cursor is not None:
            self.cursor.close()
            self.cursor = None

        if self.db is not None:
            self.db.close()
            self.db = None

    def db_commit(self):
        if self.db is not None:
            self.db.commit()

    def db_create_table(self):
        if self.cursor is not None:
            self.cursor.execute(SQL_CREATE_TABLE_WIFILOG)

    def db_insert_table_wifilog(self, column):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_WIFILOG, column.get_values())    


class Column(object):
    def __init__(self):
        self.source = ""
        self.sourceFile = ""
        self.deleted = 0
        self.repeated = 0

    def get_values(self):
        return self.source, self.sourceFile, self.deleted, self.repeated

class WIFI(Column):
    def __init__(self):
        super(WIFI, self).__init__()
        self.IP_Address = None
        self.Router_IP_Address = None
        self.Default_Gateway_IP_Address = None
        self.Router_MAC_Address = None
        self.Longitude = None
        self.Latitude = None
        self.Network = None
        self.Time = None
        self.Type = None

    def get_values(self):
        return (self.IP_Address, self.Router_IP_Address, self.Default_Gateway_IP_Address, self.Router_MAC_Address, self.Longitude,
        self.Latitude, self.Network, self.Time, self.Type) + super(WIFI, self).get_values()
        


class Generate(object):
    def __init__(self, cache_db):
        self.cache_db = cache_db

    def get_models(self):
        models = []

        self.db = sqlite3.connect(self.cache_db)
        self.cursor = self.db.cursor()

        models.extend(self._get_wifilog_models())

        self.cursor.close()
        self.db.close()
        return models

    def _get_wifilog_models(self):
        models = []

        sql = """
        select IP_Address, Router_IP_Address, Default_Gateway_IP_Address, Router_MAC_Address, Longitude, Latitude, Network, Time, Type, Source, SourceFile, Deleted, Repeated 
            from wifilog
        """
        row = None
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except Exception as e:
            pass

        while row is not None:
            wireless = WirelessConnection()
            coord = Coordinate()
            if row[0]:
                wireless.IPAddress.Value = row[0]
            if row[1]:
                wireless.RouterIPAddress.Value = row[1]
            if row[2]:
                wireless.DefaultGateway.Value = row[2]
            if row[3]:
                wireless.RouterMacAddress.Value = row[3]
            if row[4]:
                coord.Longitude.Value = row[4]
            if row[5]:
                coord.Latitude.Value = row[5]
            if coord:
                wireless.Position.Value = coord
            if row[6]:
                wireless.ConnectionName.Value = row[6]
            if row[7]:
                wireless.Time.Value = self._convert_to_timestamp(row[7])
            if row[8]:
                wireless.WirelessType.Value = self._convert_wireless_type(row[8])
            if row[9]:
                wireless.Source.Value = row[9]
            if row[10]:
                wireless.SourceFile.Value = row[10]
            if row[11]:
                wireless.Deleted = self._convert_deleted_status(row[11])
            
            models.append(wireless)
            row = self.cursor.fetchone()

        return models
  
    def _convert_to_timestamp(self, values):
        try:
            ts = TimeStamp.FromUnixTime(values, False)
            if not ts.IsValidForSmartphone():
                ts = None
            return ts
        except Exception as e:
            return None


    def _convert_wireless_type(self, type):
        if type == "Cell":
            return WirelessType.MobileNetwork
        elif type == "GPS":
            return WirelessType.GPS
        elif type == "WiFi":
            return WirelessType.Wifi


    def _convert_deleted_status(self, values):
        if values is None:
            return DeletedState.Unknown
        else:
            return DeletedState.Intact if values == 0 else DeletedState.Deleted 
