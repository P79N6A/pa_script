#coding:utf-8

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
import pickle
import base64

SQL_CREATE_TABLE_APPLISTS= """
    create table if not exists Applists(
        bind_id TEXT,
        name TEXT,
        version TEXT,
        permission BLOB,
        installedPath TEXT,
        imgUrl TEXT,
        purchaseDate INT,
        deletedDate INT,
        description TEXT,
        source TEXT,
        sourceFile TEXT,
        deleted INT DEFAULT 0,
        repeated INT DEFAULT 0
    )
"""

SQL_INSERT_TABLE_APPLISTS = """
    insert into Applists(bind_id, name, version, permission, installedPath, imgUrl,purchaseDate,deletedDate,description, source, sourceFile, deleted, repeated)
    values(?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


# 删除状态
DELETE_STATUS_UNDELETED = "0"
DELETE_STATUS_DELETED = "1"

class Apps(object):
    
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
            self.cursor.execute(SQL_CREATE_TABLE_APPLISTS)

    def db_insert_table_applists(self, column):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_APPLISTS, column.get_values())

    def db_insert_table_applists(self, column):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_APPLISTS, column.get_values())     


class Column(object):
    def __init__(self):
        self.source = ""
        self.sourceFile = ""
        self.deleted = 0
        self.repeated = 0

    def get_values(self):
        return self.source, self.sourceFile, self.deleted, self.repeated

class Info(Column):
    def __init__(self):
        super(Info, self).__init__()
        self.bind_id = None
        self.name = None
        self.version = None
        self.permission = None
        self.installedPath = None
        self.imgUrl = None
        self.purchaseDate = None
        self.deletedDate = None
        self.description = None

    def get_values(self):
        return (self.bind_id, self.name, self.version, self.permission, self.installedPath,self.imgUrl,
        self.purchaseDate,self.deletedDate, self.description) + super(Info, self).get_values()
        


class Generate(object):
    def __init__(self, cache_db):
        self.cache_db = cache_db

    def get_models(self):
        models = []

        self.db = sqlite3.connect(self.cache_db)
        self.cursor = self.db.cursor()

        models.extend(self._get_appsinstalled_models())

        self.cursor.close()
        self.db.close()
        return models

    def _get_appsinstalled_models(self):
        models = []

        sql = """
        select bind_id, name, version, permission, installedPath, imgUrl,purchaseDate,deletedDate,description, source, sourceFile, deleted, repeated
            from Applists
        """
        row = None
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except Exception as e:
            pass

        while row is not None:
            if canceller.IsCancellationRequested:
                return
            if row[12] == 0:
                application = InstalledApplication()
            elif row[12] == 1:
                application = ApplicationLog()
            else:
                return []
            application.IsAndroid.Value = True
            if row[0]:
                application.AppGUID.Value = row[0]
            if row[1]:
                application.Name.Value = row[1]
            if row[2]:
                application.Version.Value = row[2]
            if row[3]:
                permissions = pickle.loads(row[3])
                if permissions and type(permissions) == list:
                    for i in permissions:
                        application.Permissions.Add(i)
                else:
                        tmp_a = permissions.encode("utf-8")
                        tmp_b = tmp_a.split("\n")
                        tmp_c =  tmp_b[1:-2]
                        for per in tmp_c:
                            try:
                                application.Permissions.Add(per.strip().replace(",","").replace('\"',""))
                            except Exception as e:
                                pass
            if row[5]:
                try:
                    # Convert.FromBase64String(base64)
                    # imgdata = base64.b64decode(row[5])
                    application.IconData = Convert.FromBase64String(row[5])
                except Exception as e:
                    pass
            if row[6]:
                application.PurchaseDate.Value = TimeStamp.FromUnixTime(row[6], False)
            if row[7]:
                application.DeletedDate.Value = TimeStamp.FromUnixTime(row[7], False)
            if row[8]:
                application.Description.Value = row[8]
            if row[10]:
                application.SourceFile.Value = row[10]
            if row[11]:
                application.Deleted = self._convert_delete_status(row[11])

            row = self.cursor.fetchone()
            models.append(application)

        return models
        
    
    @staticmethod
    def _convert_delete_status(deleted):
        if deleted is None:
            return DeletedState.Unknown
        else:
            return DeletedState.Intact if deleted == 0 else DeletedState.Deleted

    