#coding=utf-8
from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference('model_im')
    clr.AddReference('model_map')
except:
    pass
del clr

import pickle
from System.Xml.Linq import *
import System.Data.SQLite as SQLite

import os
import sqlite3
import hashlib
import model_im
import model_map

from model_im import Account, GenerateModel

VERSION_VALUE_DB = 2
VERSION_KEY_DB = 'db'
VERSION_KEY_APP = 'app'


class Ticketing(object):
    
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
            self.db_command.CommandText = model_im.SQL_CREATE_TABLE_ACCOUNT
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = model_im.SQL_CREATE_TABLE_FRIEND
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = model_im.SQL_CREATE_TABLE_CHATROOM
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = model_im.SQL_CREATE_TABLE_CHATROOM_MEMBER
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = model_im.SQL_CREATE_TABLE_MESSAGE
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = model_im.SQL_CREATE_TABLE_DEAL
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = model_im.SQL_CREATE_TABLE_SEARCH
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = model_im.SQL_CREATE_TABLE_FAVORITE
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = model_im.SQL_CREATE_TABLE_FEED
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = model_im.SQL_CREATE_TABLE_FEED_LIKE
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = model_im.SQL_CREATE_TABLE_FEED_COMMENT
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = model_im.SQL_CREATE_TABLE_LOCATION
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = model_im.SQL_CREATE_TABLE_FAVORITE_ITEM
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = model_im.SQL_CREATE_TABLE_LINK
            self.db_command.ExecuteNonQuery()
            # self.db_command.CommandText = model_im.SQL_CREATE_TABLE_DEAL
            # self.db_command.ExecuteNonQuery()
            # self.db_command.CommandText = model_im.SQL_CREATE_TABLE_SEARCH
            # self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = model_map.SQL_CREATE_TABLE_JOURNEY
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = model_map.SQL_CREATE_TABLE_PASSENGER
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = model_im.SQL_CREATE_TABLE_VERSION
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

    def db_insert_table_account(self, column):
        self.db_insert_table(model_im.SQL_INSERT_TABLE_ACCOUNT, column.get_values())

    def db_insert_table_friend(self, column):
        self.db_insert_table(model_im.SQL_INSERT_TABLE_FRIEND, column.get_values())

    def db_insert_table_chatroom(self, column):
        self.db_insert_table(model_im.SQL_INSERT_TABLE_CHATROOM, column.get_values())

    def db_insert_table_chatroom_member(self, column):
        self.db_insert_table(model_im.SQL_INSERT_TABLE_CHATROOM_MEMBER, column.get_values())

    def db_insert_table_message(self, column):
        self.db_insert_table(model_im.SQL_INSERT_TABLE_MESSAGE, column.get_values())

    # def db_insert_table_feed(self, column):
    #     self.db_insert_table(model_im.SQL_INSERT_TABLE_FEED, column.get_values())

    # def db_insert_table_feed_like(self, column):
    #     self.db_insert_table(model_im.SQL_INSERT_TABLE_FEED_LIKE, column.get_values())

    # def db_insert_table_feed_comment(self, column):
    #     self.db_insert_table(model_im.SQL_INSERT_TABLE_FEED_COMMENT, column.get_values())

    def db_insert_table_location(self, column):
        self.db_insert_table(model_im.SQL_INSERT_TABLE_LOCATION, column.get_values())

    # def db_insert_table_deal(self, column):
    #     self.db_insert_table(model_im.SQL_INSERT_TABLE_DEAL, column.get_values())

    # def db_insert_table_search(self, column):
    #     self.db_insert_table(model_im.SQL_INSERT_TABLE_SEARCH, column.get_values())

    def db_insert_table_journey(self, column):
        self.db_insert_table(model_map.SQL_INSERT_TABLE_JOURNEY, column.get_values())

    def db_insert_table_passenger(self, column):
        self.db_insert_table(model_map.SQL_INSERT_TABLE_PASSENGER, column.get_values())

    def db_insert_table_version(self, key, version):
        self.db_insert_table(model_im.SQL_INSERT_TABLE_VERSION, (key, version))

    @staticmethod
    def need_parse(cache_db, app_version):
        if not os.path.exists(cache_db):
            return True
        db = sqlite3.connect(cache_db)
        cursor = db.cursor()
        sql = 'select key,version from version'
        row = None
        db_version_check = False
        app_version_check = False
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_ticketing.py Error: LINE {}".format(e))

        while row is not None:
            if row[0] == VERSION_KEY_DB and row[1] == VERSION_VALUE_DB:
                db_version_check = True
            elif row[0] == VERSION_KEY_APP and row[1] == app_version:
                app_version_check = True
            row = cursor.fetchone()

        if cursor is not None:
            cursor.close()
        if db is not None:
            db.close()
        return not (db_version_check and app_version_check)
