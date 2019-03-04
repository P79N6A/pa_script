#coding:utf-8

__author__ = "Xu Tao"

from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
del clr

import os
import time
import sqlite3
import traceback
import System.Data.SQLite as SQLite
from PA.InfraLib.ModelsV2 import *
from PA.InfraLib.ModelsV2.Base import *
from PA.InfraLib.ModelsV2.CommonEnum import CoordinateType

VERSION_VALUE_DB = 2

VERSION_KEY_DB = 'db'
VERSION_KEY_APP = 'app'


LOCATION_TYPE_GPS = 1  # GPS坐标
LOCATION_TYPE_GPS_MC = 2  # GPS米制坐标
LOCATION_TYPE_GOOGLE = 3  # GCJ02坐标
LOCATION_TYPE_GOOGLE_MC = 4  # GCJ02米制坐标
LOCATION_TYPE_BAIDU = 5  # 百度经纬度坐标
LOCATION_TYPE_BAIDU_MC = 6  # 百度米制坐标
LOCATION_TYPE_MAPBAR = 7  # mapbar地图坐标
LOCATION_TYPE_MAP51 = 8  # 51地图坐标



SQL_CREATE_TABLE_TALLY = '''
    create table if not exists tally(
        bookId TEXT,
        name TEXT,
        tallyType INT,
        memberCount INT,
        moneyFlag TEXT,
        createTime INT,
        updatedTime INT,
        source TEXT,
        deleted INT DEFAULT 0
    )
'''

SQL_INSERT_TABLE_TALLY = '''
    insert into tally(bookId,name,tallyType,memberCount,moneyFlag,createTime,updatedTime,source,deleted)
        values(?,?,?,?,?,?,?,?,?)
'''


SQL_CREATE_TABLE_MEMBER = '''
    create table if not exists member(
        bookId TEXT,
        memberId TEXT,
        name TEXT,
        memberType INT,
        avtar TEXT,
        phone TEXT,
        createTime INT,
        updatedTime INT,
        source TEXT,
        deleted INT DEFAULT 0
    )
'''

SQL_INSERT_TABLE_MEMBER = '''
    insert into member(bookId,memberId,name,memberType,avtar,phone,createTime,updatedTime,source,deleted)
        values(?,?,?,?,?,?,?,?,?,?)
'''


SQL_CREATE_TABLE_RECORD = '''
    create table if not exists record(
        bookId TEXT,
        bookType INT,
        name TEXT,
        memberId TEXT,
        mediaPath TEXT,
        langitude REAL,
        latitude REAL,
        locType INT,
        amount REAL,
        tip TEXT,
        remark TEXT,
        createTime INT,
        updatedTime INT,
        source TEXT,
        deleted INT DEFAULT 0
    )
'''

SQL_INSERT_TABLE_RECOED = '''
    insert into record(bookId,bookType,name,memberId,mediaPath,langitude,latitude,locType,amount,tip,remark,
        createTime,updatedTime,source,deleted) 
    values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''


SQL_CREATE_TABLE_BILL = '''
    create table if not exists bill(
        bookId TEXT,
        bookType INT,
        time INT,
        income REAL,
        outgo REAL,
        balance REAL,
        source TEXT,
        deleted INT DEFAULT 0
    )
'''

SQL_INSERT_TABLE_BILL = '''
    insert into bill(bookId,bookType,time,income,outgo,balance,source,deleted) 
        values(?,?,?,?,?,?,?,?)
'''


SQL_CREATE_TABLE_VERSION = '''
    create table if not exists version(
        key TEXT primary key,
        version INT)'''

SQL_INSERT_TABLE_VERSION = '''
    insert into version(key, version) values(?, ?)'''


class WACAI(object):
    def __init__(self):
        self.db = None
        self.db_cmd = None
        self.db_trans = None

    def db_create(self, db_path):
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "model_wacai.py Error: LINE {}".format(traceback.format_exc()))
                return False

        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(db_path))
        if self.db is not None:
            self.db.Open()
            self.db_cmd = SQLite.SQLiteCommand(self.db)
            self.db_trans = self.db.BeginTransaction()

            self.db_create_table()
            self.db_commit()

    def db_close(self):
        self.db_trans = None
        if self.db_cmd is not None:
            self.db_cmd.Dispose()
            self.db_cmd = None
        if self.db is not None:
            self.db.Close()
            self.db = None

    def db_commit(self):
        if self.db_trans is not None:
            self.db_trans.Commit()
        self.db_trans = self.db.BeginTransaction()

    def db_create_table(self):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = SQL_CREATE_TABLE_TALLY
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_MEMBER
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_RECORD
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_BILL
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_VERSION
            self.db_cmd.ExecuteNonQuery()

    def db_insert_table(self, sql, values):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = sql
            self.db_cmd.Parameters.Clear()
            for value in values:
                param = self.db_cmd.CreateParameter()
                param.Value = value
                self.db_cmd.Parameters.Add(param)
            self.db_cmd.ExecuteNonQuery()

    def db_insert_table_tally(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_TALLY, column.get_values())

    def db_insert_table_member(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_MEMBER, column.get_values())

    def db_insert_table_record(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_RECOED, column.get_values())

    def db_insert_table_bill(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_BILL, column.get_values())

    def db_insert_table_version(self, key, version):
        self.db_insert_table(SQL_INSERT_TABLE_VERSION, (key, version))

    '''
    版本检测分为两部分
    如果中间数据库结构改变，会修改db_version
    如果app增加了新的内容，需要修改app_version
    只有db_version和app_version都没有变化时，才不需要重新解析
    '''
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
            TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: LINE {}".format(traceback.format_exc()))

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


class Column(object):
    
    def __init__(self):
        self.source = ''
        self.deleted = 0

    def get_values(self):
        return self.source, self.deleted


# 账本
class Tally(Column):

    def __init__(self):
        super(Tally, self).__init__()
        self.bookId = None          # 账本ID[str]
        self.name = None            # 账本名称[str]
        self.tallyType = None       # 账本类型[int]
        self.memberCount = None     # 账本参与人数[int]
        self.moneyFlag = None       # 账本货币[str]
        self.createTime = None      # 账本创建时间[int]
        self.updatedTime = None     # 账本更新时间[int]

    def get_values(self):
        return (self.bookId,self.name,self.tallyType,self.memberCount,self.moneyFlag,self.createTime,
        self.updatedTime) + super(Tally, self).get_values()
    
# 参与人
class Member(Column):

    def __init__(self):
        super(Member, self).__init__()
        self.bookId = None          # 账本ID[str]
        self.memberId = None        # 成员ID[str]
        self.name = None            # 成员昵称[str]            
        self.memberType = None      # 成员类型[int]
        self.avatar = None          # 成员头像[str]
        self.phone = None           # 成员手机[str]
        self.createTime = None      # 创建时间[int]
        self.updatedTime = None     # 更新时间[int]
    
    def get_values(self):
        return (self.bookId,self.memberId,self.name,self.memberType,self.avatar,self.phone,
        self.createTime,self.updatedTime) + super(Member, self).get_values()

# 账本单条记录
class BillRecord(Column):
   
    def __init__(self):
        super(BillRecord, self).__init__()
        self.bookId = None          # 账本ID[str]
        self.bookType = None        # 记录类型[int]  [1-zhichu，2-收入]
        self.name = None            # 记录名称[str]
        self.memberId = None        # 成员id[str]
        self.mediaPath = None       # 媒体资源文件[str]
        self.langitude = None       # 经度[float]
        self.latitude = None        # 纬度[float]
        self.locType = LOCATION_TYPE_GPS  # 位置类型[int]
        self.amount = None          # 记录金额[int]
        self.tip = None             # 提示[str]
        self.remark = None          # 备注[str]
        self.createTime = None      # 创建时间[int]
        self.updatedTime = None     # 更新时间[int]

    def get_values(self):
        return (self.bookId,self.bookType,self.name,self.memberId,self.mediaPath,self.langitude,
        self.latitude,self.locType,self.amount,self.tip,self.remark,self.createTime,
        self.updatedTime) + super(BillRecord, self).get_values()

# 月度总结
class MonthBill(Column):

    def __init__(self):
        super(MonthBill, self).__init__()
        self.bookId = None          # 账本id[str]
        self.bookType = None        # 账本类型[int] 
        self.createTime = None      # 时间[int]
        self.income = None          # 收入[float]
        self.outgo = None           # 支出[float]
        self.balance = None         # 结余[float]

    def get_values(self):
        return (self.bookId,self.bookType,self.createTime,self.income,self.outgo,self.balance) + super(MonthBill, self).get_values()



class ExportModel(object):

    def __init__(self, db_path):
        self.db_path = db_path
        self.accounts_book = {}
        self.members = {}

    def get_model(self):
        models = []

        self.db = sqlite3.connect(self.db_path)
        self.cursor = self.db.cursor()

        self.get_tally_models()
        self.get_member_models()
        models.extend(self.get_record_models())
        models.extend(self.get_monthBill_models())
        models.extend(self.accounts_book.values())
        self.cursor.close()
        self.db.close()
        return models

    def get_tally_models(self):

        sql = "select * from tally"
        row = None
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error,"{0}".format(e))
       
        while row is not None:
            try:
                if canceller.IsCancellationRequested:
                    return
                bookid = row[0] if row[0] else None
                book_name = row[1] if row[1] else ""
                book_type = row[2] if row[2] else None
                memberCount = row[3] if row[3] else None
                moneyFlag = row[4] if row[4] else ""
                createTime = row[5] if row[5] else 0
                updateTime = row[6] if row[6] else 0
                source = row[7] if row[7] else None
                deleted = row[8] if row[8] else 0

                if bookid:
                    account_book = Financing.WaCai.AccountBook()
                    account_book.Name = book_name
                    account_book.Type = self._convert_accountbooks_type(book_type)
                    account_book.MoneyFlag = moneyFlag
                    account_book.CreateTime = self._convert_to_timestamp(createTime) 
                    account_book.UpdateTime = self._convert_to_timestamp(updateTime)
                    account_book.SourceFile = source
                    account_book.Deleted = self._convert_deleted_status(deleted)
                    self.accounts_book[bookid] = account_book
            except:
                TraceService.Trace(TraceLevel.Error, "model_wacai get_tally_models():{0}".format(traceback.format_exc()))
            row = self.cursor.fetchone()


    def get_member_models(self):
        sql = "select * from member"
        row = None
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error,"{0}".format(e))
       
        while row is not None:
            try:
                if canceller.IsCancellationRequested:
                    return
                bookid = row[0] if row[0] else None
                memberID = row[1] if row[1] else None
                name = row[2] if row[2] else ""
                memberType = row[3] if row[3] else None
                avatar = row[4] if row[4] else ""
                phone = row[5] if row[5] else ""
                createTime = row[6] if row[6] else 0
                updateTime = row[7] if row[7] else 0 
                source = row[8] if row[8] else ""
                deleted = row[9] if row[9] else 0

                if bookid and memberID:
                    member = Financing.WaCai.Member()
                    member.NickName = name
                    member.PhoneNumber = phone
                    member.HeadPortraitPath = avatar
                    member.CreateTime = self._convert_to_timestamp(createTime)
                    member.UpdateTime = self._convert_to_timestamp(updateTime)
                    member.SourceFile = source
                    member.Deleted = self._convert_deleted_status(deleted)

                    if bookid in self.accounts_book.keys():
                        tally_model = self.accounts_book[bookid]
                        tally_model.Members.Add(member)
                    if bookid and memberID:
                        self.members[bookid+memberID] = member
            except:
                TraceService.Trace(TraceLevel.Error, "model_wacai get_member_models():{0}".format(traceback.format_exc()))
            row = self.cursor.fetchone()


    def get_record_models(self):
        models = []

        sql = "select * from record"
        row = None
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error,"{0}".format(e))
       
        while row is not None:
            try:
                if canceller.IsCancellationRequested:
                    return
                bookId = row[0] if row[0] else None
                bookType = row[1]
                name = row[2] if row[2] else ""
                memberId = row[3] if row[3] else None
                mediaPath = row[4] if row[4] else None
                langitude = row[5] if row[5] else None
                latitude = row[6] if row[6] else None
                locType = row[7] if row[7] else None
                amount = row[8]
                tip = row[9] if row[9] else ""
                remark = row[10] if row[10] else ""
                createTime = row[11] if row[11] else 0
                updatedTime = row[12] if row[12] else 0
                source = row[13] if row[13] else ""
                deleted = row[14] if row[14] else 0

                if bookId:
                    bill_record = Financing.WaCai.BillRecord()
                    if amount is not None:
                        bill_record.Amount = amount
                    bill_record.CreateTime = self._convert_to_timestamp(createTime)
                    bill_record.UpdateTime = self._convert_to_timestamp(updatedTime)
                    bill_record.Remark = remark
                    bill_record.Tip = tip
                    bill_record.Name = name
                    if bookType == 1:
                        bill_record.Type = Financing.BillRecordType.Income
                    elif bookType == 2:
                        bill_record.Type = Financing.BillRecordType.Outgo
                    if langitude and langitude:
                        loc = Base.Location()
                        loc.Coordinate = Base.Coordinate(longitude, latitude, self._convert_coordinat_type(loc_type))
                        bill_record.Location = loc
                    if bookId in self.accounts_book.keys():
                        tally_model = self.accounts_book[bookId]
                        bill_record.AccountBook = tally_model
                    if memberId:
                        id_list = memberId.split(",")
                        for mid in id_list:
                            keys = bookId+memberId
                            if keys in self.members:
                                user = self.members[keys]
                                bill_record.Member.Add(user)
                    if mediaPath:
                        media_list = mediaPath.split(",")
                        for media in media_list:
                            model = MediaFile.ImageFile()
                            model.Path = media
                            bill_record.Medium.Add(model)
                    bill_record.SourceFile = source
                    bill_record.Deleted = self._convert_deleted_status(deleted)
                    if bill_record.AccountBook is not None:
                        models.append(bill_record)
            except:
                TraceService.Trace(TraceLevel.Error, "model_wacai get_record_models():{0}".format(traceback.format_exc()))
            row = self.cursor.fetchone()
        return models


    def get_monthBill_models(self):
        models = []

        sql = "select * from bill"
        row = None
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "{0}".format(e))
       
        while row is not None:
            try:
                if canceller.IsCancellationRequested:
                    return
                bookId = row[0] if row[0] else None
                bookType = row[1] if row[1] else None
                bill_time = row[2]
                income = row[3]
                outgo = row[4]
                balance = row[5]
                source = row[6]
                deleted = row[7] if row[7] else None

                if bookId and bill_time:
                    year,month = self._format_time(bill_time)
                    if not year and not month:
                        row = self.cursor.fetchone()
                        continue
                    month_bill = Financing.WaCai.MonthBill(year, month)
                    if bookId in self.accounts_book.keys():
                        model = self.accounts_book[bookId]
                        month_bill.AccountBook = model
                    month_bill.Balance = balance
                    month_bill.Income = income
                    month_bill.Outgo = outgo
                    month_bill.SourceFile = source
                    month_bill.Deleted = self._convert_deleted_status(deleted)
                    month_bill.CreateTime = self._convert_to_timestamp(0)
                    if month_bill.AccountBook is not None:
                        models.append(month_bill)
            except:
                TraceService.Trace(TraceLevel.Error, "model_wacai get_monthBill_models():{0}".format(traceback.format_exc()))
            row = self.cursor.fetchone()
        return models


    @staticmethod
    def _convert_deleted_status(deleted):
        if deleted is None:
            return DeletedState.Unknown
        else:
            return DeletedState.Intact if deleted == 0 else DeletedState.Deleted

    @staticmethod
    def _convert_to_timestamp(timestamp):
        if len(str(timestamp)) == 13:
            timestamp = int(str(timestamp)[0:10])
        elif len(str(timestamp)) != 13 and len(str(timestamp)) != 10:
            timestamp = 0
        elif len(str(timestamp)) == 10:
            timestamp = timestamp
        ts = TimeStamp.FromUnixTime(timestamp, False)
        if not ts.IsValidForSmartphone():
            ts = None
        return ts

    @staticmethod
    def _convert_accountbooks_type(v):
        if v is None:
            return Financing.WaCai.AccountBookType.None
        elif v == 1:
            return Financing.WaCai.AccountBookType.Family
        elif v == 2:
            return Financing.WaCai.AccountBookType.SplitBill

    @ staticmethod
    def _convert_coordinat_type(type_value):
        if type_value == 1:             # GPS坐标
            return CoordinateType.GPS 
        elif type_value == 2:           # GPS米制坐标
            return CoordinateType.GPSmc
        elif type_value == 3:           # GCJ02坐标
            return CoordinateType.Google
        elif type_value == 4:           # GCJ02米制坐标
            return CoordinateType.Googlemc
        elif type_value == 5:           # 百度经纬度坐标
            return CoordinateType.Baidu
        elif type_value == 6:           # 百度米制坐标
            return CoordinateType.Baidumc
        elif type_value == 7:           # mapbar地图坐标
            return CoordinateType.MapBar
        elif type_value == 8:           # 51地图坐标
            return CoordinateType.Map51
        elif type_value == 9:           # 51地图坐标
            return CoordinateType.GPS

    
    @staticmethod
    def _format_time(v):
        if len(str(v)) == 6:
            year = int(str(v)[:4])
            month = int(str(v)[4:])
            return year, month
        else:
            return None, None
    






