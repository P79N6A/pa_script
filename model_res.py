#coding:utf-8

import clr
from PA_runtime import *
try:
    clr.AddReference('System.Data.SQLite')
except:
    pass
import os
import sqlite3
import System.Data.SQLite as SQLite
from PA.InfraLib.ModelsV2.Base import *


SQL_CREATE_TABLE_IMAGE = '''
    create table if not exists image(
        FileName TEXT,
        Path TEXT,
        Size REAL,
        AddTime INT,
        ModifyTime INT,
        FileExtention TEXT,
        MimeType TEXT,
        Height REAL,
        Width REAL,
        Longitude REAL,
        Latitude REAL,
        Loctype INT,
        ISO TEXT,
        Make TEXT,
        Model TEXT,
        Artist TEXT,
        Software TEXT,
        Aperture TEXT,
        TakenDate INT,
        ColorSpace TEXT,
        Resolution TEXT,
        FocalLength TEXT,
        XResolution TEXT,
        YResolution TEXT,
        ExifVersion TEXT,
        ExposureTime TEXT,
        ExposureProgram TEXT,
        Type INT,
        deleted INT DEFAULT 0
    )
'''
INSERT_INTO_TABLE_IMAGE = '''
    insert into image(FileName,Path,Size,AddTime,ModifyTime,FileExtention,
        MimeType,Height,Width,Longitude,Latitude,Loctype,ISO,Make,Model,Artist,
        Software,Aperture,TakenDate,ColorSpace,Resolution,FocalLength,XResolution,
        YResolution,ExifVersion,ExposureTime,ExposureProgram,Type,deleted)
        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''
SQL_CREATE_TABLE_AUDIO = '''
    create table if not exists audio(
        FileName TEXT,
        Path TEXT,
        Size REAL,
        AddTime INT,
        ModifyTime INT,
        FileExtention TEXT,
        MimeType TEXT,
        Album TEXT,
        Artist TEXT,
        Duration INT,
        deleted INT DEFAULT 0
    )
    '''

INSERT_INTO_TABLE_AUDIO = '''
    insert into audio(FileName,Path,Size,AddTime,ModifyTime,FileExtention,MimeType,
        Album,Artist,Duration,deleted) values(?,?,?,?,?,?,?,?,?,?,?)
'''
SQL_CREATE_TABLE_VIDEO = '''
    create table if not exists video(
        FileName TEXT,
        Path TEXT,
        Size REAL,
        AddTime INT,
        ModifyTime INT,
        FileExtention TEXT,
        MimeType TEXT,
        Duration INT,
        TakenDate INT,
        deleted INT DEFAULT 0
    )
    '''

INSERT_INTO_TABLE_VIDEO = '''
    insert into video(FileName,Path,Size,AddTime,ModifyTime,FileExtention,MimeType,
        Duration,TakenDate,deleted) values(?,?,?,?,?,?,?,?,?,?)
'''

SQL_CREATE_TABLE_OTHER = '''
    create table if not exists other(
        FileName TEXT,
        Path TEXT,
        Size REAL,
        AddTime INT,
        ModifyTime INT,
        FileExtention TEXT,
        MimeType TEXT,
        Type INT,
        deleted INT DEFAULT 0
    )
    '''

INSERT_INTO_TABLE_OTHER = '''
    insert into other(FileName,Path,Size,AddTime,ModifyTime,FileExtention,MimeType,
        Type,deleted) values(?,?,?,?,?,?,?,?,?)
'''

SQL_CREATE_TABLE_VERSION = '''
    create table if not exists version(
        key TEXT primary key,
        version INT)'''

SQL_INSERT_TABLE_VERSION = '''
    insert into version(key, version) values(?, ?)'''

VERSION_KEY_DB = 'db'
VERSION_KEY_APP = 'app'
VERSION_VALUE_DB = 1

IMAGE_FILE = 1
IMAGE_THUMBNAIL_FLIE = 2
VIDEO_THUMBNAIL_FLIE = 3

UN_DELETED_STATUS = 0
DELETED_STATUS = 1

class Resources(object):
    
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
            except:
                self.db_trans.RollBack()
        self.db_trans = self.db.BeginTransaction()

    def db_create_table(self):
        if self.db_command is not None:
            self.db_command.CommandText = SQL_CREATE_TABLE_IMAGE
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = SQL_CREATE_TABLE_AUDIO
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = SQL_CREATE_TABLE_VIDEO
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = SQL_CREATE_TABLE_OTHER
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = SQL_CREATE_TABLE_VERSION
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

    def db_insert_table_image(self, column):
        self.db_insert_table(INSERT_INTO_TABLE_IMAGE, column.get_values())

    def db_insert_table_audio(self, column):
        self.db_insert_table(INSERT_INTO_TABLE_AUDIO, column.get_values())

    def db_insert_table_video(self, column):
        self.db_insert_table(INSERT_INTO_TABLE_VIDEO, column.get_values())

    def db_insert_table_other(self, column):
        self.db_insert_table(INSERT_INTO_TABLE_OTHER, column.get_values())
    
    def db_insert_table_version(self, key, version):
        self.db_insert_table(SQL_INSERT_TABLE_VERSION, (key, version))
    
    '''
    如果中间数据库结构改变，会修改db_version
    db_version没有变化时，不需要重新解析
    '''
    @staticmethod
    def need_parse(cache_db):
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
            row = cursor.fetchone()

        if cursor is not None:
            cursor.close()
        if db is not None:
            db.close()
        return not db_version_check


class Column(object):

    def __init__(self):
        self.fileName = ""
        self.path = ""
        self.size = 0
        self.addTime = 0
        self.modifyTime = 0
        self.fileExtention = None
        self.mimeType = ""

    def get_values(self):
        return self.fileName,self.path,self.size,self.addTime,self.modifyTime,self.fileExtention,self.mimeType


class MediaImage(Column):

    def __init__(self):
        super(MediaImage, self).__init__()
        self.height = None
        self.width = None
        self.longitude = None
        self.latitude = None
        self.loctype = None
        self.iso = None
        self.make = None
        self.model = None
        self.artist = None
        self.software = None
        self.aperture = None
        self.takenDate = None
        self.colorSpace = None
        self.resolution = None
        self.focalLength = None
        self.xresolution = None
        self.yresolution = None
        self.exifVersion = None
        self.exposureTime = None
        self.exposureProgram = None
        self.type = IMAGE_FILE  # 1 图片 2 图片缩略图 3 视频缩略图
        self.deleted = UN_DELETED_STATUS
    
    def get_values(self):
        return super(MediaImage, self).get_values() + (self.height ,self.width ,self.longitude ,self.latitude ,self.loctype,self.iso ,self.make ,self.model ,
        self.artist ,self.software ,self.aperture ,self.takenDate,self.colorSpace ,self.resolution ,self.focalLength ,
        self.xresolution ,self.yresolution ,self.exifVersion ,self.exposureTime ,self.exposureProgram,self.type,self.deleted)


class MediaAudio(Column):

    def __init__(self):
        super(MediaAudio, self).__init__()
        self.album = None
        self.artist = None
        self.duration = None
        self.deleted = UN_DELETED_STATUS

    def get_values(self):
        return super(MediaAudio, self).get_values() + (self.album,self.artist,self.duration,self.deleted)


class MediaVideo(Column):

    def __init__(self):
        super(MediaVideo, self).__init__()
        self.duration = None
        self.takenDate = None
        self.deleted = None

    def get_values(self):
        return super(MediaVideo, self).get_values() + (self.duration,self.takenDate,self.deleted)


class MediaOther(Column):

    def __init__(self):
        super(MediaOther, self).__init__()
        self.type = None
        self.deleted = None

    def get_values(self):
        return super(MediaOther, self).get_values() + (self.type, self.deleted)


class ExportModel(object):

    def __init__(self, db_path, build, categories):
        self.db_path = db_path
        self.build = build
        self.categories = categories
        self.model_max_count = 1000

    def get_model(self):
        self.db = sqlite3.connect(self.db_path)
        self.cursor = self.db.cursor()

        self._get_image_models()
        self._get_audio_models()
        self._get_video_models()
        self._get_other_models()

        self.cursor.close()
        self.db.close()

    def _get_image_models(self):
        models = []

        sql = """
            select * from image
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
                fileName = self._get_db_string_value(row, 0)
                path = self._get_db_string_value(row, 1)
                size = self._get_db_int_value(row, 2)
                addTime = self._get_db_int_value(row, 3)
                modifyTime = self._get_db_int_value(row, 4)
                fileExtention = self._get_db_string_value(row, 5)
                mimeType = self._get_db_string_value(row, 6)
                height = self._get_db_int_value(row, 7)
                width = self._get_db_int_value(row, 8)
                longitude = self._get_db_int_value(row, 9)
                latitude = self._get_db_int_value(row, 10)
                loctype = self._get_db_int_value(row, 11)
                iso  = self._get_db_string_value(row, 12)
                make = self._get_db_string_value(row, 13)
                model = self._get_db_string_value(row, 14)
                artist = self._get_db_string_value(row, 15)
                software = self._get_db_string_value(row, 16)
                aperture = self._get_db_string_value(row, 17)
                takenDate = self._get_db_int_value(row, 18)
                colorSpace = self._get_db_string_value(row, 19)
                resolution = self._get_db_string_value(row, 20)
                focalLength = self._get_db_string_value(row, 21)
                xresolution = self._get_db_string_value(row, 22)
                yresolution = self._get_db_string_value(row, 23)
                exifVersion = self._get_db_string_value(row, 24)
                exposureTime = self._get_db_string_value(row, 25)
                exposureProgram= self._get_db_string_value(row, 26)
                media_type = self._get_db_int_value(row, 27)
                deleted = self._get_db_int_value(row, 28)

                image = None
                if media_type == 1:
                    image = MediaFile.ImageFile()
                elif media_type == 2:
                    image = MediaFile.ThumbnailFile()
                elif media_type == 3:
                    image = MediaFile.VideoThumbnailFile()
                else:
                    image = MediaFile.ImageFile()

                image.FileName = fileName
                image.Path = path
                image.Size = size
                image.AddTime = self._unixtime_to_timestamp(addTime)  # 还没有对时间处理
                image.ModifyTime = self._unixtime_to_timestamp(modifyTime) # 还没有对时间处理
                image.FileExtention = fileExtention
                # image.MimeType = mimeType
                image.Height = height
                image.Width = width
                if longitude > 0 and latitude > 0:
                    loc = Base.Location()
                    loc.Coordinate = Base.Coordinate(longitude,latitude, CoordinateType.GPS)
                    image.Location = loc
                image.ISO = iso
                image.Make = make
                image.Model = model
                image.Artist = artist
                image.Software = software
                image.Aperture = aperture
                image.TakenDate = self._unixtime_to_timestamp(takenDate)
                image.ColorSpace = self._unixtime_to_timestamp(colorSpace)
                image.Resolution = resolution
                image.FocalLength = focalLength
                image.XResolution = xresolution
                image.YResolution = yresolution
                image.ExifVersion = exifVersion
                image.ExposureTime = exposureTime
                image.ExposureProgram = exposureProgram
                image.Deleted = self._convert_model_deleted_status(deleted)
                models.append(image)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error,"Get _get_image_models Failed! -{0}".format(e))

            if len(models) > self.model_max_count:
                self._push_models(models)
                models = []
            row = self.cursor.fetchone()
        self._push_models(models)

    def _get_audio_models(self):
        models = []

        sql = """
            select * from audio
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
                fileName = self._get_db_string_value(row, 0)
                path = self._get_db_string_value(row, 1)
                size = self._get_db_int_value(row, 2)
                addTime = self._get_db_int_value(row, 3)
                modifyTime = self._get_db_int_value(row, 4)
                fileExtention = self._get_db_string_value(row, 5)
                # mimeType = self._get_db_string_value(row, 6)
                album = self._get_db_string_value(row, 7)
                artist = self._get_db_string_value(row, 8)
                duration = self._get_db_int_value(row, 9)
                deleted = self._get_db_int_value(row, 10)

                audio = MediaFile.AudioFile()
                audio.FileName = fileName
                audio.Path = path
                audio.Size = size
                audio.AddTime = self._unixtime_to_timestamp(addTime)  # 还没有对时间处理
                audio.ModifyTime = self._unixtime_to_timestamp(modifyTime) # 还没有对时间处理
                audio.FileExtention = fileExtention
                # audio.MimeType = mimeType
                audio.Album = album
                audio.Artist = artist
                # audio.duration = duration  # timespan类型
                audio.Deleted = self._convert_model_deleted_status(deleted)
                models.append(audio)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error,"Get _get_audio_models Failed! -{0}".format(e))

            if len(models) > self.model_max_count:
                self._push_models(models)
                models = []
            row = self.cursor.fetchone()
        self._push_models(models)

    
    def _get_video_models(self):
        models = []

        sql = """
            select * from video
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
                fileName = self._get_db_string_value(row, 0)
                path = self._get_db_string_value(row, 1)
                size = self._get_db_int_value(row, 2)
                addTime = self._get_db_int_value(row, 3)
                modifyTime = self._get_db_int_value(row, 4)
                fileExtention = self._get_db_string_value(row, 5)
                # mimeType = self._get_db_string_value(row, 6)
                duration = self._get_db_int_value(row, 7)
                takenData = self._get_db_int_value(row, 8)
                deleted = self._get_db_int_value(row, 9)

                video = MediaFile.VideoFile()
                video.FileName = fileName
                video.Path = path
                video.Size = size
                video.AddTime = self._unixtime_to_timestamp(addTime)  # 还没有对时间处理
                video.ModifyTime = self._unixtime_to_timestamp(modifyTime) # 还没有对时间处理
                video.FileExtention = fileExtention
                # video.MimeType = mimeType
                # video.Duration = duration
                video.TakenDate = self._unixtime_to_timestamp(takenData)
                video.Deleted = self._convert_model_deleted_status(deleted)
                models.append(video)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error,"Get _get_video_models Failed! -{0}".format(e))

            if len(models) > self.model_max_count:
                self._push_models(models)
                models = []
            row = self.cursor.fetchone()
        self._push_models(models)

    
    def _get_other_models(self):
        models = []

        sql = """
            select * from other
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
                fileName = self._get_db_string_value(row, 0)
                path = self._get_db_string_value(row, 1)
                size = self._get_db_int_value(row, 2)
                addTime = self._get_db_int_value(row, 3)
                modifyTime = self._get_db_int_value(row, 4)
                fileExtention = self._get_db_string_value(row, 5)
                mimeType = self._get_db_string_value(row, 6)
                md_type = self._get_db_int_value(row, 7)
                deleted = self._get_db_int_value(row, 8)

                other = OtherFile.OtherFile()              
                other.FileName = fileName
                other.Path = path
                other.Size = size
                other.AddTime = self._unixtime_to_timestamp(addTime)  # 还没有对时间处理
                other.ModifyTime = self._unixtime_to_timestamp(modifyTime) # 还没有对时间处理
                other.FileExtention = fileExtention
                other.Deleted = self._convert_model_deleted_status(deleted)
                # video.MimeType = mimeType

                # if md_type == 0:
                #     other.Domain = FileDomain.Compress
                # else:
                #     other.Domain = FileDomain.Documents

                models.append(other)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error,"Get _get_other_models Failed! -{0}".format(e))

            if len(models) > self.model_max_count:
                self._push_models(models)
                models = []
            row = self.cursor.fetchone()
        self._push_models(models)


    def _push_models(self, ar_models):
        pr = ParserResults()
        pr.Categories = self.categories
        pr.Models.AddRange(ar_models)
        pr.Build(self.build)
        ds.Add(pr)

    def _get_db_string_value(self, row, index, defaultValue=""):
        return row[index] if row[index] else defaultValue
    
    def _get_db_int_value(self, row, index, defaultValue=0):
        return row[index] if row[index] else defaultValue

    def _unixtime_to_timestamp(self, timestamp):
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

    def _convert_model_deleted_status(self, v):
        if v is None:
            return DeletedState.Unknown
        elif v == 0:
            return DeletedState.Intact
        elif v == 1:
            return DeletedState.Deleted
        else:
            return DeletedState.Intact