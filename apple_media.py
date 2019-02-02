# -*- coding: utf-8 -*-
import os
import PA_runtime
import sqlite3
from PA_runtime import *
import threading
import traceback
import clr
try:
    clr.AddReference('model_media')
    clr.AddReference('mimetype_dic')
except:
    pass
del clr

import os
import re
import hashlib
from model_media import *
import model_media
import mimetype_dic
from mimetype_dic import dic1, dic2

from PIL import Image
from PIL.ExifTags import TAGS

VERSION_APP_VALUE = 3


class MediaParse(model_media.MM):
    def __init__(self, node, extractDeleted, extractSource):
        self.node = node
        self.extractDeleted = extractDeleted
        self.extractSource = extractSource
        self.db = None
        self.cache_path = ds.OpenCachePath("MEDIA")
        md5_db = hashlib.md5()
        db_name = 'media'
        md5_db.update(db_name.encode(encoding = 'utf-8'))
        self.db_cache = self.cache_path + "\\" + md5_db.hexdigest().upper() + ".db"
        self.mime_dic = mimetype_dic.dic1
        self.media = {}
        self.media_url = ''

    def parse(self):
        if self.need_parse(self.db_cache, VERSION_APP_VALUE):
            self.db_create(self.db_cache)
            self.analyze_media()
            self.analyze_thumbnail()
            self.db_insert_table_version(model_media.VERSION_KEY_DB, model_media.VERSION_VALUE_DB)
            self.db_insert_table_version(model_media.VERSION_KEY_APP, VERSION_APP_VALUE)
            self.db_commit()
            self.db_close()
        models = model_media.Generate(self.db_cache, model_media.COORDINATE_TYPE_GOOGLE).get_models()
        print(models)
        return models

    def analyze_media(self):
        try:
            db = SQLiteParser.Database.FromNode(self.node, canceller)
            if db is None:
                return 
            ts = SQLiteParser.TableSignature('ZGENERICASSET')
            media_dir = self.node.Parent.Parent.AbsolutePath
            pdir = self.node.Parent.Parent.PathWithMountPoint
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    media = Media()
                    canceller.ThrowIfCancellationRequested()
                    media.id = self._db_record_get_int_value(rec, 'Z_PK')
                    dir = self._db_record_get_string_value(rec, 'ZDIRECTORY')
                    name = self._db_record_get_string_value(rec, 'ZFILENAME')
                    if name == '':
                        continue
                    media.url = os.path.normcase(media_dir + '/' + dir + '/' + name)
                    pd = os.path.normcase(pdir + '/' + dir + '/' + name)
                    self.media_url = pd
                    media.deleted = rec.IsDeleted
                    media.size = os.path.getsize(pd)
                    media.add_date = self.transtime(rec, 'ZADDEDDATE')
                    media.modify_date = self.transtime(rec, 'ZMODIFICATIONDATE')
                    media.suffix = re.sub(".*\.", "", name).lower()
                    media.title = name
                    media.width = self._db_record_get_int_value(rec, 'ZWIDTH')
                    media.height = self._db_record_get_int_value(rec, 'ZHEIGHT')
                    if not media.suffix.lower() in dic1:
                        continue
                    fileType = dic1[media.suffix.lower()]
                    media.type = fileType
                    latitude = self._db_record_get_value(rec, 'ZLATITUDE')
                    if latitude != -180:
                        media.latitude = latitude
                    longitude = self._db_record_get_value(rec, 'ZLONGITUDE')
                    if longitude != -180:
                        media.longitude = longitude
                    media.datetaken = self.transtime(rec, 'ZSORTTOKEN')
                    media.duration = self._db_record_get_int_value(rec, 'ZDURATION')
                    if not IsDBNull(rec['ZTRASHEDDATE'].Value):
                        media.deleted = 1
                    if name not in self.media.keys():
                        self.media[name] = media.id
                    media.source = self.node.AbsolutePath
                    self.assignment(media)
                    self.db_insert_table_media(media)

                    log = MediaLog()
                    log.media_id = media.id
                    log.add_date = media.add_date 
                    log.adjustment_date = self.transtime(rec, 'ZADJUSTMENTTIMESTAMP')
                    log.cloudbatchpublish_date = self.transtime(rec, 'ZCLOUDBATCHPUBLISHDATE') 
                    log.create_date = self.transtime(rec, 'ZDATECREATED')
                    log.faceadjustment_date = self.transtime(rec, 'ZFACEADJUSTMENTVERSION') 
                    log.lastshare_date = self.transtime(rec, 'ZLASTSHAREDDATE')
                    log.modify_date = self.transtime(rec, 'ZMODIFICATIONDATE')
                    log.taken_date = self.transtime(rec, 'ZSORTTOKEN') 
                    log.trash_date = self.transtime(rec, 'ZTRASHEDDATE')
                    log.directory = os.path.dirname(media.url)
                    log.filename = name
                    log.source = media.url
                    self.db_insert_table_media_log(log)
                except:
                    pass
            self.db_commit()
        except:
            traceback.print_exc()

    def analyze_thumbnail(self):
        try:
            thumbnailDirNode  = self.node.Parent.GetByPath("/Thumbnails/V2/DCIM/100APPLE")
            if thumbnailDirNode is not None:
                fileNodes = thumbnailDirNode.Search('/.*\..*/.*\..*$')
                for fileNode in fileNodes:
                    try:
                        thumbnail = Thumbnails()
                        filename = fileNode.PathWithMountPoint
                        im = Image.open(filename)
                        width = im.size[0]
                        height = im.size[1]
                        size = os.path.getsize(filename)
                        create_date = os.path.getctime(filename)
                        suffix = re.sub(".*\.", "", os.path.basename(filename))
                        media_name = re.sub(".*/", "", os.path.dirname(filename).replace("\\", "/"))
                        media_id = self.media[media_name]
                        filepath = fileNode.AbsolutePath
                        thumbnail.url = filepath
                        thumbnail.media_id = media_id
                        thumbnail.width = width
                        thumbnail.height = height
                        thumbnail.create_date = create_date
                        thumbnail.suffix = suffix.lower()
                        thumbnail.source = fileNode.AbsolutePath
                        self.db_insert_table_thumbnails(thumbnail)
                    except:
                        pass
                self.db_commit()
        except:
            traceback.print_exc()

    def format_mac_timestamp(self, mac_time, v = 10):
        """
        from mac-timestamp generate unix time stamp
        """
        date = 0
        date_2 = mac_time
        if mac_time < 1000000000:
            date = mac_time + 978307200
        else:
            date = mac_time
            date_2 = date_2 - 978278400 - 8 * 3600
        s_ret = date if v > 5 else date_2
        return int(s_ret)

    def transtime(self, record, column, default_value=0):
        if not IsDBNull(record[column].Value):
            try:
                mactime = record[column].Value
                return self.format_mac_timestamp(mactime)
            except Exception as e:
                return default_value
        return default_value

    def get_exif_data(self, fname):
        '''获取图片metadata'''
        ret = {}
        try:
            img = Image.open(fname)
            if hasattr(img, '_getexif'):
                exifinfo = img._getexif()
                if exifinfo != None:
                    for tag, value in exifinfo.items():
                        decoded = TAGS.get(tag, tag)
                        ret[decoded] = value
                return ret
        except:
            return {}

    def assignment(self, media):
        '''给media详细信息赋值'''
        try:
            ret = self.get_exif_data(self.media_url)
            if ret is None:
                return
            if '42036' in ret:
                media.aperture = str(ret['42036'])
            if 'Artist' in ret:
                media.artist = ret['Artist']
            #if 'DateTimeOriginal' in ret:
            #    media.datetaken = ret['DateTimeOriginal']
            if 'Software' in ret:
                media.software = ret['Software']
            if 'ExifImageWidth' in ret:
                media.resolution = str(ret['ExifImageWidth']) + '*' + str(ret['ExifImageHeight'])
            if 'XResolution' in ret:
                if len(ret['XResolution']) == 2:
                    xr = ret['XResolution']
                    media.xresolution = str(int(xr[0]/xr[1]))
            if 'YResolution' in ret:
                if len(ret['YResolution']) == 2:
                    yr = ret['YResolution']
                    media.yresolution = str(int(yr[0]/yr[1]))
            if 'ColorSpace' in ret:
                ss = 'sRGB' if ret['ColorSpace'] == 1 else ret['ColorSpace']
                media.color_space = ss
            if 'Make' in ret:
                media.make = ret['Make']
            if 'Model' in ret:
                media.model = ret['Model']
            if 'ExposureTime' in ret:
                if len(ret['ExposureTime']) == 2:
                    et = ret['ExposureTime']
                    media.exposure_time = str(int(et[0]/et[1]))
            if 'ISOSpeedRatings' in ret:
                media.iso = str(ret['ISOSpeedRatings'])
            if 'FocalLength' in ret:
                if len(ret['FocalLength']) == 2:
                    fl = ret['FocalLength']
                    media.focal_length = str(int(fl[0]/fl[1]))
            if 'ExposureProgram' in ret:
                media.exposure_program = str(ret['ExposureProgram'])
            if 'ExifVersion' in ret:
                media.exif_version = str(ret['ExifVersion'])
        except:
            traceback.print_exc()

    @staticmethod
    def _db_record_get_string_value(record, column, default_value=''):
        if not record[column].IsDBNull:
            try:
                value = str(record[column].Value)
                #if record.Deleted != DeletedState.Intact:
                #    value = filter(lambda x: x in string.printable, value)
                return value
            except Exception as e:
                return default_value
        return default_value

    @staticmethod
    def _db_record_get_int_value(record, column, default_value=0):
        if not IsDBNull(record[column].Value):
            try:
                return int(record[column].Value)
            except Exception as e:
                return default_value
        return default_value

    @staticmethod
    def _db_record_get_value(record, column, default_value=0):
        if not IsDBNull(record[column].Value):
            try:
                return record[column].Value
            except Exception as e:
                return default_value
        return default_value

    @staticmethod
    def _db_record_get_blob_value(record, column, default_value=None):
        if not record[column].IsDBNull:
            try:
                value = record[column].Value
                return bytes(value)
            except Exception as e:
                return default_value
        return default_value


def analyze_apple_media(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(MediaParse(node, extractDeleted, extractSource).parse())
    pr.Build('多媒体')
    return pr

def execute(node, extractDeleted):
    return analyze_apple_media(node, extractDeleted, False)