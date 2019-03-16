#coding:utf-8

__author__ = "Xu Tao"

import clr
try:
    clr.AddReference("model_res")
except:
    pass
from PA_runtime import *
from System.IO import Path
from PA.InfraLib.ModelsV2.Base import *
from PA.InfraLib.Utils import FileTypeChecker, FileDomain

import os
import hashlib
import traceback
from PIL import Image
from PIL.ExifTags import TAGS


import model_res

VIDEOTHUMBNAILFILE = 3
THUMBNAILFILE = 2
IMAGEFILE = 1

class AppResources(object):
   
    def __init__(self, bulid, categories):
        self.res_models = []
        self.path_list = {}
        self.media_models = []
        self.node_list = {}
        self.media_path_set = set()
        self.img_thum_suffix = set()
        self.audio_thum_suffix = set()
        self.video_thum_suffix = set()
        self.checker = FileTypeChecker()
        self.prog = progress['APP', bulid]['MEDIA', '多媒体']
        self.prog_value = 0
        self.step_value = None
        self.cache_path = ds.OpenCachePath("AppResources") 
        self.descript_categories = categories
        self.build = bulid
        self.appres = model_res.Resources()
        self.unique_id = None
        self.need_load_db = False   # 是否需要存数据库

    def parse(self):
        if len(self.node_list) == 0:
            if self.prog:
                self.prog.Skip()
            raise Exception("No multimedia resource directory was passed in")
        db_path = self._get_db_path()   
        if db_path is None:
            self._reparse()
        else:
            if self.appres.need_parse(db_path):
                self.need_load_db = True
                self.appres.db_create(db_path)
                self._reparse()
                self.appres.db_insert_table_version(model_res.VERSION_KEY_DB, model_res.VERSION_VALUE_DB)
                self.appres.db_commit()
                self.appres.db_close()
            else:
                self._progess_start()
                exp = model_res.ExportModel(db_path, self.build, self.descript_categories)
                exp.set_progress(self.prog)
                exp.get_model()
                self._progress_media_models(self.media_models)
                self._progess_end()
                           
    def save_media_model(self, model):
        """
        Save media models
        """
        if model.Path not in self.media_path_set:
            self.media_models.append(model)
            self.media_path_set.add(model.Path)

    def save_res_folder(self, node, ntype):
        """
        node [Node] : multimedia resource directory \n
        type [str] :
            -Image：图片
            -Audio：音频
            -Video：视频
            -Other：多种类型资源
        """
        if node is not None:
            self.node_list[node] = ntype

    def set_thum_config(self, thum, rtype):
        if rtype.lower() == "image":
            self.img_thum_suffix.add(thum)
        elif rtype.lower() == "video":
            self.video_thum_suffix.add(thum)
        elif rtype.lower() == "audio":
            self.audio_thum_suffix.add(thum)

    def _get_all_files(self, node, all_files):
        """
        Returns all files in the current folder
        """
        files_list = node.Children
        for files in files_list:
            if files.Type == NodeType.File:
                all_files.append(files)
            else:
               self._get_all_files(files, all_files)
        return all_files

    def progress_search(self, node):
        search_models = []  # create model by search
        cached_model = []  # cached model
        res_lists = self._get_all_files(node, [])
        if len(res_lists) == 0:
            return
        for res in res_lists:
            model = self._is_created(res, self.node_list[node])  # model是否创建
            if model is None:
                continue
            model.Size = res.Size
            model.AddTime = res.CreationTime
            model.ModifyTime = res.ModifyTime
            if model.Path is None:
                # 如果是空的话,是通过search创建
                model.Path = res.AbsolutePath
                if self._is_mediafile_model(model):
                    path = res.AbsolutePath
                    model = self.assign_value_to_model(model, path)
                search_models.append(model)
                if len(search_models) >= 1000:
                    self._push_models(search_models)
                    search_models = []
            else:
                # path不为空,则是已经创建过的model
                if self._is_mediafile_model(model):
                    path = model.Path
                    model = self.assign_value_to_model(model, path)
                cached_model.append(model)
                if len(cached_model) >= 100:
                    self._push_models(cached_model, False)
                    cached_model = []
        if search_models:
            self._push_models(search_models)
        if cached_model:
            self._push_models(cached_model, False)
        self._set_progess_value()

    def _is_created(self, node, ntype):
        suffix = os.path.splitext(node.AbsolutePath)[-1][1:]
        if not suffix and ntype == "Image":
            if node.AbsolutePath.find("_") != -1:
                index = node.AbsolutePath.find("_")
                suffix = node.AbsolutePath[index+1:] 
        if node.AbsolutePath in self.path_list.keys():
            self.media_models.remove(self.path_list[node.AbsolutePath])
            return self.path_list[node.AbsolutePath]
        else:
            if ntype == "Image":
                if suffix in self.img_thum_suffix:
                    return MediaFile.ThumbnailFile()
                else:
                    return MediaFile.ImageFile()
            elif ntype == "Video":
                if suffix in self.video_thum_suffix:
                    return MediaFile.VideoThumbnailFile()
                else:
                    return MediaFile.VideoFile()
            elif ntype == "Audio":
                if suffix in self.audio_thum_suffix:
                    return
                return MediaFile.AudioFile()
            elif ntype == "Other":
                obj = self.checker.GetFileType(node.Data)  # 调用c#方法检查类型
                if obj.Domain == FileDomain.Image:
                    if suffix in self.img_thum_suffix:
                        return MediaFile.ThumbnailFile()
                    else:
                        return MediaFile.ImageFile()
                elif obj.Domain == FileDomain.Audio:
                    if suffix in self.audio_thum_suffix:
                        return
                    return MediaFile.AudioFile()
                elif obj.Domain == FileDomain.Video:
                    if suffix in self.video_thum_suffix:
                        return MediaFile.VideoThumbnailFile()
                    else:
                        return MediaFile.VideoFile()
                elif obj.Domain == FileDomain.Compress:
                    other = OtherFile.OtherFile()
                    other.Domain = FileDomain.Compress
                    return other
                elif obj.Domain == FileDomain.Documents:
                    other = OtherFile.OtherFile()
                    other.Domain = FileDomain.Documents
                    return other
                else:
                    return None

    def return_model_index(self, models):
        dicts = {}
        for i in models:
            if i.Path is None:
                continue
            dicts[i.Path] = i
        return dicts

    def _get_exif_data(self, path):
        '''获取图片metadata'''
        ret = {}
        try:
            # 把path从相对路径变成绝对路径
            _tmp = os.path.join(ds.FileSystem.MountPoint, path)
            # 判断是否是jpeg文件
            if os.path.splitext(_tmp)[1] != ".jpeg":
                if not self._is_jpeg_file(_tmp):
                    return
            img = Image.open(_tmp)
            if hasattr(img, '_getexif'):
                exifinfo = img._getexif()
                if exifinfo is not None:
                    for tag, value in exifinfo.items():
                        decoded = TAGS.get(tag, tag)
                        ret[decoded] = value
                return ret
        except:
            return {}

    def assign_value_to_model(self, image, path):
        """[get pics exif infomation]
        
        Arguments:
            image {[MediaFile.ImageFile]} -- [MediaFile.ImageFile()]
            path {[string]} -- [path of image]
        
        Returns:
            [image] -- [description]
        """
        try:
            ret = self._get_exif_data(path)
            if not ret:
                return image
            abs_path = os.path.join(ds.FileSystem.MountPoint, path)
            image.FileName = os.path.basename(abs_path)
            image.Size = os.path.getsize(abs_path)
            image.Path = path
            addTime = os.path.getctime(abs_path)
            image.FileSuffix = 'jpeg'
            image.MimeType = 'image'
            image.AddTime = self._get_timestamp(addTime)
            location = Location(image)
            coordinate = Coordinate()
            if 'GPSInfo' in ret.keys():
                latitude = 0.0
                longitude = 0.0
                try:
                    GPSInfo = ret['GPSInfo']
                    latitudeFlag = GPSInfo[1]
                    latitude = float(GPSInfo[2][0][0])/float(GPSInfo[2][0][1]) + float(GPSInfo[2][1][0])/float(GPSInfo[2][1][1])/float(60) + float(GPSInfo[2][2][0])/float(GPSInfo[2][2][1])/float(3600)
                    longitudeFlag = GPSInfo[3]
                    longitude = float(GPSInfo[4][0][0])/float(GPSInfo[4][0][1]) + float(GPSInfo[4][1][0])/float(GPSInfo[4][1][1])/float(60) + float(GPSInfo[4][2][0])/float(GPSInfo[4][2][1])/float(3600)
                    coordinate.Longitude = longitude
                    coordinate.Latitude = latitude
                    coordinate.Type = CoordinateType.GPS
                    location.Coordinate = coordinate
                except:
                    pass
            location.Time = image.AddTime
            location.SourceType = LocationSourceType.Media
            image.Location = location
            modifyTime = os.path.getmtime(abs_path)
            image.ModifyTime = self._get_timestamp(modifyTime)
            if 'ExifImageWidth' in ret.keys() and 'ImageLength' in ret.keys():
                width = ret['ExifImageWidth']
                height = ret['ImageLength']
                image.Height = height
                image.Width = width
            takenDate = os.path.getctime(abs_path)
            image.TakenDate = self._get_timestamp(takenDate)
            if '42036' in ret:
                image.Aperture = str(ret['42036'])
            if 'Artist' in ret:
                image.Artist = ret['Artist']
            if 'ColorSpace' in ret:
                ss = 'sRGB' if ret['ColorSpace'] == 1 else ret['ColorSpace']
                image.ColorSpace = str(ss)
            if 'ExifVersion' in ret:
                image.ExifVersion = str(ret['ExifVersion'])
            if 'ExposureProgram' in ret:
                image.ExposureProgram = str(ret['ExposureProgram'])
            if 'ExposureTime' in ret:
                if len(ret['ExposureTime']) == 2:
                    et = ret['ExposureTime']
                    image.ExposureTime = str(int(et[0]/et[1]))
            if 'FocalLength' in ret:
                if len(ret['FocalLength']) == 2:
                    fl = ret['FocalLength']
                    image.FocalLength = str(int(fl[0]/fl[1]))
            if 'ISOSpeedRatings' in ret:
                image.ISO = str(ret['ISOSpeedRatings'])
            if 'Make' in ret:
                image.Make = ret['Make']
            if 'Model' in ret:
                image.Model = ret['Model']
            if 'ExifImageWidth' in ret:
                image.Resolution = str(ret['ExifImageWidth']) + '*' + str(ret['ExifImageHeight'])
            if 'Software' in ret:
                image.Software = ret['Software']
            if 'XResolution' in ret:
                if len(ret['XResolution']) == 2:
                    xr = ret['XResolution']
                    image.XResolution = str(int(xr[0]/xr[1]))
                    if image.XResolution is not '' and image.XResolution is not None:
                        image.XResolution = image.XResolution + ' dpi'
            if 'YResolution' in ret:
                if len(ret['YResolution']) == 2:
                    yr = ret['YResolution']
                    image.XResolution = str(int(yr[0]/yr[1]))
                    if image.YResolution is not '' and image.YResolution is not None:
                        image.YResolution = image.YResolution + ' dpi'
            return image
        except:
            return image

    def _get_timestamp(self, timestamp):
        try:
            if isinstance(timestamp, (long, float, str)) and len(str(timestamp)) > 10:
                timestamp = int(str(timestamp)[:10])
            if isinstance(timestamp, (Int64, long, int)) and len(str(timestamp)) == 10:
                ts = TimeStamp.FromUnixTime(timestamp, False)
                if not ts.IsValidForSmartphone():
                    ts = TimeStamp.FromUnixTime(0, False)
                return ts
        except:
            return TimeStamp.FromUnixTime(0, False)

    def _set_progess_value(self):
        if self.prog is not None:
            if self.prog_value > 100:
                self.prog_value = 100
                self.prog.Value = self.prog_value
                return
            self.prog_value += self.step_value
            self.prog.Value = self.prog_value

    def _push_models(self, ar_models, save_db=True):
        pr = ParserResults()
        pr.Categories = self.descript_categories
        pr.Models.AddRange(ar_models)
        try:         
            pr.Build(self.build)
        except Exception as e:
            pass
        ds.Add(pr)
        if self.need_load_db and save_db:
            self.assign_value_to_obj_from_model(ar_models)

    def set_unique_id(self, v):
        """
        设置唯一id
        """
        self.unique_id = v

    def _progess_start(self):
        if self.prog:
            self.prog.Start()
            self.prog.Value = 0

    def _progess_end(self):
        if self.prog:
            self.prog.Value = 100
            self.prog.Finish(True)
    
    def _get_db_path(self):
        try:
            m = hashlib.md5()   
            m.update(self.unique_id.encode(encoding = 'utf-8'))
            _tmp = os.path.join(self.cache_path, self.build)
            if not os.path.exists(_tmp):
                os.makedirs(_tmp)
            db_path = os.path.join(_tmp, m.hexdigest() + ".db")
            return db_path
        except:
            return None
    
    def assign_value_to_obj_from_model(self, ar_models):
        for model in ar_models:
            if isinstance(model, (MediaFile.ImageFile, MediaFile.ThumbnailFile, MediaFile.VideoThumbnailFile)):
                try:
                    mediaimage = model_res.MediaImage()
                    mediaimage.height = model.Height
                    mediaimage.width = model.Width
                    if model.Location:
                        mediaimage.longitude = model.Location.Coordinate.Longitude
                        mediaimage.latitude = model.Location.Coordinate.Latitude
                    mediaimage.iso = model.ISO
                    mediaimage.make = model.Make
                    mediaimage.model = model.Model
                    mediaimage.artist = model.Artist
                    mediaimage.software = model.Software
                    mediaimage.aperture = model.Aperture
                    mediaimage.takenDate= model.TakenDate
                    mediaimage.colorSpace = model.ColorSpace
                    mediaimage.resolution = model.Resolution
                    mediaimage.focalLength = model.FocalLength
                    mediaimage.xresolution = model.XResolution
                    mediaimage.yresolution = model.YResolution
                    mediaimage.exifVersion = model.ExifVersion
                    mediaimage.exposureTime = model.ExposureTime
                    mediaimage.exposureProgram= model.ExposureProgram
                    mediaimage.fileName = model.FileName
                    mediaimage.fileExtention = model.FileSuffix
                    mediaimage.path = model.Path
                    mediaimage.size = model.Size
                    mediaimage.addTime = model.AddTime
                    mediaimage.modifyTime = model.ModifyTime
                    mediaimage.deleted = self._get_deleted_status(model.Deleted)
                    mediaimage.type = self._get_media_type(model)

                    self.appres.db_insert_table_image(mediaimage)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Info, "assign_value_to_obj_from_model failed {0}".format(e))

            elif isinstance(model, MediaFile.AudioFile):
                try:
                    mediaudio = model_res.MediaAudio()
                    mediaudio.fileName = model.FileName
                    mediaudio.fileExtention = model.FileSuffix
                    mediaudio.path = model.Path
                    mediaudio.size = model.Size
                    mediaudio.addTime = model.AddTime
                    mediaudio.modifyTime = model.ModifyTime
                    mediaudio.album = model.Album
                    mediaudio.artist = model.Artist
                    mediaudio.deleted = self._get_deleted_status(model.Deleted)

                    self.appres.db_insert_table_audio(mediaudio)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Info, "assign_value_to_obj_from_model failed {0}".format(e))

            elif isinstance(model, MediaFile.VideoFile):
                try:
                    mediavideo = model_res.MediaVideo()
                    mediavideo.fileName = model.FileName
                    mediavideo.fileExtention = model.FileSuffix
                    mediavideo.path = model.Path
                    mediavideo.size = model.Size
                    mediavideo.addTime = model.AddTime
                    mediavideo.modifyTime = model.ModifyTime
                    mediavideo.deleted = self._get_deleted_status(model.Deleted)

                    self.appres.db_insert_table_video(mediavideo)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Info, "assign_value_to_obj_from_model failed {0}".format(e))
            
            else:
                try:
                    if model is None:
                        continue
                    mediaother = model_res.MediaOther()
                    mediaother.fileName = model.FileName
                    mediaother.fileExtention = model.FileSuffix
                    mediaother.path = model.Path
                    mediaother.size = model.Size
                    mediaother.addTime = model.AddTime
                    mediaother.modifyTime = model.ModifyTime
                    mediaother.deleted = self._get_deleted_status(model.Deleted)

                    self.appres.db_insert_table_other(mediaother)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Info, "assign_value_to_obj_from_model failed {0}".format(e))
        self.appres.db_commit()
    
    def _get_media_type(self, model):
        if isinstance(model, MediaFile.VideoThumbnailFile):
            return VIDEOTHUMBNAILFILE
        elif isinstance(model, MediaFile.ThumbnailFile):
            return THUMBNAILFILE
        else:
            return IMAGEFILE
    
    def _is_mediafile_model(self, model):
        """
        VideoThumbnailFile和ThumbnailFile都是继承于MediaFile.ImageFile
        但是只有MediaFile.ImageFile可能有exif信息,所以需要判断一下,
        如果是MediaFile.ImageFile 返回True, 否则返回False
        """
        if isinstance(model, MediaFile.VideoThumbnailFile):
            return False
        elif isinstance(model, MediaFile.ThumbnailFile):
            return False
        elif isinstance(model, MediaFile.ImageFile):
            return True
        else:
            return False
    
    def _reparse(self):
        self._progess_start()
        self.step_value = 100 / len(self.node_list)
        if len(self.media_models) != 0:
            self.path_list = self.return_model_index(self.media_models)
        start = time.time()
        map(self.progress_search, self.node_list.keys())
        self.res_models.extend(self.media_models)
        self._push_models(self.media_models)
        self._progess_end()
        end = time.time()
        TraceService.Trace(TraceLevel.Info, "搜索{0}多媒体共计耗时{1}s".format(self.build, int(end-start)))

    def _progress_media_models(self, models):
        cache_model = []
        for model in models:
            path = model.Path
            if path is None:
                continue
            if self._is_mediafile_model(model):
                model = self.assign_value_to_model(model, path)
            cache_model.append(model)
            if len(cache_model) > 100:
                self._push_models(cache_model, False)  # 不需要存到数据库,直接push
                cache_model = []
        self._push_models(cache_model, False)

    def _is_jpeg_file(path):
        with open(path, "r") as f:
            data = f.read(11)
            if data[:4] != b'\xff\xd8\xff\xe0': 
                return False
            if data[6:] != b'JFIF\0': 
                return False
            return True

    @staticmethod
    def _get_deleted_status(v):
        if v == DeletedState.Deleted:
            return 1
        elif v == DeletedState.Intact:
            return 0
        else:
            return 2