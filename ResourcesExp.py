#coding:utf-8

import os
from PIL import Image
from PA_runtime import *
from System.IO import Path
from PIL.ExifTags import TAGS
from PA.InfraLib.ModelsV2.Base import *
from PA.InfraLib.Utils import FileTypeChecker,FileDomain


class AppResources(object):
    
    def __init__(self):
        self.res_models = []
        self.path_list = {}
        self.media_models = []
        self.node_list = {}
        self.media_path_set = set()

    def parse(self):
        if  len(self.node_list) == 0:
            raise Exception("No multimedia resource directory was passed in")
        if len(self.media_models) != 0:
            self.path_list = self.return_model_index(self.media_models)

        map(self.progress_search, self.node_list.keys())
        self.res_models.extend(self.media_models)
        return self.res_models

    
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
            if isinstance(model, MediaFile.ImageFile):
                # 调用model_media方法，获取图片信息
                # 如果model有path属性，则取出path值
                path = None
                if model.Path is not None:
                    path = model.Path
                    self.assign_value_to_model(model, path)
                else:
                    model.Path = res.AbsolutePath
                self.res_models.append(model)
            else:
                if model.Path is None:
                    model.Path = res.AbsolutePath
                
                self.res_models.append(model)


    def _is_created(self, node, ntype):
        if node.AbsolutePath in self.path_list.keys():
            self.media_models.remove(self.path_list[node.AbsolutePath])
            return self.path_list[node.AbsolutePath]
        else:
            if ntype == "Image":
                return MediaFile.ImageFile()
            elif ntype == "Video":
                return MediaFile.VideoFile()
            elif ntype == "Audio":
                return MediaFile.AudioFile()
            elif ntype == "Other":
                checker = FileTypeChecker()
                obj = checker.GetFileType(node.Data)  # 调用c#方法检查类型
                if obj.Domain == FileDomain.Image:
                    return MediaFile.ImageFile()
                elif obj.Domain == FileDomain.Audio:
                    return MediaFile.AudioFile()
                elif obj.Domain == FileDomain.Video:
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
            path = Path.Combine(ds.FileSystem.MountPoint, path)
            img = Image.open(path)
            if hasattr(img, '_getexif'):
                exifinfo = img._getexif()
                if exifinfo != None:
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
            if ret is {}:
                return
            abs_path = Path.Combine(ds.FileSystem.MountPoint, path)
            image.FileName = os.path.basename(abs_path)
            image.Size = os.path.getsize(abs_path)
            image.Path = path
            addTime = os.path.getctime(abs_path)
            image.FileExtention = 'jpg'
            image.MimeType = 'image'
            image.AddTime = self._get_timestamp(addTime)
            location = Base.Location(image)
            coordinate = Base.Coordinate()
            try:
                latitude = None
                longitude = None
                if 'GPSInfo' in ret.keys():
                    latitude = 0.0
                    longitude = 0.0
                    try:
                        GPSInfo = ret['GPSInfo']
                        latitudeFlag = GPSInfo[1]
                        latitude = float(GPSInfo[2][0][0])/float(GPSInfo[2][0][1]) + float(GPSInfo[2][1][0])/float(GPSInfo[2][1][1])/float(60) + float(GPSInfo[2][2][0])/float(GPSInfo[2][2][1])/float(3600)
                        longitudeFlag = GPSInfo[3]
                        longitude = float(GPSInfo[4][0][0])/float(GPSInfo[4][0][1]) + float(GPSInfo[4][1][0])/float(GPSInfo[4][1][1])/float(60) + float(GPSInfo[4][2][0])/float(GPSInfo[4][2][1])/float(3600)
                    except:
                        pass
                coordinate.Longitude = longitude
                coordinate.Latitude = latitude
                coordinate.Type = CoordinateType.GPS
            except:
                pass
            location.Coordinate = coordinate
            location.Time = image.AddTime
            location.SourceType = LocationSourceType.Media
            image.Location = location
            modifyTime = os.path.getmtime(abs_path)
            image.ModifyTime = self._get_timestamp(modifyTime)
            try:
                width = None
                height = None
                if 'ExifImageWidth' in ret.keys() and 'ImageLength' in ret.keys():
                    width = ret['ExifImageWidth']
                    height = ret['ImageLength']
                image.Height = height
                image.Width = width
            except:
                pass
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
