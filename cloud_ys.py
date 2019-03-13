# coding:utf-8

from PA_runtime import *

from PA.InfraLib.ModelsV2.Base import *
from PA.InfraLib.ModelsV2.SafeCloud import *

import json
import time
import traceback


class YinShi(object):

    def __init__(self, node, extract_deleted, extract_source):
        self.root = node
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source

    def parse(self):
        results = []
        if self.root is None:
            return
        data_dir = DataDirectory()
        data_dir.Path = self.root.AbsolutePath
        results.append(data_dir)
        for node in self.root.Children:
            if node.Type == NodeType.Directory:
                user = self._ge_user_account(node)
                results.append(user)
                results.extend(self._get_all_binding_devices(node, user))
                results.extend(self._get_wacthlists(node, user))
                results.extend(self._get_my_favorites(node, user))
                results.extend(self._get_cloudvideo_list(node, user))
        return results

    def _ge_user_account(self, node):
        account_id = node.Name
        user = UserAccount()
        user.Account = account_id
        user.SourceFile = node.AbsolutePath
        return user

    def _get_all_binding_devices(self, node, appUserAccount):
        models = []
        if node is None:
            return
        devices_node = node.GetByPath("所有绑定设备/AllDevices.txt")
        if devices_node is None:
            return
        try:
            with open(devices_node.PathWithMountPoint, "r") as f:
                data = json.loads(f.read())
                cloud_devices = self._get_json_string_value(
                    data, "cloudDevices")
                for item in cloud_devices:
                    owner_id = self._get_json_string_value(item, "ownerId")
                    dev_name = self._get_json_string_value(item, "devName")
                    dev_enable = self._get_json_string_value(item, "enable")
                    dev_model = self._get_json_string_value(item, "devModel")
                    create_time = self._format_str_time(
                        self._get_json_string_value(item, "createTime"))
                    expire_time = self._format_str_time(
                        self._get_json_string_value(item, "expireTime"))
                    update_time = self._format_str_time(
                        self._get_json_string_value(item, "updateTime"))

                    bindingDev = BindingDevice()
                    bindingDev.SourceFile = devices_node.AbsolutePath
                    bindingDev.AppUserAccount = appUserAccount
                    bindingDev.DeviceName = dev_name
                    bindingDev.DeviceEnable = dev_enable
                    bindingDev.DeviceModel = dev_model
                    bindingDev.CreateTime = create_time
                    bindingDev.ExpireTime = expire_time
                    bindingDev.UpdateTime = update_time

                    models.append(bindingDev)
        except Exception as e:
            TraceService.Trace(
                TraceLevel.Error, "{0}".format(traceback.format_exc()))
        return models

    def _get_wacthlists(self, node, appUserAccount):
        models = []
        if node is None:
            return
        watches_node = node.GetByPath("我的关注/关注.txt")
        if watches_node is None:
            return
        try:
            with open(watches_node.PathWithMountPoint, "r") as f:
                data = json.loads(f.read())
                watches_users = self._get_json_string_value(
                    data, "userSubscribers")
                for item in watches_users:
                    user_id = self._get_json_string_value(item, "userId")
                    user_name = self._get_json_string_value(item, "userName")
                    nick_name = self._get_json_string_value(item, "nickname")
                    avatar_path = self._get_json_string_value(
                        item, "avatarPath")
                    attention_time = self._format_int_time(
                        self._get_json_int_value(item, "attentionTime"))
                    status = self._get_json_int_value(item, "status")

                    follow_user = Follow()
                    follow_user.SourceFile = watches_node.AbsolutePath
                    follow_user.AppUserAccount = appUserAccount
                    follow_user.FolloweredId = user_id
                    follow_user.UserName = user_name
                    follow_user.NickName = nick_name
                    follow_user.HeadPortraitPath = avatar_path
                    follow_user.Status = status
                    follow_user.AttentionTime = attention_time

                    models.append(follow_user)
        except:
            TraceService.Trace(
                TraceLevel.Error, "{0}".format(traceback.format_exc()))
        return models

    def _get_my_favorites(self, node, appUserAccount):
        models = []
        if node is None:
            return
        favor_node = node.GetByPath("我的收藏/全部收藏.txt")
        if favor_node is not None:
            try:
                with open(favor_node.PathWithMountPoint, "r") as f:
                    data = json.loads(f.read())
                    favorites_lists = self._get_json_string_value(
                        data, "favorites")
                    for item in favorites_lists:
                        owner_id = self._get_json_string_value(item, "ownerId")
                        owner_name = self._get_json_string_value(
                            item, "ownerName")
                        owner_avatar = self._get_json_string_value(
                            item, "ownerAvatar")
                        nick_name = self._get_json_string_value(
                            item, "nickname")
                        title = self._get_json_string_value(item, "title")
                        status = self._get_json_int_value(item, "status")
                        conver_url = self._get_json_string_value(
                            item, "coverUrl")
                        remark = self._get_json_string_value(item, "memo")
                        longitude = self._get_json_string_value(
                            item, "longitude")
                        latitude = self._get_json_string_value(
                            item, "latitude")
                        address = self._get_json_string_value(item, "address")
                        upload_time = self._format_str_time(self._get_json_string_value(
                            item, "uploadTime"))
                        like_count = self._get_json_int_value(
                            item, "likeCount")
                        remark_count = self._get_json_int_value(
                            item, "remarkCount")
                        viewed_count = self._get_json_int_value(
                            item, "viewedCount")

                        favorites = Yingshi.Favorites()
                        favorites.SourceFile = favor_node.AbsolutePath
                        favorites.AppUserAccount = appUserAccount
                        favorites.AccountId = owner_id
                        favorites.UserName = owner_name
                        favorites.NickName = nick_name
                        favorites.HeadPortraitPath = owner_avatar
                        favorites.Title = title
                        favorites.CoverUrl = conver_url
                        favorites.Status = int(status)
                        favorites.Remark = remark
                        favorites.LikeCount = like_count
                        favorites.RemarkCount = remark_count
                        favorites.ViewedCount = viewed_count
                        favorites.UploadTime = upload_time

                        if longitude and latitude:
                            loc = Location()
                            loc.Coordinate = Coordinate(
                                float(longitude), float(latitude))
                            loc.AddressName = address
                            favorites.Location = loc

                        models.append(favorites)
            except:
                TraceService.Trace(
                    TraceLevel.Error, "{0}".format(traceback.format_exc()))
        return models

    def _get_cloudvideo_list(self, node, appUserAccount):
        if node is None:
            return
        cloudvideo_node = node.GetByPath("云视频列表/CloudVideo.txt")
        models = []
        if cloudvideo_node is None:
            return
        try:
            with open(cloudvideo_node.PathWithMountPoint, "r") as f:
                data = json.loads(f.read())
                video_lists = self._get_json_string_value(data, "files")
                for item in video_lists:
                    owner_id = self._get_json_string_value(item, "owner_id")
                    file_name = self._get_json_string_value(item, "file_name")
                    file_type = self._get_json_int_value(item, "file_type")
                    file_size = self._get_json_int_value(item, "file_size")
                    start_time = self._format_special_time(
                        self._get_json_string_value(item, "start_time"))
                    stop_time = self._format_special_time(
                        self._get_json_string_value(item, "stop_time"))
                    create_time = self._format_str_time(
                        self._get_json_string_value(item, "create_time"))
                    cover_pic = self._get_json_string_value(item, "coverPic")
                    download_path = self._get_json_string_value(
                        item, "downloadPath")

                    cloud_video = Yingshi.CloudVideo()
                    cloud_video.SourceFile = cloudvideo_node.AbsolutePath
                    cloud_video.AppUserAccount = appUserAccount
                    cloud_video.OwnerId = owner_id
                    cloud_video.HeadPortraitPath = cover_pic
                    cloud_video.FileName = file_name
                    cloud_video.FileSize = file_size
                    cloud_video.FileType = file_type
                    cloud_video.DownloadPath = download_path
                    cloud_video.CreateTime = create_time
                    cloud_video.StartTime = start_time
                    cloud_video.StopTime = stop_time

                    models.append(cloud_video)
        except:
            TraceService.Trace(
                TraceLevel.Error, "{0}".format(traceback.format_exc()))
        return models

    @staticmethod
    def _get_json_string_value(data, key, defalut_value=""):
        if key in data:
            return data[key]
        else:
            return defalut_value

    @staticmethod
    def _get_json_int_value(data, key, defalut_value=0):
        if key in data:
            return data[key]
        else:
            return defalut_value

    @staticmethod
    def _format_str_time(v):
        fmt = "%Y-%m-%d %H:%M:%S"
        try:
            struct_time = time.strptime(v, fmt)
            unix_time = time.mktime(struct_time)
            ts = TimeStamp.FromUnixTime(unix_time, False)
            if not ts.IsValidForSmartphone():
                ts = None
            return ts
        except:
            return None

    @staticmethod
    def _format_int_time(timestamp):
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
    def _format_special_time(v):
        fmt = "%Y%m%d%H%M%S"
        try:
            struct_time = time.strptime(v, fmt)
            unix_time = time.mktime(struct_time)
            ts = TimeStamp.FromUnixTime(unix_time, False)
            if not ts.IsValidForSmartphone():
                ts = None
            return ts
        except:
            return None


def analyze_yinshi(node, extract_deleted, extract_source):
    pr = ParserResults()
    results = YinShi(node, extract_deleted, extract_source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("萤石云视频")
    return pr
