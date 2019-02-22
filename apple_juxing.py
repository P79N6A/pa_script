# -*- coding: utf-8 -*-
__author__ = 'xiaoyuge'

import os
import PA_runtime
from PA_runtime import *
import clr
try:
    clr.AddReference('System.Data.SQLite')
    clr.AddReference('model_im')
except:
    pass
del clr

import System.Data.SQLite as SQLite
import model_im

import os
import hashlib
from System.Text import Encoding
from PA_runtime import *
import json
import traceback
import time

fid = 3

class JuxingParse(model_im.IM):
    def __init__(self, dir, extractDeleted, extractSource):
        self.dir = dir
        self.extractDeleted = extractDeleted
        self.extractSource = extractSource
        self.db = None
        self.cache_path = ds.OpenCachePath("聚星众赢")
        md5_db = hashlib.md5()
        db_name = '聚星众赢'
        md5_db.update(db_name.encode(encoding = 'utf-8'))
        self.db_cache = self.cache_path + "\\" + md5_db.hexdigest().upper() + ".db"

    def parse(self):
        try:
            self.db_create(self.db_cache)
            cache_dir = self.dir + r'\Documents\RequestCache'
            db_dir = self.dir + r'\Library\Application Support\聚星众赢\AntHouseModel.sqlite'
            self.parse_db(db_dir)
            #设置一个未知好友作为不知道消息发送对象时的好友
            friend = model_im.Friend()
            friend.account_id = self.uid
            friend.friend_id = 2
            friend.nickname = '未知好友'
            friend.fullname = '未知好友'
            self.db_insert_table_friend(friend)
            self.parse_cache(cache_dir)
            models = model_im.GenerateModel(self.db_cache).get_models()
            return models
        except:
            traceback.print_exc()

    def parse_db(self, db_dir):
        try:
            models = []
            db = SQLite.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(db_dir))
            db.Open()
            db_cmd = SQLite.SQLiteCommand(db)
            try:
                if db is None:
                    return
                db_cmd.CommandText = '''select ZMEMBERDATA from ZAHMEMBER'''
                sr = db_cmd.ExecuteReader()
                while (sr.Read()):
                    account = model_im.Account()
                    if canceller.IsCancellationRequested:
                        break
                    content = self._db_reader_get_blob_value(sr, 0)
                    if content is not None:
                            tree = BPReader.GetTree(MemoryRange.FromBytes(Array[Byte](bytearray(content))))
                            if tree is None:
                                break
                            member_dic = self.bplist2json(tree)
                            sex = self.verify_dic(member_dic, 'sex')
                            username = self.verify_dic(member_dic, 'username')
                            group = self.verify_dic(member_dic, 'group')
                            nickname = self.verify_dic(member_dic, 'nickname')
                            province = self.verify_dic(member_dic, 'province')
                            wechat = self.verify_dic(member_dic, 'wechat')
                            city = self.verify_dic(member_dic, 'city')
                            password = self.verify_dic(member_dic, 'password')
                            avatar = self.verify_dic(member_dic, 'avatar')
                            pnickname = self.verify_dic(member_dic, 'pnickname')
                            name = self.verify_dic(member_dic, 'name')
                            mobile = self.verify_dic(member_dic, 'mobile')
                            self.uid = self.verify_dic(member_dic, 'uid')
                            account = model_im.Account()
                            friend = model_im.Friend()
                            account.account_id = self.uid
                            friend.account_id = self.uid
                            friend.friend = 1
                            account.nickname = nickname
                            friend.nickname = nickname
                            account.username = username
                            friend.fullname = username
                            account.password = password
                            account.photo = avatar
                            friend.photo = avatar
                            account.telephone = mobile
                            friend.telephone = mobile
                            account.gender = sex
                            friend.gender = sex
                            account.province = province
                            account.city = city
                            friend.address = province + city
                            account.signature = '微信：'+wechat + ' 推荐人：'+pnickname
                            friend.signature = '微信：'+wechat + ' 推荐人：'+pnickname
                            self.db_insert_table_account(account)
                            self.db_insert_table_friend(friend)
                self.db_commit()
            except Exception as e:
                print(e)
        except:
            traceback.print_exc()

    def parse_cache(self, cache_dir):
        try:
            for parent, dirnames, filenames in os.walk(cache_dir):
                for filename in filenames:
                    file = os.path.join(parent, filename)
                    f = open(file, 'rb')
                    content = bytes(f.read())
                    if content is not None:
                        tree = BPReader.GetTree(MemoryRange.FromBytes(Array[Byte](bytearray(content))))
                        if tree is None:
                            break
                        bplist_dic = self.bplist2json(tree)
                        self.generate_model(bplist_dic)
                    f.close()
        except:
            pass

    def bplist2json(self, value):
        if str(type(value)) != "<type 'Dictionary[str, IKNode]'>" and str(type(value)) != "<type 'BPObjectTreeNode'>" and str(type(value)) != "<type 'List[IKNode]'>":
            return value
        d = {}
        for c, j in enumerate(value):
            key = 'key'+str(c) if j.Key is None else j.Key
            if key == 'attachments':
                pass
            v = '' if j.Value is None else j.Value
            v = self.bplist2json(v)
            d[str(key)] = v
        if d == {}:
            if str(type(value)) == "<type 'List[IKNode]'>":
                if len(value) == 0:
                    return ''
                else:
                    return self.bplist2json(value.Value)
            if str(type(value)) == "<type 'BPObjectTreeNode'>":
                return self.bplist2json(value.Value)
            return str(value.Value)
        return d

    def generate_model(self, bplist_dic):
        try:
            global fid
            if 'data' not in bplist_dic:
                return []
            elif bplist_dic['data'] is '':
                return []
            data = bplist_dic['data']
            #获取用户数据（设置为系统消息）
            if not isinstance(data, dict):
                return[]
            if 'agent_title' in data:
                agent_title = self.verify_dic(data, 'agent_title')
                regist_time = self.verify_dic(data, 'reg_time')
                province = self.verify_dic(data, 'province')
                city = self.verify_dic(data, 'city')
                address = self.verify_dic(data, 'address')
                area = self.verify_dic(data, 'area')
                agent_money = self.verify_dic(data, 'agent_money')
                nickname = self.verify_dic(data, 'nickname')
                username = self.verify_dic(data, 'username')
                telecode = self.verify_dic(data, 'telecode')
                mobile = self.verify_dic(data, 'mobile')
                sex = self.verify_dic(data, 'sex')
                parent_nickname = self.verify_dic(data, 'parent_nickname')
                parent_name = self.verify_dic(data, 'parent_name')
                parent_telecode = self.verify_dic(data, 'parent_telecode')
                parent_mobile = self.verify_dic(data, 'parent_mobile')
                friend = model_im.Friend()
                friend.account_id = self.uid
                friend.nickname = nickname
                friend.fullname = username
                friend.telephone = mobile
                friend.address = province + city + address
                friend.signature = '代理头衔：'+agent_title+' 代理金额：'+agent_money+' 上级代理人:'+parent_name+' 上级代理人联系方式：'+parent_telecode+parent_mobile
                self.db_insert_table_friend(friend)
            #推荐消息
            elif 'share_url' in data:
                desc = self.verify_dic(data, 'desc')
                seller_name = self.verify_dic(data, 'seller_name')
                logo = self.verify_dic(data, 'logo')
                share_url = self.verify_dic(data, 'share_url')
                title = self.verify_dic(data, 'title')
                message = model_im.Message()
                message.account_id = self.uid
                message.talker_id = 2
                message.talker_name = '未知'
                message.sender_id = 1
                message.is_sender = 1
                message.type = model_im.FAVORITE_TYPE_LINK
                message.talker_type = model_im.CHAT_TYPE_FRIEND
                link = model_im.Link()
                message.link_id = link.link_id
                link.title = title
                link.url = share_url
                link.content = seller_name +  ':' + desc
                link.image = logo
                self.db_insert_table_link(link)
                self.db_insert_table_message(message)
            #刷新好友[设置为系统消息]
            elif 'login' in data:
                name = self.verify_dic(data, 'name')
                group_level = self.verify_dic(data, 'group_level')
                username = self.verify_dic(data, 'username')
                author_img = self.verify_dic(data, 'author_img')
                pnickname = self.verify_dic(data, 'pnickname')
                group = self.verify_dic(data, 'group')
                sex = self.verify_dic(data, 'sex')
                puid = self.verify_dic(data, 'puid')
                province = self.verify_dic(data, 'province')
                signature = self.verify_dic(data, 'signature')
                address = self.verify_dic(data, 'address')
                id_number = self.verify_dic(data, 'id_number')
                usable_money = self.verify_dic(data, 'usable_money')
                telcode = self.verify_dic(data, 'telcode')
                last_login_time = self.verify_dic(data, 'last_login_time')
                avatar = self.verify_dic(data, 'avatar')
                reg_time = self.verify_dic(data, 'reg_time')
                city = self.verify_dic(data, 'city')
                qq = self.verify_dic(data, 'qq')
                email = self.verify_dic(data, 'last_login_ip')
                nickname = self.verify_dic(data, 'nickname')
                pusername = self.verify_dic(data, 'pusername')
                mobile = self.verify_dic(data, 'mobile')
                wechat = self.verify_dic(data, 'wechat')
                friend = model_im.Friend()
                friend.account_id = self.uid
                friend.nickname = nickname
                friend.fullname = username
                friend.photo = avatar
                friend.gender = sex
                friend.telephone = mobile
                friend.email = email
                friend.address = province + city + address
                friend.signature = '上级代理人：'+pnickname+' 所属群组：'+group+' 身份证号：'+id_number+' 注册时间:'+reg_time+' qq：'+qq+' 微信：'+wechat+' 个性签名：'+signature
                self.db_insert_table_friend(friend)
            elif 'data' in data:
                if not isinstance(data['data'], dict):
                    return []
                if 'key0' in data['data']:
                    #推荐成员
                    if 'layer_title' in data['data']['key0']:
                        data = data['data']['key0']
                        layer_title = self.verify_dic(data, 'layer_title')
                        uid = self.verify_dic(data, 'uid')
                        avatar = self.verify_dic(data, 'avatar')
                        sex = self.verify_dic(data, 'sex')
                        name = self.verify_dic(data, 'name')
                        reg_time = self.verify_dic(data, 'reg_time')
                        group_icon = self.verify_dic(data, 'group_icon')
                        group_title = self.verify_dic(data, 'group_title')
                        nickname = self.verify_dic(data, 'nickname')
                        message = model_im.Message()
                        message.account_id = self.uid
                        message.talker_id = 2
                        message.talker_name = '未知'
                        message.is_sender = 1
                        message.type = model_im.MESSAGE_CONTENT_TYPE_LINK
                        message.content = '推荐成员\n' + 'id:' + uid + '\n姓名：' + name + '\n注册时间：' + reg_time + '头衔：' + layer_title 
                        message.talker_type = model_im.CHAT_TYPE_FRIEND
                        self.db_insert_table_message(message)
                        friend = model_im.Friend()
                        friend.account_id = self.uid
                        friend.nickname = nickname
                        friend.fullname = name
                        friend.photo = avatar
                        friend.gender = sex
                        self.db_insert_table_friend(friend)
                    #动态
                    elif 'islike' in data['data']['key0']:
                        dat = data['data']
                        for key in dat.keys():
                            data = dat[key]
                            view = self.verify_dic(data, 'view')
                            comment_list = self.verify_dic(data, 'comment_list')
                            create_time = self.verify_dic(data, 'create_time')
                            id = self.verify_dic(data, 'id')
                            description = self.verify_dic(data, 'description')
                            content = self.verify_dic(data, 'content')
                            video_url = self.verify_dic(data, 'video_url')
                            comment_count = self.verify_dic(data, 'comment_count')
                            avatar = self.verify_dic(data, 'avatar')
                            islike = self.verify_dic(data, 'islike')
                            photo_url = []
                            if 'key0' in data['photo_url']:
                                for key in data['photo_url'].keys():
                                    photo_url.append(data['photo_url'][key])
                            title = self.verify_dic(data, 'title')
                            uid = self.verify_dic(data, 'uid')
                            likes = self.verify_dic(data, 'likes')
                            share = self.verify_dic(data, 'share')
                            name = self.verify_dic(data, 'name')
                            friend = model_im.Friend()
                            friend.account_id = self.uid
                            friend.friend_id = uid
                            friend.nickname = name
                            friend.fullname = name
                            friend.photo = avatar
                            feed = model_im.Feed()
                            feed.account_id = self.uid
                            feed.sender_id = uid
                            feed.content = content
                            feed.image_path = ','.join(photo_url)
                            feed.video_path = video_url
                            if create_time is not '':
                                feed.send_time = int(time.mktime(time.strptime(create_time, "%Y-%m-%d %H:%M:%S")))
                            feed.likecount = likes
                            feed.rtcount = share
                            feed.commentcount = comment_count
                            self.db_insert_table_friend(friend)
                            self.db_insert_table_feed(feed)
                    #获取群组信息
                    elif 'group_id' in data['data']['key0']:
                        dat = data['data']
                        for key in dat.keys():
                            title = self.verify_dic(data, 'title')
                            group_id = self.verify_dic(data, 'group_id')
                            level = self.verify_dic(data, 'level')
                            if group_id is '':
                                return
                            chatroom = model_im.Chatroom()
                            chatroom.account_id = self.uid
                            chatroom.chatroom_id = group_id
                            chatroom.name = title
                            self.db_insert_table_chatroom(chatroom)
                #获取位置信息
                elif 'obj' in data['data']:
                    city = self.verify_dic(data, 'city')
                    area = self.verify_dic(data, 'area')
                    personalName = self.verify_dic(data, 'personalName')
                    site = self.verify_dic(data, 'site')
                    province = self.verify_dic(data, 'province')
                    location = province + city + area + site + personalName
            #广告（系统）
            elif 'showimg' in data:
                imgurl = self.verify_dic(data, 'imgurl')
                url = self.verify_dic(data, 'url')
                message = model_im.Message()
                message.account_id = self.uid
                message.talker_id = 2
                message.talker_name = '未知'
                message.sender_id = 2
                message.is_sender = 0
                message.type = model_im.FAVORITE_TYPE_LINK
                message.talker_type = model_im.CHAT_TYPE_FRIEND
                link = model_im.Link()
                message.link_id = link.link_id
                link.url = url
                link.image = imgurl
                self.db_insert_table_link(link)
                self.db_insert_table_message(message)
            #获取帮助信息
            elif 'url' in data:
                url = self.verify_dic(data, 'url')
            elif isinstance(data, str):
                #获取认证信息
                if re.findall('author', str(data)):
                    authentification_url = data
                #获取下载信息
                elif re.findall('download', str(data)):
                    download_url = data
            self.db_commit()
        except:
            traceback.print_exc()

    def verify_dic(self, dic, key, default_value = ''):
        if key not in dic:
            return default_value
        elif dic[key] is None:
            return default_value
        else: 
            return str(dic[key])

    @staticmethod
    def _db_reader_get_blob_value(reader, index, default_value=None):
        if not reader.IsDBNull(index):
            try:
                return bytes(reader.GetValue(index))
            except Exception as e:
                return default_value
        else:
            return default_value
        return reader.GetString(index) if not reader.IsDBNull(index) else default_value
        
def analyze_apple_juxing(node, extractDeleted, extractSource):
    pr = ParserResults()
    dir = r'C:\com.anthouse.juxing'
    try:
        if os.path.exists(dir):
            progress.Start()
            pr.Models.AddRange(JuxingParse(dir, extractDeleted, extractSource).parse())
            pr.Build('聚星众赢')
            return pr
        else:
            progress.Skip()
    except:
        progress.Skip()