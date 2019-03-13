# -*- coding: utf-8 -*-
from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference('model_qq')
    clr.AddReference('bcp_im_qq')
except:
    pass
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from PA.InfraLib.Utils import *
import System.Data.SQLite as SQLite

import os
import sqlite3
import shutil
import hashlib
import model_qq
from bcp_im_qq import *


class GenerateQQBcp(object):
    def __init__(self, bcp_path, mount_path, cache_db, bcp_db, collect_target_id, contact_account_type):
        self.bcp_path = bcp_path
        self.mount_path = mount_path
        self.cache_db = cache_db
        self.bcp_db = bcp_db
        self.collect_target_id = collect_target_id
        self.contact_account_type = contact_account_type
        self.cache_path = os.path.join(bcp_path, contact_account_type)
        self.im = IM()
        self.friends = {}
        self.chatrooms = {}

    def generate(self):
        self.im.db_create(self.bcp_db)
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.cache_db))
        self.db.Open()
        self._generate_account()
        self._generate_friend()
        self._generate_group()
        self._generate_group_member()
        self._generate_message()
        self._generate_feed()
        self._generate_search()
        if self.db is not None:
            self.db.Close()
            self.db = None
        self.im.db_close()

    def _generate_account(self):
        try:
            if canceller.IsCancellationRequested:
                return []
            if not db_has_table(self.db, 'account'):
                return []

            sql = '''select account_id, nickname, username, password, photo, telephone, email, gender, age, country, 
                            province, city, address, birthday, signature, source, deleted, repeated
                     from account'''

            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                deleted = 0
                try:
                    source = db_reader_get_string_value(r, 15)
                    deleted = db_reader_get_int_value(r, 16, None)
                    account_id = db_reader_get_string_value(r, 0)
                    nickname = db_reader_get_string_value(r, 1)
                    username = db_reader_get_string_value(r, 2)
                    password = db_reader_get_string_value(r, 3)
                    photo = db_reader_get_string_value(r, 4, None)
                    telephone = db_reader_get_string_value(r, 5)
                    email = db_reader_get_string_value(r, 6)
                    gender = db_reader_get_int_value(r, 7)
                    age = db_reader_get_int_value(r, 8)
                    country = db_reader_get_string_value(r, 9)
                    province = db_reader_get_string_value(r, 10)
                    city = db_reader_get_string_value(r, 11)
                    address = db_reader_get_string_value(r, 12)
                    birthday = db_reader_get_int_value(r, 13)
                    signature = db_reader_get_string_value(r, 14)

                    account = Account(self.collect_target_id, self.contact_account_type, account_id, username)
                    account.delete_status = self._convert_delete_status(deleted)
                    account.regis_nickname = nickname
                    account.password = password
                    account.area = country
                    account.city_code = city
                    account.msisdn = telephone
                    account.email_account = email
                    account.sexcode = self._convert_sexcode(gender)
                    account.age = self._convert_sexcode(age)
                    account.postal_address = address
                    account.sign_name = signature
                    #account.birthday = birthday
                    account.user_photo = photo
                    self.im.db_insert_table_account(account)

                    if account_id not in [None, '']:
                        self.friends[self._get_user_key(account_id, account_id)] = {'nickname':nickname, 'photo': photo}
                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "bcp_qq.py Error: LINE {}".format(traceback.format_exc()))
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "bcp_qq.py Error: LINE {}".format(traceback.format_exc()))
        self.im.db_commit()

    def _generate_friend(self):
        if canceller.IsCancellationRequested:
            return []
        if not db_has_table(self.db, 'friend'):
            return []

        sql = '''select account_id, friend_id, nickname, remark, photo, type, gender, region, signature, 
                        add_time, source, deleted, repeated
                 from friend '''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                deleted = 0
                try:
                    source = db_reader_get_string_value(r, 10)
                    deleted = db_reader_get_int_value(r, 11, None)
                    account_id = db_reader_get_string_value(r, 0)
                    user_id = db_reader_get_string_value(r, 1)
                    nickname = db_reader_get_string_value(r, 2)
                    remark = db_reader_get_string_value(r, 3)
                    photo = db_reader_get_string_value(r, 4)
                    user_type = db_reader_get_int_value(r, 5)
                    gender = db_reader_get_int_value(r, 6)
                    region = db_reader_get_string_value(r, 7)
                    signature = db_reader_get_string_value(r, 8)
                    add_time = db_reader_get_int_value(r, 9)
                    
                    friend = Friend(self.collect_target_id, self.contact_account_type, account_id, None)
                    friend.delete_status = self._convert_delete_status(deleted)
                    friend.friend_id = user_id
                    friend.friend_nickname = nickname
                    friend.friend_remark = remark
                    #friend.msisdn = 
                    #friend.email_account = 
                    friend.sexcode = self._convert_sexcode(gender)
                    #friend.age = 
                    #friend.postal_address = row[10]
                    friend.sign_name = signature
                    #friend.birthday = row[11]
                    friend.user_photo = photo
                    self.im.db_insert_table_friend(friend)

                    if deleted == 0 or self._get_user_key(account_id, user_id) not in self.friends:
                        self.friends[self._get_user_key(account_id, user_id)] = {'nickname':nickname, 'remark':remark, 'photo': photo}
                    
                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "bcp_qq.py Error: LINE {}".format(traceback.format_exc()))
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "bcp_qq.py Error: LINE {}".format(traceback.format_exc()))
        self.im.db_commit()

    def _generate_group(self):
        if canceller.IsCancellationRequested:
            return []
        if not db_has_table(self.db, 'chatroom'):
            return []

        sql = '''select account_id, chatroom_id, name, photo, is_saved, notice, owner_id, create_time, join_time, 
                        sp_id, source, deleted, repeated
                 from chatroom'''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                deleted = 0
                try:
                    source = db_reader_get_string_value(r, 10)
                    deleted = db_reader_get_int_value(r, 11, None)
                    account_id = db_reader_get_string_value(r, 0)
                    user_id = db_reader_get_string_value(r, 1)
                    nickname = db_reader_get_string_value(r, 2)
                    photo = db_reader_get_string_value(r, 3, None)
                    is_saved = db_reader_get_int_value(r, 4)
                    notice = db_reader_get_string_value(r, 5)
                    owner_id = db_reader_get_string_value(r, 6)
                    create_time = db_reader_get_int_value(r, 7)
                    join_time = db_reader_get_int_value(r, 8)
                    sp_id = db_reader_get_int_value(r, 9)

                    group = Group(self.collect_target_id, self.contact_account_type, account_id, None)
                    group.delete_status = self._convert_delete_status(deleted)
                    group.group_num = user_id
                    group.group_name = nickname
                    group.friend_id = owner_id
                    # group.group_owner_nickname = None
                    group.group_member_count = 0
                    group.group_max_member_cout = 0
                    group.group_notice = notice
                    group.group_description = None
                    group.group_owner_internal_id = owner_id
                    # group.group_admin_nickname = None
                    group.create_time = create_time
                    group.groupphoto = photo
                    self.im.db_insert_table_group(group)

                    if deleted == 0 or self._get_user_key(account_id, user_id) not in self.chatrooms:
                        self.chatrooms[self._get_user_key(account_id, user_id)] = {'nickname':nickname, 'photo': photo}

                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "bcp_qq.py Error: LINE {}".format(traceback.format_exc()))
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "bcp_qq.py Error: LINE {}".format(traceback.format_exc()))
        self.im.db_commit()

    def _generate_group_member(self):
        if canceller.IsCancellationRequested:
            return
        if not db_has_table(self.db, 'chatroom_member'):
            return []

        sql = '''select account_id, chatroom_id, member_id, display_name, deleted 
                 from chatroom_member'''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                deleted = 0
                try:
                    deleted = db_reader_get_int_value(r, 4, None)
                    account_id = db_reader_get_string_value(r, 0)
                    chatroom_id = db_reader_get_string_value(r, 1)
                    member_id = db_reader_get_string_value(r, 2)
                    display_name = db_reader_get_string_value(r, 3)
                    
                    gm = GroupMember(self.collect_target_id, self.contact_account_type, account_id, None)
                    gm.delete_status = self._convert_delete_status(deleted)
                    gm.group_num = chatroom_id
                    gm.group_name = self.chatrooms.get(self._get_user_key(account_id, chatroom_id), {}).get('nickname')
                    gm.friend_id = member_id
                    gm.friend_nickname = self.friends.get(self._get_user_key(account_id, member_id), {}).get('nickname')
                    gm.friend_remark = self.friends.get(self._get_user_key(account_id, member_id), {}).get('remark')
                    gm.fixed_phone = None
                    gm.email_account = None
                    #gm.sexcode = self._convert_sexcode(row[7])
                    #gm.age = row[8]
                    #gm.postal_address = row[9]
                    #gm.sign_name = row[11]
                    #gm.birthday = row[10]
                    gm.user_photo = self.friends.get(self._get_user_key(account_id, member_id), {}).get('photo')
                    self.im.db_insert_table_group_member(gm)
                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "bcp_qq.py Error: LINE {}".format(traceback.format_exc()))
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "bcp_qq.py Error: LINE {}".format(traceback.format_exc()))
        self.im.db_commit()

    def _generate_message(self):
        if canceller.IsCancellationRequested:
            return []
        if not db_has_table(self.db, 'message'):
            return []
        sql = '''select account_id, talker_id, talker_type, sender_id, timestamp, msg_id, type, content, media_path, 
                        media_thum_path, status, is_recall, location_latitude, location_longitude, location_elevation, location_address, 
                        location_type, deal_money, deal_description, deal_remark, deal_status, deal_mode, deal_create_time, 
                        deal_expire_time, link_url, link_title, link_content, link_image, link_from, business_card_username, 
                        business_card_nickname, business_card_gender, business_card_photo, business_card_region, business_card_signature, 
                        source, deleted, repeated
                 from message '''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                deleted = 0
                try:
                    source = db_reader_get_string_value(r, 35)
                    deleted = db_reader_get_int_value(r, 36, None)
                    account_id = db_reader_get_string_value(r, 0)
                    talker_id = db_reader_get_string_value(r, 1)
                    talker_type = db_reader_get_int_value(r, 2)
                    sender_id = db_reader_get_string_value(r, 3)
                    timestamp = db_reader_get_int_value(r, 4, None)
                    msg_id = db_reader_get_string_value(r, 5)
                    msg_type = db_reader_get_int_value(r, 6)
                    content = db_reader_get_string_value(r, 7)
                    media_path = db_reader_get_string_value(r, 8)
                    media_thum_path = db_reader_get_string_value(r, 9)
                    is_recall = db_reader_get_int_value(r, 11)
                    
                    location_latitude = db_reader_get_float_value(r, 12)
                    location_longitude = db_reader_get_float_value(r, 13)
                    location_elevation = db_reader_get_float_value(r, 14)
                    location_address = db_reader_get_string_value(r, 15)
                    location_type = db_reader_get_int_value(r, 16)
                    
                    deal_money = db_reader_get_string_value(r, 17)
                    deal_description = db_reader_get_string_value(r, 18)
                    deal_remark = db_reader_get_string_value(r, 19)
                    deal_status = db_reader_get_int_value(r, 20)
                    deal_mode = db_reader_get_int_value(r, 21)
                    deal_create_time = db_reader_get_int_value(r, 22)
                    deal_expire_time = db_reader_get_int_value(r, 23)

                    link_url = db_reader_get_string_value(r, 24)
                    link_title = db_reader_get_string_value(r, 25)
                    link_content = db_reader_get_string_value(r, 26)
                    link_image = db_reader_get_string_value(r, 27)
                    link_from = db_reader_get_string_value(r, 28)

                    business_card_username = db_reader_get_string_value(r, 29)
                    business_card_nickname = db_reader_get_string_value(r, 30)
                    business_card_gender = db_reader_get_int_value(r, 31)
                    business_card_photo = db_reader_get_string_value(r, 32)
                    business_card_region = db_reader_get_string_value(r, 33)
                    business_card_signature = db_reader_get_string_value(r, 34)

                    bcp_media_path = None
                    if msg_type in [model_qq.MESSAGE_CONTENT_TYPE_IMAGE, model_qq.MESSAGE_CONTENT_TYPE_VOICE, model_qq.MESSAGE_CONTENT_TYPE_VIDEO]:
                        if media_path not in [None, ''] and (not media_path.startswith('http')):
                            bcp_media_path = self._copy_file_to_bcp_folder(media_path)
                    if talker_id.endswith("@chatroom"):
                        message = GroupMessage(self.collect_target_id, self.contact_account_type, account_id, None)
                        message.delete_status = self._convert_delete_status(deleted)
                        message.group_num = talker_id
                        message.group_name = self.chatrooms.get(self._get_user_key(account_id, talker_id), {}).get('nickname')
                        message.friend_id = sender_id
                        message.friend_nickname = self.friends.get(self._get_user_key(account_id, sender_id), {}).get('nickname')
                        if bcp_media_path is None:
                            message.content = content
                        else:
                            message.content = bcp_media_path
                        message.mail_send_time = timestamp
                        message.local_action = self._convert_local_action(account_id == sender_id)
                        message.talk_id = msg_id
                        message.media_type = self._convert_media_type(msg_type)
                        if msg_type == model_qq.MESSAGE_CONTENT_TYPE_LOCATION:
                            message.company_address = location_address
                            message.longitude = location_longitude
                            message.latitude = location_latitude
                            message.above_sealevel = location_elevation
                        self.im.db_insert_table_group_message(message)
                    else:
                        message = FriendMessage(self.collect_target_id, self.contact_account_type, account_id, None)
                        message.delete_status = self._convert_delete_status(deleted)
                        message.regis_nickname = None  # 昵称
                        message.friend_id = talker_id
                        message.friend_nickname = self.friends.get(self._get_user_key(account_id, talker_id), {}).get('nickname')
                        if bcp_media_path is None:
                            message.content = content
                        else:
                            message.content = bcp_media_path
                        message.mail_send_time = timestamp
                        message.local_action = self._convert_local_action(account_id == sender_id)
                        message.talk_id = msg_id
                        message.media_type = self._convert_media_type(msg_type)
                        if msg_type == model_im.MESSAGE_CONTENT_TYPE_LOCATION:
                            message.company_address = location_address
                            message.longitude = location_longitude
                            message.latitude = location_latitude
                            message.above_sealevel = location_elevation
                        self.im.db_insert_table_friend_message(message)

                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "bcp_qq.py Error: LINE {}".format(traceback.format_exc()))
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "bcp_qq.py Error: LINE {}".format(traceback.format_exc()))
        self.im.db_commit()

    def _generate_feed(self):
        if canceller.IsCancellationRequested:
            return []
        if not db_has_table(self.db, 'feed'):
            return []
        models = []

        sql = '''select account_id, sender_id, content, image_path, video_path, timestamp, link_url, 
                        link_title, link_content, link_image, link_from, like_id, like_count, comment_id, 
                        comment_count, location_latitude, location_longitude, location_elevation, 
                        location_address, location_type, source, deleted, repeated
                 from feed '''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                deleted = 0
                try:
                    source = db_reader_get_string_value(r, 20)
                    deleted = db_reader_get_int_value(r, 21, None)
                    account_id = db_reader_get_string_value(r, 0)
                    sender_id = db_reader_get_string_value(r, 1)
                    content = db_reader_get_string_value(r, 2)
                    image_path = db_reader_get_string_value(r, 3)
                    video_path = db_reader_get_string_value(r, 4)
                    timestamp = db_reader_get_int_value(r, 5, None)
                    link_url = db_reader_get_string_value(r, 6)
                    link_title = db_reader_get_string_value(r, 7)
                    link_content = db_reader_get_string_value(r, 8)
                    link_image = db_reader_get_string_value(r, 9)
                    link_from = db_reader_get_string_value(r, 10)
                    like_id = db_reader_get_int_value(r, 11)
                    like_count = db_reader_get_int_value(r, 12)
                    comment_id = db_reader_get_int_value(r, 13)
                    comment_count = db_reader_get_int_value(r, 14)
                    location_latitude = db_reader_get_float_value(r, 15)
                    location_longitude = db_reader_get_float_value(r, 16)
                    location_elevation = db_reader_get_float_value(r, 10)
                    location_address = db_reader_get_string_value(r, 18)
                    location_type = db_reader_get_int_value(r, 19)
                    
                    feed = Feed(self.collect_target_id, self.contact_account_type, account_id, None)
                    feed.delete_status = self._convert_delete_status(deleted)
                    feed.friend_id = sender_id
                    feed.friend_nickname = self.friends.get(self._get_user_key(account_id, sender_id), {}).get('nickname')
                    feed.mail_send_time = timestamp
                    feed.weibo_message = content
                    feed.weibo_reply_counter = comment_count
                    feed.weibo_like_counter = like_count
                    feed.company_address = location_address
                    feed.longitude = location_longitude
                    feed.latitude = location_latitude
                    feed.above_sealevel = location_elevation
                    self.im.db_insert_table_feed(feed)

                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "bcp_qq.py Error: LINE {}".format(traceback.format_exc()))
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "bcp_qq.py Error: LINE {}".format(traceback.format_exc()))
        self.im.db_commit()

    def _generate_search(self):
        if canceller.IsCancellationRequested:
            return []
        if not db_has_table(self.db, 'search'):
            return []

        sql = '''select account_id, key, timestamp, source, deleted, repeated
                 from search'''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                deleted = 0
                try:
                    source = db_reader_get_string_value(r, 3)
                    deleted = db_reader_get_int_value(r, 4, None)
                    account_id = db_reader_get_string_value(r, 0)
                    key = db_reader_get_string_value(r, 1)
                    timestamp = db_reader_get_int_value(r, 2, None)

                    search = Search(self.collect_target_id, self.contact_account_type, account_id, None)
                    search.delete_status = self._convert_delete_status(deleted)
                    search.create_time = timestamp
                    search.keyword = key
                    self.im.db_insert_table_search(search)
                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "bcp_qq.py Error: LINE {}".format(traceback.format_exc()))
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "bcp_qq.py Error: LINE {}".format(traceback.format_exc()))
        self.im.db_commit()

    def _copy_file_to_bcp_folder(self, src_file):
        src_path = (self.mount_path + src_file).replace('/', '\\')
        if not os.path.exists(src_path):
            return None
        hash = self._md5(src_file)
        dst_path = os.path.join(self.cache_path, hash[0:2], hash[2:4], os.path.basename(src_file))
        if os.path.exists(dst_path):
            return os.path.relpath(dst_path, self.bcp_path)
        dst_dir = os.path.dirname(dst_path)
        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir)
        try:
            shutil.copy(src_path, dst_path)
        except Exception as e:
            return None
        return os.path.relpath(dst_path, self.bcp_path)

    def _get_user_key(self, account_id, user_id):
        return account_id + "#*#" + user_id

    @staticmethod
    def _md5(src):
        m = hashlib.md5()
        m.update(src.encode('utf8'))
        return m.hexdigest()

    @staticmethod
    def _convert_sexcode(sexcode):
        if sexcode == model_qq.GENDER_NONE:
            return SEXCODE_UNKNOWN
        elif sexcode == model_qq.GENDER_MALE:
            return SEXCODE_MALE
        elif sexcode == model_qq.GENDER_FEMALE:
            return SEXCODE_FEMALE
        else:
            return SEXCODE_OTHER

    @staticmethod
    def _convert_media_type(media_type):
        if media_type == model_im.MESSAGE_CONTENT_TYPE_TEXT:
            return MEDIA_TYPE_TEXT
        elif media_type == model_im.MESSAGE_CONTENT_TYPE_IMAGE:
            return MEDIA_TYPE_IMAGE
        elif media_type == model_im.MESSAGE_CONTENT_TYPE_VOICE:
            return MEDIA_TYPE_VOICE
        elif media_type == model_im.MESSAGE_CONTENT_TYPE_VIDEO:
            return MEDIA_TYPE_VIDEO
        else:
            return MEDIA_TYPE_OTHER
            

    @staticmethod
    def _convert_delete_status(status):
        if status == 0:
            return DELETE_STATUS_NOT_DELETED
        else:
            return DELETE_STATUS_DELETED

    @staticmethod
    def _convert_local_action(is_send):
        if is_send == 0:
            return LOCAL_ACTION_RECEIVER
        else:
            return LOCAL_ACTION_SENDER


def db_has_table(db, table_name):
    try:
        sql = "select count(*) from sqlite_master where type='table' and name='{}' ".format(table_name)
        cmd = db.CreateCommand()
        cmd.CommandText = sql
        r = cmd.ExecuteReader()
        r.Read()
        if r and db_reader_get_int_value(r, 0) >= 1:
            return True
        else:
            return False
    except Exception as e:
        return False


def db_reader_get_string_value(reader, index, default_value=''):
    return reader.GetString(index) if not reader.IsDBNull(index) else default_value


def db_reader_get_int_value(reader, index, default_value=0):
    return reader.GetInt64(index) if not reader.IsDBNull(index) else default_value


def db_reader_get_float_value(reader, index, default_value=0):
    return reader.GetFloat(index) if not reader.IsDBNull(index) else default_value