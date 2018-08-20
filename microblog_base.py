#coding:utf-8

#
#   Celebreate Blog 分析项目
#   1.cookie
#   2.contact
#   3.blog ===> didn't find
#   4.chat ===> didn't find
#   5.search ===> found/
#
import sqlite3 as sql
import os
import sys

class MicroBlogAccount(object):
    def __init__(self):
        self.account_id = str()
        self.user_id = str()
        self.nick_name = str()
        self.fans_quantity = 0
        self.noticed_quantity = 0
        self.blog_quantity = 0
        self.message_quantity = 0
        self.real_name = str()
        self.signature = str()
        self.email = str()
        self.sex = 99
        self.area_string = str() # 所在区域，用string描述
        self.mobile = str()
        self.telephone = str() # 固定电话
        self.description = ""
        self.photo = "" # 通常情况下是本地的一个文件 或者是网上的一个链接...
        self.source_file = ""
    def generate_sqlite_turple(self):
        res = [None] * 17
        res[0] = self.account_id
        res[1] = self.user_id
        res[2] = self.nick_name
        res[3] = self.photo
        res[4] = self.real_name
        res[5] = self.signature
        res[6] = self.email
        res[7] = self.sex
        res[8] = self.area_string
        res[9] = self.mobile
        res[10] = self.telephone
        res[11] = self.description
        res[12] = self.fans_quantity
        res[13] = self.noticed_quantity
        res[14] = self.blog_quantity
        res[15] = self.message_quantity
        res[16] = self.source_file
        return res

Follow_Unkown = 0
Follow_Following = 1
Follow_Followed = 2
Follow_Other = 9

class MricoBlogFriends(object):
    def __init__(self):
        self.account_id = str()
        self.user_id = str()
        self.nick_name = str()
        self.group = str() # 分组
        self.home_page = str()
        self.follow_type = Follow_Other
        self.real_name = ""
        self.signature = ""
        self.email = "" # this is temprory, which may be hard to extract
        self.description = ""
        self.birth = 0
        self.reg_time = 0
        self.update_time = 0
        self.login_time = 0
        self.fan_quantity = 0
        self.follow_quantity = 0
        self.message_quantity = 0
        self.blog_quantity = 0
        self.icon = ""
        self.account = "" # key to refer which account's friends...
        self.source_file = "" # indicate the file we dump those information...
        self.area_string = ""
    def generate_sqlite_turple(self):
        res = [None] * 19
        res[0] = self.account_id
        res[1] = self.user_id
        res[2] = self.nick_name
        res[3] = self.home_page
        res[4] = self.follow_type
        res[5] = self.icon
        res[6] = self.real_name
        res[7] = self.signature
        res[8] = self.area_string
        res[9] = self.email
        res[10] = self.description
        res[11] = self.birth
        res[12] = self.update_time
        res[13] = self.login_time
        res[14] = self.fan_quantity
        res[15] = self.follow_quantity
        res[16] = self.message_quantity
        res[17] = self.account
        res[18] = self.source_file
        return res
    
Blog_Create = 0
Blog_Retweet = 1
Blog_Comment = 2
Blog_Support = 3
Blog_Other = 9

class MircorBlogBlogs(object):
    def __init__(self):
        self.sender = ""
        self.sender_name = ""
        self.blog_type = Blog_Other
        self.topic = ""
        self.content = ""
        self.comment_quantity = 0
        self.retweet_quantity = 0
        self.like_quantity = 0
        self.blog_id = ""
        self.media_cache = ""
        self.location = ""
        self.latitude = ""
        self.logitude = ""
        self.media_type = 0
        self.account = ""
        self.source_file = ""
        self.send_time = 0
    def generate_sqlite_turple(self):
        res = [None] * 17 
        res[0] = self.sender
        res[1] = self.sender_name
        res[2] = self.blog_type
        res[3] = self.topic
        res[4] = self.content
        res[5] = self.send_time
        res[6] = self.comment_quantity
        res[7] = self.retweet_quantity
        res[8] = self.like_quantity
        res[9] = self.blog_id
        res[10] = self.media_cache
        res[11] = self.location
        res[12] = self.media_type
        res[13] = self.logitude
        res[14] = self.latitude
        res[15] = self.account
        res[16] = self.source_file
        return res

class MicroBlogMessage(object):
    def __init__(self):
        self.sender_id = ""
        self.sender_nick = ""
        self.content = ""
        self.timestamp = 0
        self.is_sender = False
        self.talk_id = "" # 描述与谁的会话。。。
        self.media = ""
        self.media_type = 0
        self.account_id = ""
        self.source_file = ""

    def generate_sqlite_turple(self):
        res = [None] * 10
        res[0] = self.sender_id
        res[1] = self.sender_nick
        res[2] = self.content
        res[3] = self.media
        res[4] = self.media_type
        res[5] = self.timestamp
        res[6] = self.is_sender
        res[7] = self.talk_id
        res[8] = self.account_id
        res[9] = self.source_file
        return res

class MicroBlogSearch(object):
    def __init__(self):
        self.search_time = 0
        self.key_word = ""
        self.account_id = "" 
        self.source_file = ""

    def generate_sqlite_turple(self):
        pass

class BlogSqliteBase(object):
    def __init__(self, env):
        if env.get('cache') is None:
            raise ValueError('You must give out dir...')
        self.out_sqlite = os.path.join(env.get('cache'), 'BlogC37R')
        self.version = env.get('version')
        self.analysed = False
        if os.path.exists(self.out_sqlite):
            self.check_res()
        if self.analysed:
            return
        self.db = sql.connect(self.out_sqlite)
        self.create_tbs()
    
    def check_res(self):
        db = sql.connect(self.out_sqlite)
        cur = db.cursor()
        cur.execute('''
            select res, version from tb_version
        ''')
        row = cur.fetchone()
        if row is not None:
            if row[0] == 1 and row[1] == self.version:
                cur.close()
                db.close()
                self.analysed = True
                return
        cur.close()
        db.close()
        #os.path
        # 删除文件
        os.remove(self.out_sqlite)
    
    def create_tbs(self):
        cursor = self.db.cursor()
        #create version table
        cursor.execute('''
        create table if not exists tb_version(res int default 0, version int default -1)
        ''')
        # create account table
        cursor.execute('''
        create table if not exists tb_account(account_id text, user_id text, nick text,icon text, real_name text, signature text,
        email text, sex text, area text, mobile text, telephone text,description text, fq int, nq int, bq int, mq int, source_file text)
        ''')
        # create friends table
        cursor.execute('''
        create table if not exists tb_friends(account_id text, user_id text, nick_name text, home_page text, follow_type int, icon text,
        real_name text, signature text, area text, email text, description text, birth int, update_time int, login int, fq int, foq int, mq int,account text, source_file text)
        ''')
        # create blogs table
        cursor.execute('''
        create table if not exists tb_blogs(sender text, sender_nick text,blog_type int, topic text, content text, time_stamp int, cq int, req int, lq int,
        blog_id text, media text, location text, media_type int, longti text, lati text, account text, source_file text)
        ''')
        # create message sqlite
        cursor.execute('''
        create table if not exists tb_message(sender text, sender_nick text, content text,media text, media_type int, time int, is_sender int, talk_id,
        account text, source_file text)
        ''')
        # create search sqlite
        cursor.execute('''
        create table if not exists tb_search(search_time int, key_word text, account_id text, source_file text)
        ''')

    def insert_account(self, acc):
        cur = self.db.cursor()
        cur.execute('''
        insert into tb_account values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''', acc)
    
    def insert_friends(self, frnd):
        cur = self.db.cursor()
        cur.execute('''
        insert into tb_friends values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''', frnd)

    def insert_blogs(self, blog):
        cur = self.db.cursor()
        cur.execute('''
        insert into tb_blogs values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''', blog)
    
    def insert_messages(self, msg):
        cur = self.db.cursor()
        cur.execute('''
        insert into tb_message values(?,?,?,?,?,?,?,?,?,?)
        ''', msg)

class BlogBase(BlogSqliteBase):
    def __init__(self, env):
        super(BlogBase, self).__init__(env)
    
    def on_execute(self, cmd, val):
        cursor = self.db.cursor()
        cursor.execute(cmd, val)
    
    def on_commit(self):
        self.db.commit()