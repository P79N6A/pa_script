#_*_ coding:utf-8 _*_
__author__ = 'xiaoyuge'

from PA_runtime import *
import clr
try:
    clr.AddReference('System.Xml.Linq')
    clr.AddReference('model_contact')
    clr.AddReference('model_media')
    clr.AddReference('model_soundrecord')
    clr.AddReference('model_notes')
    clr.AddReference('mimetype_dic')
except:
    pass
del clr

from System.Xml.Linq import *
from PA.InfraLib.Utils import *

import model_contact
import model_media
import model_soundrecord
import model_notes
from mimetype_dic import dic1, dic2

import re
import json
import time
import hashlib
import traceback

SQL_CREATE_TABLE_ACCOUNT = '''
    create table if not exists account(
        account_id TEXT, 
        username TEXT,
        password TEXT, 
        photo TEXT, 
        gender INT, 
        address TEXT, 
        birthday INT, 
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_ACCOUNT = '''
    insert into account(account_id, username, password, photo, gender, 
                        address, birthday, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

class HuaweiParser(model_contact.MC, model_media.MM, model_soundrecord.MS, model_notes.MN):
    def __init__(self, node, extractDeleted, extractSource):
        #self.root = node
        self.node = node
        self.extractDeleted = extractDeleted
        self.extractSource = extractSource
        self.db = None
        self.db_cmd = None
        self.cache_path = ds.OpenCachePath("华为备份")
        md5_db = hashlib.md5()
        db_name = "华为云勘"
        md5_db.update(db_name.encode(encoding = 'utf-8'))
        self.cachedb = self.cache_path + "\\" + md5_db.hexdigest().upper() + ".db"
        self.sourceDB = self.cache_path + '\\HuaweiSourceDB'

    def db_create_table(self):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = SQL_CREATE_TABLE_ACCOUNT
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_contact.SQL_CREATE_TABLE_CONTACTS
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_soundrecord.SQL_CREATE_TABLE_RECORDS
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_media.SQL_CREATE_TABLE_MEDIA
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_media.SQL_CREATE_TABLE_THUMBNAILS
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_media.SQL_CREATE_TABLE_MEDIA_LOG
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_notes.SQL_CREATE_TABLE_NOTES
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_contact.SQL_CREATE_TABLE_VERSION
            self.db_cmd.ExecuteNonQuery()
    
    def db_insert_table(self, sql, values):
        try:
            if self.db_cmd is not None:
                self.db_cmd.CommandText = sql
                self.db_cmd.Parameters.Clear()
                for value in values:
                    param = self.db_cmd.CreateParameter()
                    param.Value = value
                    self.db_cmd.Parameters.Add(param)
                self.db_cmd.ExecuteNonQuery()
        except Exception as e:
            print(e)

    def db_insert_table_account(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_ACCOUNT, column.get_values())

    def db_insert_table_records(self, column):
        self.db_insert_table(model_soundrecord.SQL_INSERT_TABLE_RECORDS, column.get_values())

    def db_insert_table_notes(self, column):
        self.db_insert_table(model_notes.SQL_INSERT_TABLE_NOTES, column.get_values())

    def parse(self):
        self.db_create(self.cachedb)
        pr = ParserResults()
        file_nodes = self.node.Search('\..*$')
        has_media = 0
        media_models = []
        for self.file_node in file_nodes:
            filename = self.file_node.PathWithMountPoint
            if re.findall('privateUserInfo', filename):
                pr1 = ParserResults()
                prog1 = progress.GetBackgroundProgress("云勘华为备份", DescripCategories.HuaweiCloudBackup)
                prog1.Start()
                account_models = self.parse_user(filename)
                pr1.Models.Add(account_models)
                pr1.Categories = DescripCategories.HuaweiCloudBackup
                pr1.Build('云勘华为备份')
                ds.Add(pr1)
                prog1.Finish(True)
            elif re.findall('simpleContacts', filename):
                pr2 = ParserResults()
                prog2 = progress.GetBackgroundProgress("云勘华为备份-联系人", DescripCategories.Contacts)
                prog2.Start()
                self.parse_contact(filename)
                contact_models = model_contact.Generate(self.cachedb).get_models()
                pr2.Models.AddRange(contact_models)
                pr2.Categories = DescripCategories.Contacts
                pr2.Build('云勘华为备份')
                ds.Add(pr2)
                prog2.Finish(True)
            elif re.findall('defaultRecordingDevice.json', filename):
                pr3 = ParserResults()
                prog3 = progress.GetBackgroundProgress("云勘华为备份-录音", DescripCategories.Recordings)
                prog3.Start()
                self.parse_recording(filename)
                record_models = model_soundrecord.Generate(self.cachedb).get_models()
                pr3.Models.AddRange(record_models)
                pr3.Categories = DescripCategories.Recordings
                pr3.Build('云勘华为备份')
                ds.Add(pr3)
                prog3.Finish(True)
            elif re.findall('simplenote', filename):
                pr4 = ParserResults()
                prog4 = progress.GetBackgroundProgress("云勘华为备份-备忘录", DescripCategories.Notes)
                prog4.Start()
                self.parse_note(filename)
                note_models = model_notes.Generate(self.cachedb).get_models()
                pr4.Models.AddRange(note_models)
                pr4.Categories = DescripCategories.Notes
                pr4.Build('云勘华为备份')
                ds.Add(pr4)
                prog4.Finish(True)
            video_suffix = dic2['video']
            image_suffix = dic2['image']
            try:
                if filename.endswith(tuple(image_suffix)):
                    if has_media == 0:
                        prog5 = progress.GetBackgroundProgress("云勘华为备份-多媒体", DescripCategories.Photos)
                        prog5.Start()
                        has_media = 1
                    ret = model_media.Generate(None, None).get_exif_data(filename)
                    media_model = model_media.Generate(None, None).get_exif_info(ret, self.file_node)
                    if media_model is not None:
                        media_models.extend(media_model)
                    else:
                        media_model = model_media.Generate(None, None).get_normal_image_info(self.file_node)
                        media_models.append(media_model)
                elif filename.endswith(tuple(video_suffix)):
                    if has_media == 0:
                        prog5 = progress.GetBackgroundProgress("云勘华为备份-多媒体", DescripCategories.Photos)
                        prog5.Start()
                        has_media = 1
                    media_models.append(model_media.Generate(None, None).get_video_info(self.file_node))
            except:
                pass
        if len(media_models) != 0:
            pr5 = ParserResults()
            pr5.Models.AddRange(media_models)
            pr5.Categories = DescripCategories.Photos
            pr5.Build('云勘华为备份')
            ds.Add(pr5)
            prog5.Finish(True)
        return pr

    def parse_user(self, file):
        f = open(file, 'r')
        lines = f.readlines()
        f.close()
        account = Account()
        user = Common.User()
        account.photo = self.file_node.Parent.AbsolutePath + '/headPhoto.png'
        if account.photo not in [None, '']:
            user.PhotoUris.Add(self._get_uri(account.photo))
        for line in lines:
            if re.findall('^华为号：(.*)', line):
                account.account_id = re.findall('^华为号： (.*)', line)[0]
                user.ID.Value = account.account_id
            elif re.findall('^昵称：(.*)', line):
                account.username = re.findall('^昵称： (.*)', line)[0]
                user.Name.Value = account.username
                user.Username.Value = account.username
            elif re.findall('^地区：(.*)', line):
                account.address = re.findall('^地区：(.*)', line)[0]
                address = Contacts.StreetAddress()
                address.FullName.Value = account.address
                user.Addresses.Add(address)
        account.source = self.file_node.AbsolutePath
        user.SourceFile.Value = account.source
        self.db_insert_table_account(account)
        return user

    def _get_uri(self, path):
        if path.startswith('http') or len(path) == 0:
            return ConvertHelper.ToUri(path)
        else:
            return ConvertHelper.ToUri(path)

    def parse_contact(self, file):
        f = open(file, 'r')
        lines = f.readlines()
        f.close()
        data = ''.join(lines).strip().replace('\n', '')
        try:
            data = json.loads(data)
            if "Rbeans" in data:
                contacts = data["Rbeans"]
                for contact in contacts:
                    c = model_contact.Contact()
                    contactId = contact["contactId"]
                    name_info = contact["name"]
                    first_name = name_info["firstName"]
                    last_name = name_info["lastName"]
                    middle_name = name_info["middleName"]
                    name = first_name + middle_name + last_name
                    c.raw_contact_id = contactId
                    c.name = name
                    c.source = self.file_node.AbsolutePath
                    self.db_insert_table_call_contacts(c)
                self.db_commit()
        except:
            pass

    def parse_recording(self, file):
        f = open(file, 'r')
        lines = f.readlines()
        f.close()
        data = ''.join(lines).strip().replace('\n', '')
        try:
            data = json.loads(data)
            if "fileList" in data:
                fileList = data["fileList"]
                for file in fileList:
                    record = model_soundrecord.Records()
                    create_time = file["createTime"]
                    modify_time = file["modifyTime"]
                    name = file["name"]
                    size = file["size"]
                    record.record_create = self.time2timestamp(create_time)
                    record.record_size = size
                    record.record_name = os.path.basename(name)
                    if record.record_name is not None:
                        record_nodes = self.node.Search(record.record_name + '$')
                        if len(list(record_nodes)) != 0:
                            record.record_url = record_nodes[0].AbsolutePath
                    record.source = self.file_node.AbsolutePath
                    self.db_insert_table_records(record)
                self.db_commit()
        except:
            traceback.print_exc()

    def parse_note(self, file):
        f = open(file, 'r')
        lines = f.readlines()
        f.close()
        data = ''.join(lines)
        try:
            data = json.loads(data)
            if "rspInfo" in data:
                respInfo = data["rspInfo"]
                for info in respInfo:
                    note = model_notes.Notes()
                    dat = info["data"]
                    dat = json.loads(dat)
                    has_attachment = self._verify_dict(dat, "has_attachment")
                    first_attachment = self._verify_dict(dat, "first_attachment_name")
                    modify_date = self._verify_dict(dat, "modified")
                    title = self._verify_dict(dat, "title")
                    note.has_attachment = has_attachment
                    note.title = title
                    note.attach_name = first_attachment
                    if first_attachment is not None:
                        attach_url = self.node.Search(first_attachment + '$')
                        if len(list(attach_url)) != 0:
                            note.attach_url = attach_url[0].AbsolutePath
                    note.modified = self.time2timestamp(modify_date)
                    note.source = self.file_node.AbsolutePath
                    self.db_insert_table_notes(note)
                self.db_commit()
        except:
            traceback.print_exc()



    @staticmethod
    def _verify_dict(dic, string, default_value = None):
        return dic[string] if string in dic else default_value

    @staticmethod
    def time2timestamp(tss1):
        try:
            timeArray = time.strptime(tss1, "%Y-%m-%d %H:%M:%S")
            timestamp = int(str(time.mktime(timeArray))[0:10:1])
        except:
            return 0

class Account(object):
    def __init__(self):
        super(Account, self).__init__()
        self.account_id  = None  # 账户ID[TEXT]
        self.username    = None  # 用户名[TEXT]
        self.password    = None  # 密码[TEXT]
        self.photo       = None  # 头像[TEXT]
        self.gender      = 0  # 性别[INT]
        self.address     = None  # 地址[TEXT]
        self.birthday    = None  # 生日[INT]
        self.source      = ''
        self.deleted     = 0
        self.repeated    = 0

    def get_values(self):
        return (self.account_id, self.username, self.password,
                self.photo, self.gender, self.address, self.birthday,
                self.source, self.deleted, self.repeated)

def analyze_cloud_huawei(node, extractDeleted, extractSource):
    if node is not None:
        pr = HuaweiParser(node, extractDeleted, extractSource).parse()
        return pr

def execute(node, extractDeleted):
    return analyze_cloud_huawei(node, extractDeleted, False)