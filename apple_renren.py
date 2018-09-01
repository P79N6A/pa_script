#coding=utf-8

import PA_runtime
import clr 
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions
from PA_runtime import *

import os
import sqlite3
import json
import model_im

# app 数据库版本
VERSION_APP_VALUE = 1

def analyze_renren(root, extract_deleted, extract_source):
    pr = ParserResults()

    models = RenRenParser(root, extract_deleted, extract_source).parse()

    mlm = ModelListMerger()

    pr.Models.AddRange(list(mlm.GetUnique(models)))
    return pr

class RenRenParser(model_im.IM):
    def __init__(self, node, extracted_deleted, extract_source):
        super(RenRenParser, self).__init__()
        self.extract_deleteds = False
        self.extract_source = extract_source
        self.root = node 
        self.app_name = 'RenRen'
        self.mount_dir = node.FileSystem.MountPoint
        self.cache_path = ds.OpenCachePath('RenRen')
        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)
        self.cache_db = os.path.join(self.cache_path, 'cache.db')

    def parse(self):
        if self.need_parse(self.cache_db, VERSION_APP_VALUE):
            self.db_create(self.cache_db)
            user_list = self.get_user_list()
            for user in user_list:
                self.contacts = {}
                self.user = user
                self.parse_user()
                self.user = None
                self.contacts = None
            self.db_insert_table_version(model_im.VERSION_KEY_DB, model_im.VERSION_VALUE_DB)
            self.db_insert_table_version(model_im.VERSION_KEY_APP, VERSION_APP_VALUE)
            self.db_commit()
            self.close()
        models  = self.get_models_from_cache_db()
        return models

    def get_models_from_cache_db(self):
        models = model_im.GenerateModel(self.cache_db, self.mount_dir).get_models()
        return models

    def get_user_list(self):
        user_list = []
        node = self.root.GetByPath('/Documents')
        if node is not None:
            for file in os.listdir(node.PathWithMountPoint):
                if file.isdigit():
                    user_list.append(file)
        return user_list

    def parse_user(self):
        self.get_user()
        self.get_contacts()
        self.get_chats()

    def get_user(self):
        if self.user is None:
            return False

        node = self.root.GetByPath('/Documents/' + self.user + '/userCaches/userProfileDict')
        if node is None:
            return

        root = None
        try:
            root = BPReader.GetTree(node)
        except:
            return

        account = model_im.Account()
        account.user_name = self.bpreader_node_get_value(root, 'user_name', '')
        account.photo = self.bpreader_node_get_value(root, 'head_url', '')
        self.db_insert_table_account(account)
        self.db_commit()
        return True

    def get_contacts(self):
        if self.user is None:
            return True

    def get_chats(self):
        if self.user is None:
            return True

    @staticmethod
    def bpreader_node_get_value(node, key, default_value = None):
        if key in node.Children and node.Children[key] is not None:
            return node.Children[key].Value
        return default_value
