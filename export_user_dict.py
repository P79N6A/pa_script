# -*- coding: utf-8 -*-
__author__ = "TaoJianping"

import clr

clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')

try:
    clr.AddReference('ScriptUtils')
except Exception:
    pass

del clr

import PA_runtime
from ScriptUtils import ParserBase, DataModel, Fields, TimeHelper
import System
from PA_runtime import *
from System.Data.SQLite import *
from System.Xml.Linq import *
from PA.InfraLib.ModelsV2.Base import UserDictionary

# CONST
UserDict_VERSION = 1
DEBUG = False


class IOSWords(DataModel):
    __table__ = "ZTEXTREPLACEMENTENTRY"

    ts = Fields.FloatField(column_name="ZTIMESTAMP")
    words = Fields.CharField(column_name="ZPHRASE")
    key = Fields.CharField(column_name="ZSHORTCUT")


class AndroidWords(DataModel):
    __table__ = "words"

    words = Fields.CharField(column_name="word")
    key = Fields.CharField(column_name="shortcut")
    location = Fields.CharField(column_name="locale")


class IosUserDictParser(ParserBase):
    def __init__(self, root, extract_deleted, extract_source):
        super(IosUserDictParser, self).__init__(
            self._get_root_node(root, times=0),
            extract_deleted,
            extract_source,
            app_name="UserDict",
            app_version=UserDict_VERSION,
            debug=DEBUG,
        )



    def _main(self):
        """解析的逻辑主函数"""
        db = self.root
        IOSWords.connect(db)

        models = []
        for word in IOSWords.objects.all:
            d = UserDictionary()
            d.CreatetTime = TimeHelper.convert_timestamp_for_c_sharp(TimeHelper.convert_ts_for_ios(int(word.ts)))
            d.Phrase = word.words
            d.ShortCut = word.key
            models.append(d)

        return models

    def parse(self):
        """程序入口"""
        return self._main()


class AndroidUserDictParser(IosUserDictParser):

    def _main(self):
        """解析的逻辑主函数"""
        db = self.root
        AndroidWords.connect(db)

        models = []
        for word in AndroidWords.objects.all:
            d = UserDictionary()
            d.Locale = word.location
            d.Phrase = word.words
            d.ShortCut = word.key
            models.append(d)

        return models


def analyze_UserDict_for_android(root, extract_deleted, extract_source):
    pr = ParserResults()
    pr.Categories = DescripCategories.UserDictionary
    results = AndroidUserDictParser(root, extract_deleted, extract_source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("UserDict")
    return pr


def analyze_UserDict_for_ios(root, extract_deleted, extract_source):
    pr = ParserResults()
    pr.Categories = DescripCategories.UserDictionary
    results = IosUserDictParser(root, extract_deleted, extract_source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("UserDict")
    return pr
