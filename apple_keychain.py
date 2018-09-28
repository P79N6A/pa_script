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

import Mono
import os
import gc

# app Êý¾Ý¿â°æ±¾
VERSION_APP_VALUE = 1

def analyze_keychain(root, extract_deleted, extract_source):
    pr = ParserResults()

    models = YouXinParser(root, extract_deleted, extract_source).parse()

    mlm = ModelListMerger()

    pr.Models.AddRange(list(mlm.GetUnique(models)))

    pr.Build('KeyChain')

    gc.collect()
    return pr

def execute(node, extractDeleted):
    return analyze_keychain(node, extractDeleted, False)

class KeyChain():
    def __init__(self, node, extract_deleted, extract_source):
        
