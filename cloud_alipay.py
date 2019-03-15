# coding=utf-8
from collections import OrderedDict

__author__ = 'YangLiyuan'

import clr

clr.AddReference('PNFA.Common')

from PA_runtime import *
from PA.InfraLib.ModelsV2.SafeCloud import *

try:
    clr.AddReference('ScriptUtils')
except:
    pass

from ScriptUtils import parse_decorator

@parse_decorator
def analyze_cloud_alipay(node, extract_deleted, extract_source):
    pr = ParserResults()
    dir = DataDirectory()
    dir.Path = node.AbsolutePath
    pr.Models.Add(dir)
    pr.Build('支付宝(云勘)')
    return pr

