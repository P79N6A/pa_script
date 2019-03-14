#_*_ coding:utf-8 _*_
__author__ = 'xiaoyuge'

from PA_runtime import *
from PA.InfraLib.ModelsV2.SafeCloud import *

import re

def analyze_cloud_qqmail(node, extractDeleted, extractSource):
    if node is None:
        return
    model = DataDirectory()
    model.Path = node.AbsolutePath
    pr = ParserResults()
    pr.Models.Add(model)
    pr.Build('QQ邮箱（云勘）')
    return pr
    
def execute(node, extractDeleted):
    return analyze_cloud_qqmail(node, extractDeleted, False)