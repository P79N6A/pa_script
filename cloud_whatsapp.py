# coding:utf-8

from PA_runtime import *
from PA.InfraLib.ModelsV2.SafeCloud import *


def analyze_whatsapp(node, extract_deleted, extract_source):
    if node is None:
        return
    model = DataDirectory()
    model.Path = node.AbsolutePath
    pr = ParserResults()
    pr.Models.Add(model)
    return pr