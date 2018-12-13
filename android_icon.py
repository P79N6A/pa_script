#coding=utf-8

import PA_runtime
from PA_runtime import *
import os


class AppIcon(object):

    def __init__(self, node, extract_deleted, extract_source):
        self.root = node
        self.extract_deleted = False
        self.extract_source = extract_source

    
    def parse(self):
        models = []
        models.extend(self.get_data())
        print models
        return models

    def get_data(self):
        models = []
        if self.root is None:
            return
        for icon_file in self.root.Files:
            try:
                icon = KeyValueModel()
                name = os.path.splitext(icon_file.Name)[0]
                icon.Key.Value = name
                icon.Value.Value = icon_file.PathWithMountPoint
                models.append(icon)
            except Exception as e:
                pass
        return models


def analyze_icon(root, extract_deleted, extract_source):
    pr = ParserResults()

    models = AppIcon(root, extract_deleted, extract_source).parse()
    mlm = ModelListMerger()
    if models:
        pr.Models.AddRange(list(mlm.GetUnique(models)))
        pr.Build('AndroidIcon')
        pr.Categories = DescripCategories.AndroidIcon
    return pr