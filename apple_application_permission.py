#coding:utf-8

__author__ = "Xu Tao"

import PA_runtime
from PA_runtime import *
from collections import defaultdict


class AppPerm(object):

    def __init__(self, node, extractDeleted, extractSource):
        self.root = node
        self.extractDeleted = extractDeleted
        self.extractSource = extractSource

    
    def parse(self):
        if self.root is None:
            return
        db = SQLiteParser.Database.FromNode(self.root)
        if db is None:
            return
        if "access" not in db.Tables:
            return
        models = []
        dicts = defaultdict(list)
        tbs = SQLiteParser.TableSignature("access")
        for rec in db.ReadTableRecords(tbs, False):
            if canceller.IsCancellationRequested:
                return
            try:
                if "client" in rec and (not rec["client"].IsDBNull) and "service" in rec and (not rec["service"].IsDBNull):
                    dicts[rec["client"].Value].append(rec["service"].Value)
            except Exception as e:
                print(e)

        models.extend(self.get_models(dicts))

        return models

    def get_models(self, dicts):
        models = []
        for key in dicts:
            application = InstalledApplication()
            application.Identifier.Value = key
            for per in dicts[key]:
                try:
                    application.Permissions.Add(per)
                except Exception as e:
                    print(e)
            models.append(application)
        return models

    
def analyze_apple_perm(node, extractDeleted, extractSource):
    pr = ParserResults()
    results = AppPerm(node, extractDeleted, extractSource).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("应用列表")
    return pr