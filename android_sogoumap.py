#coding=utf-8
import PA_runtime
from PA_runtime import *
import json
import os
import re
import time
import clr
try:
    clr.AddReference('model_map')
except:
    pass
del clr
import model_map

APPVERSION = "1.0"

class SogouMap(object):
    
    def __init__(self, node, extractDeleted, extractSource):
        self.root = node
        self.extractDeleted = extractDeleted
        self.extractSource = extractSource
        self.cache = ds.OpenCachePath("sogouMap")
        self.sogoudb = model_map.Map()

    def parse_search(self):
        try:
            db = SQLiteParser.Database.FromNode(self.root)
            if db is None:
                return
            tbs = SQLiteParser.TableSignature("history_result_table")
            if self.extractDeleted:
                SQLiteParser.Tools.AddSignatureToTable(tbs, "logicId", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(tbs, self.extractDeleted, True):
                if canceller.IsCancellationRequested:
                    return
                search = model_map.Search()
                search.source = "搜狗地图:"
                search.sourceApp = "搜狗地图"
                search.sourceFile = self.root.AbsolutePath
                try:
                    if "tm" in rec:
                        date = rec["tm"].Value
                        strtime = time.strptime(date, "%Y-%m-%d %H:%M:%S")
                        unixtime = time.mktime(strtime)
                        search.create_time = unixtime

                    if rec["type"].Value == 7:
                        moreinfo = rec["logicId"].Value
                        name, types, id, addr = re.split(",", moreinfo)
                        if rec.Deleted == DeletedState.Deleted:
                            search.deleted = 1
                        search.keyword = name
                        search.address = addr
                    elif rec["type"].Value == 101 or rec["type"].Value == 5:
                        data = rec["logicId"].Value
                        index = self.check_digit_index(data)
                        if index is None:
                            pass
                        else:
                            search.keyword = data[:index]
                            xhx_index =  self.trans_to_langlat(data)
                            if xhx_index != -1:
                                lang = data[index:xhx_index]
                                lat = data[(xhx_index+1):]
                                search.pos_x = float(lang)
                                search.pos_y = lat
                    try:
                        self.sogoudb.db_insert_table_search(search)
                    except Exception as e:
                        print(e)    
                except Exception as e:
                    print(e)
        except Exception as e:
            print(e)
        self.sogoudb.db_commit()

    def check_digit_index(self, strings):
        for i in strings:
            if i.isdigit():
                try:
                    return strings.index(i)
                except Exception as e:
                    print("not found digit")
                
    def trans_to_langlat(self, strings):
        return strings.find("_")

    def check_to_update(self, path_db, appversion):
        if os.path.exists(path_db) and path_db[-6:-3] == appversion:
            return False
        else:
            return True  

    def parse(self):
        db_path = self.cache + "/sogou_db_1.0.db"
        if self.check_to_update(db_path, APPVERSION):
            self.sogoudb.db_create(db_path)
            self.parse_search()
            self.sogoudb.db_close()
        
        generate = model_map.Genetate(db_path)
        tmpresult = generate.get_models()
        return tmpresult
        

def analyze_sogoumap(node, extractDeleted, extractSource):
    pr = ParserResults()
    results = SogouMap(node, extractDeleted, extractSource).parse()
    if results:
        for i in results:
            pr.Models.Add(i)
    pr.Build("搜狗地图")
    return pr

def execute(node, extract_deleted):
    return analyze_sogoumap(node, extract_deleted, False)
