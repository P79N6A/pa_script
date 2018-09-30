#coding=utf-8
import PA_runtime
from PA_runtime import *
import json
import os
import re
import time
import clr
import traceback
try:
    clr.AddReference('model_map')
    clr.AddReference("bcp_gis")
except:
    pass
del clr
import model_map
import bcp_gis


class SogouMap(object):
    
    def __init__(self, node, extractDeleted, extractSource):
        self.root = node
        self.extractDeleted = extractDeleted
        self.extractSource = extractSource
        self.cache = ds.OpenCachePath("sogouMap")
        self.sogoudb = model_map.Map()

    def parse_search(self):
        try:
            db = SQLiteParser.Database.FromNode(self.root, canceller)
            if db is None:
                return
            tbs = SQLiteParser.TableSignature("history_result_table")
            if self.extractDeleted:
                SQLiteParser.Tools.AddSignatureToTable(tbs, "logicId", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(tbs, self.extractDeleted, True):
                if canceller.IsCancellationRequested:
                    return
                search = model_map.Search()
                search.source = "搜狗地图"
                search.sourceApp = "搜狗地图"
                search.sourceFile = self.root.AbsolutePath
                try:
                    if "tm" in rec:
                        date = rec["tm"].Value
                        try:
                            strtime = time.strptime(date, "%Y-%m-%d %H:%M:%S")
                            unixtime = time.mktime(strtime)
                            search.create_time = unixtime
                        except Exception as e:
                            pass
                        # search.create_time = unixtime

                    if "type" in rec and rec["type"].Value == 7:
                        if "logicID" in rec:
                            moreinfo = rec["logicId"].Value
                            name, types, id, addr = re.split(",", moreinfo)[:3]
                            if rec.Deleted == DeletedState.Deleted:
                                search.deleted = 1
                            search.keyword = name
                            search.address = addr
                    elif "type" in rec and rec["type"].Value == 101 or rec["type"].Value == 5:
                        if "logicId" in rec:
                            data = rec["logicId"].Value
                            index = self.check_digit_index(data)
                            if index is None:
                                continue
                            else:
                                search.keyword = data[:index]
                                xhx_index =  self.trans_to_langlat(data)
                                if xhx_index != -1:
                                    lang = data[index:xhx_index]
                                    lat = data[(xhx_index+1):]
                                    search.pos_x = float(lang)
                                    search.pos_y = lat
                    try:
                        if search.keyword or search.item_type or search.address or search.pos_x or search.pos_y:
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


    def parse(self):
        db_path = model_map.md5(self.cache, self.root.AbsolutePath)
        self.sogoudb.db_create(db_path)
        self.parse_search()
        self.sogoudb.db_close()
        tmp_dir = ds.OpenCachePath("tmp")
        PA_runtime.save_cache_path(bcp_gis.NETWORK_APP_MAP_SOGOU, db_path, tmp_dir)
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
