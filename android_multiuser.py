#coding:utf-8

__author__ = "Xu Tao"
__date__ = "2019-2-25"
__maintainer__ = 'Xu Tao'

import json

import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
del clr

from PA_runtime import *
from System.Xml.Linq import *
from PA.InfraLib.ModelsV2.Sys import User
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions


def analyze_multi_user(node, extract_deleted, extract_source):
    pr = ParserResults()
    cnode = node.Children
    if len(cnode) == 0:
        return
    results = []
    for unode in cnode:
        if unode.Type == NodeType.File:
            data = parse_xml(unode)
            if data:
                results.append(data)
    if results:
        pr.Models.AddRange(results)
        pr.Build("系统账户")
    return pr


def parse_xml(node):
    try:
        dicts = {}
        data = XElement.Load(node.Data)
        if data is None:
            return
        if str(data.Name) == "user":
            model = User()
            aid = data.Attribute("id").Value if data.Attribute("id") else None
            if aid:
                model.Id = aid
            created_time = data.Attribute("created").Value if data.Attribute("created") else None
            if created_time:
                model.CreateTime = convert_to_timestamp(created_time)
            lastlogin_time = data.Attribute("lastLoggedIn").Value if data.Attribute("lastLoggedIn") else None
            if lastlogin_time:
                model.LastLoginTime = convert_to_timestamp(lastlogin_time)
            if data.Element("name"):
                name = data.Element("name").Value
                model.NickName = name
            return model
    except Exception as e:
        return None

        
def convert_to_timestamp(timestamp):
    if len(str(timestamp)) == 13:
        timestamp = int(str(timestamp)[0:10])
    elif len(str(timestamp)) != 13 and len(str(timestamp)) != 10:
        timestamp = 0
    elif len(str(timestamp)) == 10:
        timestamp = timestamp
    ts = TimeStamp.FromUnixTime(timestamp, False)
    if not ts.IsValidForSmartphone():
        ts = None
    return ts



def analyze_app_authtokens(node, extract_deleted, extract_source):
    pr = ParserResults()
    dir_list = node.Children
    if len(dir_list) == 0:
        return
    results = []
    for unode in dir_list:
        if unode.Type == NodeType.Directory:
            auth_node = unode.GetByPath("accounts_ce.db")
            if auth_node is None:
                continue
            db = SQLiteParser.Database.FromNode(auth_node)
            if "authtokens" not in db.Tables:
                continue
            tbs = SQLiteParser.TableSignature("authtokens")
            for rec in db.ReadTableRecords(tbs, extract_deleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        return
                    model = AuthToeknsOfAndroid()
                    key_value = KeyValueModel()
                    key_value.Deleted = rec.Deleted

                    if "type" in rec and (not rec["type"].IsDBNull):
                        key_value.Key.Value = rec["type"].Value
                    if "authtoken" in rec and (not rec["authtoken"].IsDBNull):
                        key_value.Value.Value = rec["authtoken"].Value
                    if key_value:
                        model.Tokens.Add(key_value)
                        results.append(model)    
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error,e)
    if results:
        pr.Models.AddRange(results)
        pr.Build("密码")
    return pr