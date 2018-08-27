#coding=utf-8
import os
import PA_runtime
from PA_runtime import *

def analyze_notes(node, extractDeleted, extractSource):  #分析note方法  extractDelete作用：判断数据库中数据是否被软删除，如果被软删除则extractDelete设置为True，之后读取软删除内容
    pr = ParserResults()  #建立pr对象，方便将读取的内容写入C#Model中
    db = SQLiteParser.Database.FromNode(node)  #连接指定结点的数据库
    if db is None:
        return
    ts = SQLiteParser.TableSignature('ZNOTEBODY')  #获取数据库中ZNOTEBODY表
    if extractDeleted:  #数据软删除之后被隐藏，新建字段保存
        ts['ZCONTENT'] = TextNotNull
        ts['Z_OPT'] = SQLiteParser.Signatures.NumericSet(1)

    body_dic = {}
    
    for record in db.ReadTableRecords(ts, extractDeleted):  #按行读取数据表ZNOTEBODY数据
        if IsDBNull(record['ZCONTENT'].Value):  #判断数据表中ZCONTENT字段内容是否为空，为空跳过
            continue
        
        if record.Deleted == DeletedState.Intact:  #筛选出未被删除的数据
            key = record['Z_PK'].Value  #读取数据中Z_PK字段的值
            if key not in body_dic:  #字典中不存在ZCONTENT的键则往字典中添加SQLiteParser对象
                body_dic[key] = record['ZCONTENT']
        else:  #提取被删除的数据放入Note对象中存入C#模型
            res = Note()
            res.Deleted = record.Deleted
            SQLiteParser.Tools.ReadColumnToField(record, "ZCONTENT", res.Body, extractSource)
            pr.Models.Add(res)
    
    ts = SQLiteParser.TableSignature('ZNOTE')  #获取数据库ZNOTE表
    if extractDeleted:  #数据软删除之后初始化表
        ts['ZTITLE'] = TextNotNull
        ts['ZBODY'] = IntNotNull
        ts['ZMODIFICATIONDATE'] = ts['ZCREATIONDATE'] = SQLiteParser.Signatures.NumericSet(4, 7)
        ts['Z_OPT'] = ts['ZCONTAINSCJK'] = ts['ZEXTERNALFLAGS'] = ts['ZDELETEDFLAG'] = SQLiteParser.Signatures.NumericSet(1)

    
    for record in db.ReadTableRecords(ts, extractDeleted, True):  #按行读取ZNOTE中数据
        if not record['ZBODY'].Value in body_dic:  #跳过空数据
            continue
        if record['ZCREATIONDATE'].Value == 0 or record['ZMODIFICATIONDATE'].Value == 0:  #跳过日期异常的数据
            continue

        res = Note()  
        res.Deleted = record.Deleted  #记录数据状态
        if not IsDBNull(record['ZTITLE'].Value):  #ZTITLE字段读取成功后将数据保存到Note对象中
            res.Title.Value = record['ZTITLE'].Value
            if extractSource:  #读取到位置之后将字段位置保存到Note对象中
                res.Title.Source = MemoryRange(record['ZTITLE'].Source)
        if not IsDBNull(record['ZSUMMARY'].Value):   #读取到ZSUMMARY字段数据后将数据保存到Note对象中     
            res.Summary.Value = record['ZSUMMARY'].Value
            if extractSource:  #保存字段位置
                res.Summary.Source = MemoryRange(record['ZSUMMARY'].Source)
        if not IsDBNull(body_dic[record['ZBODY'].Value].Value):
            res.Body.Value = body_dic[record['ZBODY'].Value].Value
            if extractSource:
                res.Body.Source = MemoryRange(body_dic[record['ZBODY'].Value].Source)
        if not IsDBNull(record['ZCREATIONDATE'].Value):
            try:
                res.Creation.Value = TimeStamp(epoch.AddSeconds(record['ZCREATIONDATE'].Value), True)  #时间戳？
                if extractSource:
                    res.Creation.Source = MemoryRange(record['ZCREATIONDATE'].Source)
            except:
                pass
        if not IsDBNull(record['ZMODIFICATIONDATE'].Value):
            try:
                res.Modification.Value = TimeStamp(epoch.AddSeconds(record['ZMODIFICATIONDATE'].Value), True)
                if extractSource:
                    res.Modification.Source = MemoryRange(record['ZMODIFICATIONDATE'].Source)
            except:
                pass
        pr.Models.Add(res)

    return pr

def analyze_old_notes(node, extractDeleted, extractSource):
    db = SQLiteParser.Database.FromNode(node)
    if db is None:
        return

    ts = SQLiteParser.TableSignature('note_bodies')
    if extractDeleted:
        ts['data'] = TextNotNull 

    body_dic = {}
    for record in db.ReadTableRecords(ts, extractDeleted):
        if IsDBNull(record['data'].Value):
            continue
        key = record['note_id'].Value        
        body_dic[key] = record['data']

    ts = SQLiteParser.TableSignature('Note')
    if extractDeleted:
        ts['summary'] = ts['title'] = TextNotNull
        ts['modification_date'] = ts['creation_date'] = SQLiteParser.Signatures.NumericSet(7)

    pr = ParserResults()

    for record in db.ReadTableRecords(ts, extractDeleted, True):
        if len(record) != 8:
            continue
        res = Note()
        res.Deleted = record.Deleted
        if not IsDBNull(record['title'].Value):
            res.Title.Value = record['title'].Value
            if extractSource:
                res.Title.Source = MemoryRange(record['title'].Source)
        if not IsDBNull(record['summary'].Value):
            res.Summary.Value = record['summary'].Value
            if extractSource:
                res.Summary.Source = MemoryRange(record['summary'].Source)
        if record['ROWID'].Value in body_dic:
            res.Body.Value = body_dic[record['ROWID'].Value].Value
            if extractSource:
                res.Body.Source = MemoryRange(body_dic[record['ROWID'].Value].Source)
        if not IsDBNull(record['creation_date'].Value) and record['creation_date'].Value > 0:
            res.Creation.Value = TimeStamp(epoch.AddSeconds(record['creation_date'].Value), True)
            if extractSource:
                res.Creation.Source = MemoryRange(record['creation_date'].Source)
        if not IsDBNull(record['modification_date'].Value) and record['modification_date'].Value > 0:
            res.Modification.Value = TimeStamp(epoch.AddSeconds(record['modification_date'].Value), True)
            if extractSource:
                res.Modification.Source = MemoryRange(record['modification_date'].Source)
        pr.Models.Add(res)

    return pr