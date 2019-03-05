#coding:utf-8

__author__ = "Xu Tao"

import clr
try:
    clr.AddReference("model_wacai")
    clr.AddReference("MapUtil")
except:
    pass

from PA_runtime import *

import json
import model_wacai
from MapUtil import md5

VERSION_APP_VALUE = 1


class WaCaiTally(object):

    def __init__(self, node, extract_deleted, extract_source):
        self.root = node.Parent.Parent.Parent
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.wacai = model_wacai.WACAI()
        self.cache = ds.OpenCachePath("挖财记账")

    def parse(self):
        if self.root is None:
            return
        db_path = md5(self.cache, self.root.AbsolutePath)
        if self.wacai.need_parse(db_path, VERSION_APP_VALUE):
            self.wacai.db_create(db_path)
            group_node = self.root.GetByPath("/Documents/GroupTallySDK")
            self.get_group_tally(group_node)
            family_node = self.root.GetByPath("/Documents/FamilyTallySDK")
            self.get_family_data(family_node)

        if not canceller.IsCancellationRequested:
            self.wacai.db_insert_table_version(model_wacai.VERSION_KEY_DB, model_wacai.VERSION_VALUE_DB)
            self.wacai.db_insert_table_version(model_wacai.VERSION_KEY_APP, VERSION_APP_VALUE)
        
        if self.wacai.db is not None:
            self.wacai.db_commit()
            self.wacai.db_close()
        
        models = model_wacai.ExportModel(db_path).get_model()
        return models

    def get_group_tally(self, node):
        if node is None:
            return
        self.get_grounp_tally_books(node)
        self.get_group_member(node)
        self.get_group_bill(node)

    def get_family_data(self, node):
        self.get_family_tally(node)
        self.get_family_member(node)
        self.get_family_bill(node)
        self.get_family_monthly_bill(node)

    def get_grounp_tally_books(self, node):
        db_node = node.GetByPath("grouptally.db")
        if db_node is None:
            return                   
        db = SQLiteParser.Database.FromNode(db_node)
        if "TBL_GROUP" not in db.Tables:
            return
        tbs = SQLiteParser.TableSignature("TBL_GROUP")
        for rec in db.ReadTableRecords(tbs, self.extract_deleted, True):
            try:
                if canceller.IsCancellationRequested:
                    return
                tid = self._get_table_record_value(rec, "id")  # int
                tname = self._get_table_record_value(rec, "name")
                ttype = self._get_table_record_value(rec, "type")

                tally_book = model_wacai.Tally()
                tally_book.source = db_node.AbsolutePath
                if rec.Deleted == DeletedState.Deleted:
                    tally_book.deleted = 1
                tally_book.bookId = str(tid)
                tally_book.name = tname
                tally_book.tallyType = 2

                if tally_book.bookId:
                    self.wacai.db_insert_table_tally(tally_book)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error,"{0}".format(e))
        self.wacai.db_commit()
            
    def get_group_member(self, node):
        db_node = node.GetByPath("grouptally.db")
        if db_node is None:
            return
        db = SQLiteParser.Database.FromNode(db_node)
        if "TBL_GROUP_MEMBER" not in db.Tables:
            return
        tbs = SQLiteParser.TableSignature("TBL_GROUP_MEMBER")
        for rec in db.ReadTableRecords(tbs, self.extract_deleted, True):
            try:
                if canceller.IsCancellationRequested:
                    return
                bookid = self._get_table_record_value(rec, "bookid")
                mid = self._get_table_record_value(rec, "mid")
                name = self._get_table_record_value(rec, "name")
                avatar = self._get_table_record_value(rec, "avatar")

                member = model_wacai.Member()
                member.source = db_node.AbsolutePath
                if rec.Deleted == DeletedState.Deleted:
                    member.deleted = 1
                member.bookId = str(bookid)
                member.memberId = str(mid)
                member.avatar = avatar
                if member.bookId and member.memberId:
                    self.wacai.db_insert_table_member(member)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error,"{0}".format(e))
        self.wacai.db_commit()
          
    def get_group_bill(self, node):
        db_node = node.GetByPath("grouptally.db")
        if db_node is None:
            return
        db = SQLiteParser.Database.FromNode(db_node)
        if "TBL_GROUP_BILL" not in db.Tables:
            return
        tbs = SQLiteParser.TableSignature("TBL_GROUP_BILL")
        for rec in db.ReadTableRecords(tbs, self.extract_deleted, True):
            try:
                if canceller.IsCancellationRequested:
                    return
                bill_record = model_wacai.BillRecord()
                bill_record.source = db_node.AbsolutePath
                if rec.Deleted == DeletedState.Deleted:
                    bill_record.deleted = 1
                tid = self._get_table_record_value(rec, "bookid")  # int
                bill_record.bookId = str(tid)
                ttype = self._get_table_record_value(rec, "type")
                create_time = self._get_table_record_value(rec, "billtime")
                bill_record.createTime = create_time
                rec_data = self._get_table_record_value(rec, "data")
                if rec_data:
                    try:
                        bill_data = json.loads(rec_data)
                        if "data" in bill_data:
                            data = bill_data["data"]
                            if "subcategoryName" in data:
                                name = data["subcategoryName"]
                                bill_record.name = name
                            if "amount" in data:
                                amount = data["amount"]
                                bill_record.amount = amount
                            if "type" in data:
                                bill_type = data["type"]  # COST PRE
                                if bill_type == "COST":
                                    bill_record.bookType = 1
                                elif bill_type == "PRE":
                                    bill_record.bookType = 2
                            if "tip" in data:
                                tip = data["tip"]
                                bill_record.tip = tip
                            if "remark" in data:
                                remark = data["remark"]
                                bill_record.remark = remark
                    except Exception as e:
                        TraceService.Trace(TraceLevel.Info,"{0}".format(e))
                if bill_record.bookId:
                    self.wacai.db_insert_table_record(bill_record)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error,"{0}".format(e))
        self.wacai.db_commit()

    '''
    这个表没有id根据匹配，根据名称匹配怕会造成数据混淆，先取消
    def get_group_monthly_bill(self, node):
        db_node = node.GetByPath("grouptally.db")
        if db_node is None:
            return
        db = SQLiteParser.Database.FromNode(db_node)
        if "TBL_GROUP_SPENT" in db.Tables:
            return
        tbs = SQLiteParser.TableSignature("TBL_GROUP_SPENT")
        for rec in db.ReadTableRecords(tbs, self.extract_deleted, True):
            try:
                if canceller.IsCancellationRequested:
                    return
                month = self._get_table_record_value(rec, "month")
                
            except Exception as e:
                TraceService.Trace(TraceLevel.Error,"{0}".format(e))
    '''
    def get_family_tally(self, node):
        fa_node = node.GetByPath("MultiPeopleTally.db")
        if fa_node is None:
            return
        db = SQLiteParser.Database.FromNode(fa_node)
        if "TBL_BOOK" not in db.Tables:
            return
        tbs = SQLiteParser.TableSignature("TBL_BOOK")
        for rec in db.ReadTableRecords(tbs, self.extract_deleted, True):
            try:
                if canceller.IsCancellationRequested:
                    return
                tally = model_wacai.Tally()
                tally.source = fa_node.AbsolutePath
                if rec.Deleted == DeletedState.Deleted:
                    rec.deleted = 1
                fid = self._get_table_record_value(rec, "id")
                tally.bookId = str(fid)
                tally.tallyType = 1
                data = self._get_table_record_value(rec, "data")
                if data:
                    try:
                        json_tally = json.loads(data)
                        if "createdTime" in json_tally:
                            create_time = json_tally["createdTime"]
                            tally.createTime = create_time
                        if "updatedTime" in json_tally:
                            update_time = json_tally["updatedTime"]
                            tally.updatedTime = update_time
                        if "name" in json_tally:
                            t_name = json_tally["name"]
                            tally.name = t_name
                        if "memberCount" in json_tally:
                            m_count = json_tally["memberCount"]
                            tally.memberCount = m_count
                        if "currencyFlag" in json_tally:
                            money_flag = json_tally["currencyFlag"]
                            tally.moneyFlag = money_flag
                    except Exception as e:
                        TraceService.Trace(TraceLevel.Info,"{0}".format(e))
                        continue
                if tally.bookId:
                    self.wacai.db_insert_table_tally(tally)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error,"{0}".format(e))
        self.wacai.db_commit()

    def get_family_member(self, node):
        bill_node = node.GetByPath("MultiPeopleTallyRN.db")
        if bill_node is None:
            return
        db = SQLiteParser.Database.FromNode(bill_node)
        if "TBL_MEMBER" not in db.Tables:
            return
        tbs = SQLiteParser.TableSignature("TBL_MEMBER")
        for rec in db.ReadTableRecords(tbs, self.extract_deleted, True):
            try:
                if canceller.IsCancellationRequested:
                    return
                member = model_wacai.Member()
                member.source = bill_node.AbsolutePath
                if rec.Deleted == DeletedState.Deleted:
                    member.deleted = 1
                bookid = self._get_table_record_value(rec, "bookid")
                member.bookId = str(bookid)
                mtype = self._get_table_record_value(rec, "type") # 成员类型 1 家庭公共 0 普通
                member.memberType = mtype
                isdelete = self._get_table_record_value(rec, "isdelete") # 0 未删除 1 删除
                data = self._get_table_record_value(rec, "data")
                if data:
                    try:
                        m_id = b_id = None
                        json_member = json.loads(data)
                        if "id" in json_member:
                            m_id = json_member["id"]
                            member.memberId = m_id
                        if "bookId" in json_member:
                            b_id = json_member["bookId"]
                        if "name" in json_member:
                            name = json_member["name"]
                            member.name = name
                        if "mobile" in json_member:
                            photo = json_member["mobile"]
                            member.phone = photo
                        if "avatar" in json_member:
                            avatar = json_member["avatar"]
                            member.avatar = avatar
                        if "createdTime" in json_member:
                            create_time = json_member["createdTime"]
                            member.createTime = create_time
                        if "updatedTime" in json_member:
                            update_time = json_member["updatedTime"]
                            member.updatedTime = update_time
                    except Exception as e:
                        TraceService.Trace(TraceLevel.Info,"{0}".format(e))
                        continue
                if member.bookId and member.memberId:
                    self.wacai.db_insert_table_member(member)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error,"{0}".format(e))
        self.wacai.db_commit()

    def get_family_bill(self, node):
        bill_node = node.GetByPath("MultiPeopleTallyRN.db")
        if bill_node is None:
            return
        db = SQLiteParser.Database.FromNode(bill_node)
        if "TBL_BILL" not in db.Tables:
            return
        tbs = SQLiteParser.TableSignature("TBL_BILL")
        for rec in db.ReadTableRecords(tbs, self.extract_deleted, True):
            try:
                if canceller.IsCancellationRequested:
                    return
                bill_record = model_wacai.BillRecord()
                bill_record.source = bill_node.AbsolutePath
                if rec.Deleted == DeletedState.Deleted:
                    bill_record.deleted = 1
                bookid = self._get_table_record_value(rec, "bookid")
                bill_record.bookId = str(bookid)
                mid = self._get_table_record_value(rec, "mid") # 成员类型 1 家庭公共 0 普通
                billtime = self._get_table_record_value(rec, "billtime") # 0 未删除 1 删除
                bill_record.createTime = billtime
                data = self._get_table_record_value(rec, "data")
                if data:
                    try:
                        record = json.loads(data)
                        if "comment" in record:
                            comment = record["comment"]
                            bill_record.remark = comment
                        if "createTime" in record:
                            create_time = record["createTime"]
                            bill_record.createTime = create_time
                        if "updatedTime" in record:
                            update_time = record["updatedTime"]
                            bill_record.updatedTime = update_time
                        if "latitude" in record and "longitude" in record:
                            lng = record["longitude"]
                            lat = record["latitude"]
                            if lng and lat:
                                bill_record.langitude = lng
                                bill_record.latitude = lat
                        if "amount" in record:
                            amount = float(record["amount"])
                            bill_record.amount = amount
                        if "recType" in record:
                            rectype = record["recType"]  # 1 支出 2 收入
                            if rectype == 1:
                                bill_record.bookType = 1
                            elif rectype == 2:
                                bill_record.bookType = 2
                        if "attachments" in record:
                            if record["attachments"]:
                                attach = record["attachments"]
                                try:
                                    attach_data = json.loads(attach)
                                    urls = []
                                    for res in attach_data:
                                        if "addr" in res:
                                            urls.append(res["addr"])
                                    if len(urls) != 0:
                                        bill_record.mediaPath = ','.join(str(u) for u in urls)
                                except:
                                    pass
                        if "members" in record:
                            if record["members"]:
                                mids = []
                                for item in record["members"]:
                                    try:
                                        if "memberId" in item:
                                            mids.append(item["memberId"])
                                    except:
                                        pass
                                if len(mids) != 0:
                                    bill_record.memberId = ','.join(str(u) for u in mids)
                    except Exception as e:
                        TraceService.Trace(TraceLevel.Info,"{0}".format(e))
                        continue
                if bill_record.bookId and bill_record.amount:
                    self.wacai.db_insert_table_record(bill_record)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error,"{0}".format(e))
        self.wacai.db_commit()

    def get_family_monthly_bill(self, node):
        bill_node = node.GetByPath("MultiPeopleTallyRN.db")
        if bill_node is None:
            return
        db = SQLiteParser.Database.FromNode(bill_node)
        if "TBL_TOTAL" not in db.Tables:
            return
        tbs = SQLiteParser.TableSignature("TBL_TOTAL")
        for rec in db.ReadTableRecords(tbs, self.extract_deleted, True):
            try:
                if canceller.IsCancellationRequested:
                    return
                monthbill = model_wacai.MonthBill()
                monthbill.source = bill_node.AbsolutePath
                if rec.Deleted == DeletedState.Deleted:
                    monthbill.deleted = 1
                bill_type = self._get_table_record_value(rec, "type")
                monthbill.bookType = bill_type
                book_id = self._get_table_record_value(rec, "bookid")
                monthbill.bookId = book_id
                book_time = self._get_table_record_value(rec, "time")
                monthbill.createTime = book_time
                data = self._get_table_record_value(rec, "data")
                if data:
                    try:
                        month_data = json.loads(data)
                        if "monthIncome" in month_data:
                            income = month_data["monthIncome"]
                            monthbill.income = self._get_actual_amount(income)
                        if "monthOutgo" in month_data:
                            outgo = month_data["monthOutgo"]
                            monthbill.outgo = self._get_actual_amount(outgo)
                        if "monthBalance" in month_data:
                            balance = month_data["monthBalance"]
                            monthbill.balance = self._get_actual_amount(balance)
                    except:
                        pass
                if monthbill.bookId and monthbill.createTime:
                    self.wacai.db_insert_table_bill(monthbill)
            except Exception as e:
               TraceService.Trace(TraceLevel.Error,"{0}".format(e))
               pass
        self.wacai.db_commit()

    def _get_table_record_value(self, rec, column):
        if column in rec and (not rec[column].IsDBNull):
            return rec[column].Value
        else:
            return None

    def _get_actual_amount(self, amount):
        try:
            if amount is not None:
                return float(amount) / 100
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "{0}".format(e))
            return 0


def analyze_wacaitally(node, extract_deleted, extract_source):
    pr = ParserResults()
    results = WaCaiTally(node, extract_deleted, extract_source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("挖财记账理财")
    return pr


def execute(node, extract_deleted):
    return analyze_wacaitally(node, extract_deleted, False)
