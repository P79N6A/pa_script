# coding=utf-8
import os
import re

import PA_runtime
from PA_runtime import *
import clr
try:
    clr.AddReference('model_sms')
except:
    pass
del clr
from model_sms import *

SMS_TYPE_ALL    = 0
SMS_TYPE_INBOX  = 1
SMS_TYPE_SENT   = 2
SMS_TYPE_DRAFT  = 3
SMS_TYPE_OUTBOX = 4
SMS_TYPE_FAILED = 5
SMS_TYPE_QUEUED = 6


def execute(node, extract_deleted):
    """ main """
    return analyze_sms(node, extract_deleted, extract_source=False)

def analyze_sms(node, extract_deleted, extract_source):
    """
        android 小米 短信 (user_de/0/com.android.providers.telephony/databases/mmssms.db)
    """
    pr = ParserResults()
    res = SMSParser(node, extract_deleted, extract_source).parse()
    pr.Models.AddRange(res)
    return pr


class SMSParser(object):
    """  """
    def __init__(self, node, extract_deleted, extract_source):

        self.root = node
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source

        self.sms = Model_SMS()
        self.cachepath = ds.OpenCachePath("AndroidSMS")
        self.cachedb = self.cachepath + "\\AndroidSMS.db"
        self.sms.db_create(self.cachedb)
        self.sms.db_create_table()
        
        self.unread_msgs = []

    def parse(self):
        node = self.root.GetByPath("/mmssms.db")
        self.db = SQLiteParser.Database.FromNode(node)
        if self.db is None:
            return
        self.source_mmmssms_db = node.AbsolutePath

        self.parse_main()

        self.sms.db_close()
        models = GenerateModel(self.cachedb).get_models()
        return models

    def parse_main(self):
        """
        sms
            _id                INTEGER PRIMARY KEY,
            thread_id          INTEGER,
            address            TEXT,
            person             INTEGER, 联系人（模块）列表里的序号，陌生人为null
            date               INTEGER, 毫秒
            date_sent          INTEGER DEFAULT 0,
            read               INTEGER DEFAULT 0,
            status             INTEGER DEFAULT -1, -1 默认值， 0-complete ， 64-pending ， 128-failed
            type               INTEGER,
                                    ALL=0;INBOX=1;SENT=2;DRAFT=3;OUTBOX=4;FAILED=5;QUEUED=6;
            subject            TEXT,
            body               TEXT,
            seen               INTEGER DEFAULT 0,
            timed              INTEGER DEFAULT 0,
            deleted            INTEGER DEFAULT 0,
            sync_state         INTEGER DEFAULT 0,
            marker             INTEGER DEFAULT 0,
            source             TEXT,
            bind_id            INTEGER DEFAULT 0,
            mx_status          INTEGER DEFAULT 0,
            mx_id              INTEGER,
            out_time           INTEGER DEFAULT 0,
            account            TEXT,
            sim_id             INTEGER DEFAULT 0,
            block_type         INTEGER DEFAULT 0,
            advanced_seen      INTEGER DEFAULT 0,
            b2c_ttl            INTEGER DEFAULT 0,
            b2c_numbers        TEXT,
            fake_cell_type     INTEGER DEFAULT 0,
            url_risky_type     INTEGER DEFAULT 0,
            creator            TEXT,
            favorite_date      INTEGER DEFAULT 0
        """
        self.parse_sim_cards()
        self.parse_sms()
        # self.parse_mms()
        
    def parse_sim_cards(self):
        """ 
            sms - 短信
        """
        try:
            for rec in self.my_read_table('sim_cards'):
                if canceller.IsCancellationRequested:
                    return
                if IsDBNull(rec['number'].Value):
                    continue
                sim = Sim_cards()
                sim.sim_id       = rec['sim_id'].Value
                sim.number       = rec['number'].Value
                sim.sync_enabled = rec['sync_enabled'].Value
                sim.source       = self.source_mmmssms_db
                try:
                    self.sms.db_insert_table_sim_cards(sim)
                except:
                    exc()
            try:
                self.sms.db_commit()
            except:
                pass
        except:
            pass

    def parse_sms(self):
        """ 
            sms - 短信
        """
        for rec in self.my_read_table(table_name='sms'):
            if canceller.IsCancellationRequested:
                return
            if IsDBNull(rec['body'].Value):
                continue
            sms = SMS()
            sms.sms_id             = rec['_id'].Value
            sms.sender_phonenumber = rec['address'].Value
            sms.sms_or_mms         = 'sms'
            sms.type               = rec['type'].Value    # SMS_TYPE
            sms.subject            = rec['subject'].Value # decode or what
            sms.body               = rec['body'].Value.replace('\0', '')
            sms.send_time          = rec['date_sent'].Value    # or date_sent?
            sms.deliverd           = rec['date'].Value
            sms.is_sender          = 1 if sms.type == SMS_TYPE_SENT else 0
            try: # 华为没有的字段
                sms.sim_id         = rec['sim_id'].Value
                sms.deleted        = rec['deleted'].Value
            except:
                pass    
            sms.source             = self.source_mmmssms_db
            try:
                self.sms.db_insert_table_sms(sms)
            except:
                exc()
        try:
            self.sms.db_commit()
        except:
            exc()

    def parse_mms(self):
        """ 
            pdu - 彩信
        """
        for rec in self.my_read_table(table_name='pdu'):
            if IsDBNull(rec['address'].Value) or IsDBNull(rec['body'].Value):
                continue
            sms = Message()
            sms.sms_or_mms         = 'sms'
            sms.sender_phonenumber = rec['address'].Value
            sms.sms_id             = rec['_id'].Value
            sms.subject            = rec['subject'].Value # decode or what
            sms.body               = rec['body'].Value
            sms.send_time          = self._long2int_timestamp(rec['date'].Value)
            sms.deliverd           = self._long2int_timestamp(rec['date'].Value)    
            sms.status             = rec['type'].Value    # SMS_TYPE
            sms.is_sender          = 1 if sms.status == SMS_TYPE_SENT else 0
            sms.deleted            = rec['deleted'].Value
            sms.source             = self.source_mmmssms_db
            try:
                self.sms.db_insert_table_sms(sms)
            except:
                exc()
        try:
            self.sms.db_commit()
        except:
            exc()            

    def my_read_table(self, table_name):
        """
            读取手机数据库, 单数据库模式
        :type table_name: str
        :rtype: db.ReadTableRecords()
        """
        if self.db is None:
            return
        tb = SQLiteParser.TableSignature(table_name)
        return self.db.ReadTableRecords(tb, self.extract_deleted, True)
