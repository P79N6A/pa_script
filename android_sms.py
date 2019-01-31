# coding=utf-8

__author__ = 'YangLiyuan'

from PA_runtime import *
import clr
try:
    clr.AddReference('model_sms')
    clr.AddReference('bcp_basic')
    clr.AddReference('ScriptUtils')
except:
    pass
del clr

import sqlite3
import hashlib
import bcp_basic
import model_sms
from ScriptUtils import DEBUG, CASE_NAME, exc, tp, base_analyze, BaseParser, parse_decorator


SMS_TYPE_ALL    = 0
SMS_TYPE_INBOX  = 1
SMS_TYPE_SENT   = 2
SMS_TYPE_DRAFT  = 3
SMS_TYPE_OUTBOX = 4
SMS_TYPE_FAILED = 5
SMS_TYPE_QUEUED = 6

VERSION_APP_VALUE = 2


@parse_decorator
def analyze_sms(node, extract_deleted, extract_source):
    """
        node: sms/sms.db$
        android 小米 短信 (user_de/0/com.android.providers.telephony/databases$ - mmssms.db)
    """    
    node_path = node.AbsolutePath
    tp(node_path)
    tp(node.Children)
    _Parser = None

    if node_path.endswith('sms/sms.db'):
        _Parser = AndroidSMSParser_fs_logic
    elif node_path.endswith('com.android.providers.telephony/databases'):
        _Parser = AndroidSMSParser
    # else:
    #     # huawei
    #     node = node.FileSystem.GetByPath('sms.db')
    #     if node:
    #         _Parser = AutoBackupHuaweiParser
    return base_analyze(_Parser,
                        node,
                        bcp_basic.BASIC_SMS_INFORMATION,
                        VERSION_APP_VALUE,
                        bulid_name='短信',
                        db_name='AndroidSMS') if _Parser else None


class AndroidSMSParser(BaseParser):
    def __init__(self, node, db_name):
        super(AndroidSMSParser, self).__init__(node, db_name)
        self.VERSION_VALUE_DB = model_sms.VERSION_VALUE_DB
        self.root = node
        self.csm = model_sms.ModelSMS()
        self.Generate = model_sms.GenerateModel

        self.contacts = {}
        self.sim_phonenumber = {}

    def parse_main(self):

        self.pre_parse_calls()
        if self._read_db('mmssms.db'):
            if 'sim_cards' in self.cur_db.Tables:
                self.parse_sim_cards('sim_cards')
            self.parse_sms('sms')
            # self.parse_mms()
        
    def parse_sim_cards(self, table_name):
        """ 
            sms - 短信
        """
        for rec in self._read_table(table_name):
            try:
                if self._is_empty(rec, 'number'):
                    continue
                sim = model_sms.SimCard()
                sim.sim_id       = rec['sim_id'].Value
                sim.number       = rec['number'].Value
                sim.sync_enabled = rec['sync_enabled'].Value
                sim.source       = self.cur_db_source
                sim.deleted      = 1 if rec.IsDeleted else 0  
                self.sim_phonenumber[sim.sim_id] = sim.number
                self.csm.db_insert_table_sim_cards(sim)
            except:
                exc()
        self.csm.db_commit()

    def pre_parse_calls(self):
        ''' calls.db - contacts

            RecNo	FieldName	
            0	raw_contact_id	INTEGER
            1	mimetype_id	INTEGER
            2	mail	TEXT
            3	company	TEXT
            4	title	TEXT
            5	last_time_contact	INTEGER
            6	last_time_modify	INTEGER
            7	times_contacted	INTEGER
            8	phone_number	TEXT
            9	name	TEXT
            10	address	TEXT
            11	notes	TEXT
            12	telegram	TEXT
            13	head_pic	BLOB
            14	source	TEXT
            15	deleted	INTEGER
            16	repeated	INTEGER   
        '''
        contacts = {}
        # 关联 通讯录  CALLS/F2BB91E8E7436EAA944C378D44066A79.db
        BASE_DIR   = os.path.dirname(self.cachepath)
        # tp(BASE_DIR)
        calls_path = os.path.join(BASE_DIR, 'Contact')
        # tp(calls_path)
        try:
            if not os.listdir(calls_path):
                exc('####### android_sms.py: Contact 目录下没有 db')
                return 
            # tp('calls_path', calls_path)
            for f  in os.listdir(calls_path):
                if f.endswith('.db'):
                    calls_db_path = os.path.join(calls_path, f)
        except:
            exc('####### android_sms.py: db 不存在 #######')
            return 
        try:
            self.calls_db = sqlite3.connect(calls_db_path)
            cursor = self.calls_db.cursor()            
            cursor.execute(''' select * from contacts ''')
            for row in cursor:
                contacts[row[8]] = row[9]
            self.contacts = contacts
        except:
            exc('##### android_sms.py 关联通讯录失败 #######')
        finally:
            cursor.close()
            self.calls_db.close()            

    def parse_sms(self, table_name):
        """ sms - 短信
        
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
            (xiaomi: service_center)
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
        for rec in self._read_table(table_name):
            if (self._is_empty(rec, 'type', 'body') or
                self._is_duplicate(rec, '_id')):
                continue
            if rec['address'].Value and not self._is_num(rec['address'].Value):
                continue
            sms = model_sms.SMS()
            try: # 华为没有的字段
                sms.sim_id  = rec['sim_id'].Value
                sms.deleted = rec['deleted'].Value
                sms.smsc    = rec['service_center'].Value
            except:
                pass    
            sms._id            = rec['_id'].Value
            sms.read_status    = rec['read'].Value
            sms.type           = rec['type'].Value    # SMS_TYPE
            sms.subject        = rec['subject'].Value 
            sms.body           = rec['body'].Value
            sms.send_time      = rec['date_sent'].Value
            sms.delivered_date = rec['date'].Value
            # tp('sms type', sms.type)
            sms.is_sender   = 1 if sms.type in (SMS_TYPE_SENT, SMS_TYPE_OUTBOX, SMS_TYPE_DRAFT) else 0
            if sms.is_sender == 1:  # 发
                sms.sender_phonenumber = self.sim_phonenumber.get(sms.sim_id, None) if sms.sim_id else None
                sms.sender_name        = self._get_contacts(sms.sender_phonenumber)
                sms.recv_phonenumber   = rec['address'].Value
                sms.recv_name          = self._get_contacts(sms.recv_phonenumber)
            else:                   # 收
                sms.sender_phonenumber = rec['address'].Value
                sms.sender_name        = self._get_contacts(sms.sender_phonenumber)
                sms.recv_phonenumber   = self.sim_phonenumber.get(sms.sim_id, None) if sms.sim_id else None
                sms.recv_name          = self._get_contacts(sms.recv_phonenumber)

            sms.deleted = 1 if rec.IsDeleted or sms.deleted else 0         
            sms.source = self.cur_db_source
            try:
                self.csm.db_insert_table_sms(sms)
            except:
                exc()
        self.csm.db_commit()

    def parse_mms(self):
        """ 
            pdu - 彩信
        """
        for rec in self._read_table(table_name='pdu'):
            try:
                if IsDBNull(rec['address'].Value) or IsDBNull(rec['body'].Value):
                    continue
                mms = model_sms.SMS()
                mms.sender_phonenumber = rec['address'].Value
                mms.mms_id             = rec['_id'].Value
                mms.subject            = rec['subject'].Value # decode or what
                mms.body               = rec['body'].Value
                mms.send_time          = rec['date'].Value
                mms.deliverd           = rec['date'].Value
                mms.status             = rec['type'].Value    # SMS_TYPE
                mms.is_sender          = 1 if mms.type in (SMS_TYPE_SENT, SMS_TYPE_OUTBOX) else 0
                mms.deleted = 1 if rec.IsDeleted else rec['deleted'].Value
                mms.source             = self.cur_db_source
                # self.csm.db_insert_table_mms(mms)
            except Exception as e:
                exc()
        
    def _get_contacts(self, sender_phonenumber):
        try:
            if isinstance(sender_phonenumber, str) and len(sender_phonenumber) == 11:
                for i in ('+86', '86'):
                    name = self.contacts.get(i + sender_phonenumber, None)
                    if name:
                        return name
            return self.contacts.get(sender_phonenumber, None)
        except:
            exc()
            return None

    @staticmethod
    def _is_num(address):
        try:
            if isinstance(int(address), (int, long, Int64)):
                return True
            return False
        except:
            return False


class AndroidSMSParser_fs_logic(AndroidSMSParser):
    ''' 处理逻辑提取案例, 非 tar 包, sms/sms.db$ '''
    def __init__(self, node, db_name):
        super(AndroidSMSParser_fs_logic, self).__init__(node, db_name)

    def parse_main(self):
        ''' sms/sms.db '''
        self.pre_parse_calls()

        if self._read_db(node=self.root):
            self.parse_sms('SMS') 

    def parse_sms(self, table_name):
        """ sms/sms.db - SMS

        RecNo	FieldName	SQLType
        1	phoneNumber	        TEXT
        2	time	            TEXT
        3	name	            TEXT
        4	shortType	        INTEGER
        5	isMms	            INTEGER
        6	theme	            TEXT
        7	shortRead	        INTEGER
        8	body	            TEXT
        9	path	            TEXT
        """
        for rec in self._read_table(table_name):
            try:
                if (self._is_empty(rec, 'body', 'phoneNumber') or 
                    rec['isMms'].Value==1):
                    continue
                sms = model_sms.SMS()
                # sms.sms_id             = rec['_id'].Value
                # sms.sender_phonenumber = rec['phoneNumber'].Value if rec['phoneNumber'].Value != 'insert-address-token' else None
                # sms.sender_name        = self.contacts.get(sms.sender_phonenumber, None)
                if rec['shortType'].Value == 1:
                    sms.type = SMS_TYPE_INBOX 
                elif rec['shortType'].Value == 2:
                    sms.type = SMS_TYPE_OUTBOX
                else:
                    sms.type = SMS_TYPE_ALL
                sms.read_status    = rec['shortRead'].Value
                sms.subject        = rec['theme'].Value
                sms.body           = rec['body'].Value.replace('\0', '')
                sms.send_time      = self._convert_2_timestamp(rec['time'].Value)
                sms.delivered_date = sms.send_time
                sms.is_sender      = 1 if sms.type == SMS_TYPE_OUTBOX else 0

                if sms.is_sender == 1:  # 发
                    sms.sender_phonenumber = self.sim_phonenumber.get(sms.sim_id, None) if sms.sim_id else None
                    sms.sender_name        = self._get_contacts(sms.sender_phonenumber)
                    sms.recv_phonenumber   = rec['phoneNumber'].Value if rec['phoneNumber'].Value != 'insert-address-token' else None
                    sms.recv_name          = self._get_contacts(sms.recv_phonenumber)
                else:                   # 收
                    sms.sender_phonenumber = rec['phoneNumber'].Value if rec['phoneNumber'].Value != 'insert-address-token' else None
                    sms.sender_name        = self._get_contacts(sms.sender_phonenumber)
                    sms.recv_phonenumber   = self.sim_phonenumber.get(sms.sim_id, None) if sms.sim_id else None
                    sms.recv_name          = self._get_contacts(sms.recv_phonenumber)

                sms.source  = self.cur_db_source
                sms.deleted = 1 if rec.IsDeleted else 0
                self.csm.db_insert_table_sms(sms)
            except:
                exc()
        self.csm.db_commit()

    @staticmethod
    def _convert_2_timestamp(format_time):
        ''' '2013-10-10 23:40:00' => 10位 时间戳
        '''
        try:
            ts = time.strptime(format_time, "%Y-%m-%d %H:%M:%S")
            return time.mktime(ts)
        except:
            exc()
            return 


class AutoBackupHuaweiParser(AndroidSMSParser):
    def __init__(self, node, db_name):
        super(AutoBackupHuaweiParser, self).__init__(node, db_name)

    def parse_main(self):
        ''' sms/sms.db '''
        self.pre_parse_calls()

        if self._read_db(node=self.root):
            self.parse_sms('sms_tb') 

    def parse_sms(self, table_name):
        """ /sms.db - SMS

            FieldName	        SQLType	             	
            date	                INTEGER
            address	                TEXT
            read	                INTEGER
            date_sent	            INTEGER
            subject	                TEXT
            sub_id	                INTEGER
            reply_path_present	    INTEGER
            type	                INTEGER
            body	                TEXT
            see n	                INTEGER
            thread_id	            INTEGER
            protocol	            INTEGER
            time_body	            TEXT
            addr_body	            TEXT
            group_id	            INTEGER
            service_center	        TEXT
            error_code	            INTEGER
            locked	                INTEGER
            network_type	        INTEGER
            status	                INTEGER
        """
        for rec in self._read_table(table_name):
            if self._is_empty(rec, 'type', 'body'):
                continue
            if rec['address'].Value and not self._is_num(rec['address'].Value):
                continue
            sms = model_sms.SMS()
            sms.smsc           = rec['service_center'].Value
            sms._id            = rec['_id'].Value
            sms.read_status    = rec['read'].Value
            sms.type           = rec['type'].Value    # SMS_TYPE
            sms.subject        = rec['subject'].Value 
            sms.body           = rec['body'].Value
            sms.send_time      = rec['date_sent'].Value
            sms.delivered_date = rec['date'].Value
            sms.is_sender   = 1 if sms.type in (SMS_TYPE_SENT, SMS_TYPE_OUTBOX, SMS_TYPE_DRAFT) else 0
            if sms.is_sender == 1:  # 发
                sms.sender_phonenumber = self.sim_phonenumber.get(sms.sim_id, None) if sms.sim_id else None
                sms.sender_name        = self._get_contacts(sms.sender_phonenumber)
                sms.recv_phonenumber   = rec['address'].Value
                sms.recv_name          = self._get_contacts(sms.recv_phonenumber)
            else:                   # 收
                sms.sender_phonenumber = rec['address'].Value
                sms.sender_name        = self._get_contacts(sms.sender_phonenumber)
                sms.recv_phonenumber   = self.sim_phonenumber.get(sms.sim_id, None) if sms.sim_id else None
                sms.recv_name          = self._get_contacts(sms.recv_phonenumber)

            sms.deleted = 1 if rec.IsDeleted or sms.deleted else 0         
            sms.source = self.cur_db_source
            try:
                self.csm.db_insert_table_sms(sms)
            except:
                exc()
        self.csm.db_commit()
