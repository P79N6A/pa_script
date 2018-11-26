# coding=utf-8
__author__ = 'YangLiyuan'

import time
import clr
try:
    clr.AddReference('model_sms')
    clr.AddReference('bcp_basic')
except:
    pass
del clr
from model_sms import *
import bcp_basic


SMS_TYPE_ALL    = 0
SMS_TYPE_INBOX  = 1
SMS_TYPE_SENT   = 2
SMS_TYPE_DRAFT  = 3
SMS_TYPE_OUTBOX = 4
SMS_TYPE_FAILED = 5
SMS_TYPE_QUEUED = 6

VERSION_APP_VALUE = 1


def analyze_sms(node, extract_deleted, extract_source):
    """
        node: sms/sms.db$
        android 小米 短信 (user_de/0/com.android.providers.telephony/databases$ - mmssms.db)
    """
    # test_p node.AbsolutePath
    node_path = node.AbsolutePath

    res = []
    if node_path.endswith('sms/sms.db'):
        res = SMSParser_no_tar(node, extract_deleted, extract_source).parse()
    #elif node_path.endswith('user_de/0/com.android.providers.telephony/databases'):
    elif node_path.endswith('com.android.providers.telephony/databases'):
        res = SMSParser(node, extract_deleted, extract_source).parse()

    if DEBUG:
        if res:
            for sms in res:
                test_p('sms.Body.Value:', sms.Body.Value)
        else:
            test_p('sms res is null')

    pr = ParserResults()
    if res:
        pr.Models.AddRange(res)
        pr.Build('短信')
    return pr


class SMSParser(object):
    """  """
    def __init__(self, node, extract_deleted, extract_source):
        self.root = node
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source

        self.m_sms = Model_SMS()
        self.cachepath = ds.OpenCachePath("AndroidSMS")
        hash_str = hashlib.md5(node.AbsolutePath).hexdigest()
        self.cache_db = self.cachepath + '\\{}.db'.format(hash_str)

        self.contacts = {}
        self.sim_phonenumber = {}

    def parse(self):
        if DEBUG or self.m_sms.need_parse(self.cache_db, VERSION_APP_VALUE):
        #if self.m_sms.need_parse(self.cache_db, VERSION_APP_VALUE):
            node = self.root.GetByPath("mmssms.db")
            self.db = SQLiteParser.Database.FromNode(node, canceller)
            if self.db is None:
                return []
            
            self.m_sms.db_create(self.cache_db)
            self.source_mmmssms_db = node.AbsolutePath
            self.parse_main()

            # 数据库填充完毕，请将中间数据库版本和app数据库版本插入数据库，用来检测app是否需要重新解析
            if not canceller.IsCancellationRequested:
                self.m_sms.db_insert_table_version(VERSION_KEY_DB, VERSION_VALUE_DB)
                self.m_sms.db_insert_table_version(VERSION_KEY_APP, VERSION_APP_VALUE)
                self.m_sms.db_commit()
            self.m_sms.db_close() 

        tmp_dir = ds.OpenCachePath('tmp')
        save_cache_path(bcp_basic.BASIC_SMS_INFORMATION, self.cache_db, tmp_dir)

        models = GenerateModel(self.cache_db, self.cachepath).get_models()
        return models        

    def parse_main(self):

        self.pre_parse_calls()
        self.parse_sim_cards()
        self.parse_sms()
        # self.parse_mms()
        
    def parse_sim_cards(self):
        """ 
            sms - 短信
        """
        try:
            for rec in self._read_table('sim_cards'):
                if IsDBNull(rec['number'].Value):
                    continue
                sim = Sim_cards()
                sim.sim_id       = rec['sim_id'].Value
                sim.number       = rec['number'].Value
                sim.sync_enabled = rec['sync_enabled'].Value
                sim.source       = self.source_mmmssms_db
                sim.deleted      = 1 if rec.IsDeleted else 0  
                self.sim_phonenumber[sim.sim_id] = sim.number
                try:
                    self.m_sms.db_insert_table_sim_cards(sim)
                except:
                    exc()
            try:
                self.m_sms.db_commit()
            except:
                pass
        except:
            pass

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
        # test_p(BASE_DIR)
        calls_path = os.path.join(BASE_DIR, 'Contact')
        # test_p(calls_path)

        try:
            if not os.listdir(calls_path):
                exc('####### android_sms.py: Contact 目录下没有 db')
                return 
            # test_p('calls_path', calls_path)
            for f  in os.listdir(calls_path):
                if f.endswith('.db'):
                    calls_db_path = os.path.join(calls_path, f)
        except:
            exc('####### android_sms.py: db 不存在')
            # exc()
            return 

        try:
            self.calls_db = sqlite3.connect(calls_db_path)
            cursor = self.calls_db.cursor()            
            cursor.execute(''' select * from contacts ''')
            for row in cursor:
                contacts[row[8]] = row[9]
            self.contacts = contacts
            # test_p(contacts)
        except:
            exc('##### android_sms.py #######: 关联通讯录失败')
            # exc()
            # pass
        finally:
            cursor.close()
            self.calls_db.close()            

    def parse_sms(self):
        """ sms - 短信
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
        for rec in self._read_table(table_name='sms'):
            if canceller.IsCancellationRequested:
                return            
            if self._is_empty(rec, 'type', 'body'):
                continue
            if rec['address'].Value and not self._is_num(rec['address'].Value):
                continue
            sms = SMS()
            try: # 华为没有的字段
                sms.sim_id  = rec['sim_id'].Value
                sms.deleted = rec['deleted'].Value
                sms.smsc    = rec['service_center'].Value
            except:
                pass    
            sms._id         = rec['_id'].Value
            sms.read_status = rec['read'].Value
            sms.type        = rec['type'].Value    # SMS_TYPE

            sms.subject     = rec['subject'].Value 
            sms.body        = rec['body'].Value
            sms.send_time   = rec['date_sent'].Value
            sms.deliverd    = rec['date'].Value
            # test_p('sms type', sms.type)
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
            sms.source = self.source_mmmssms_db
            try:
                self.m_sms.db_insert_table_sms(sms)
            except:
                exc()
        try:
            self.m_sms.db_commit()
        except:
            exc()

    # TODO
    # def parse_mms(self):
    #     """ 
    #         pdu - 彩信
    #     """
    #     for rec in self._read_table(table_name='pdu'):
    #         if IsDBNull(rec['address'].Value) or IsDBNull(rec['body'].Value):
    #             continue
    #         sms = Message()
    #         sms.sender_phonenumber = rec['address'].Value
    #         sms.sms_id             = rec['_id'].Value
    #         sms.subject            = rec['subject'].Value # decode or what
    #         sms.body               = rec['body'].Value
    #         sms.send_time          = self._long2int_timestamp(rec['date'].Value)
    #         sms.deliverd           = self._long2int_timestamp(rec['date'].Value)    
    #         sms.status             = rec['type'].Value    # SMS_TYPE
    #         sms.is_sender          = 1 if sms.type in (SMS_TYPE_SENT, SMS_TYPE_OUTBOX) else 0
    #         sms.deleted = 1 if rec.IsDeleted else rec['deleted'].Value
    #         sms.source             = self.source_mmmssms_db
    #         try:
    #             self.m_sms.db_insert_table_sms(sms)
    #         except Exception as e:
    #             exc()
    #     try:
    #         self.m_sms.db_commit()
    #     except Exception as e:
    #         exc()            

    def _read_table(self, table_name):
        """
            读取手机数据库, 单数据库模式
        :type table_name: str
        :rtype: db.ReadTableRecords()
        """
        try:
            if self.db is None:
                return [] 
            tb = SQLiteParser.TableSignature(table_name)
            return self.db.ReadTableRecords(tb, self.extract_deleted, True)
        except:
            exc()
            return []

    def _get_contacts(self, sender_phonenumber):
        try:
            if len(sender_phonenumber) == 11:
                for i in ('+86', '86'):
                    name = self.contacts.get(i + sender_phonenumber, None)
                    if name:
                        return name
            return self.contacts.get(sender_phonenumber, None)
        except:
            return None

    @staticmethod
    def _is_empty(rec, *args):
        ''' 过滤 DBNull 空数据, 有一空值就跳过
        
        :type rec:   rec
        :type *args: str
        :rtype: bool
        '''
        try:
            for i in args:
                if IsDBNull(rec[i].Value) or rec[i].Value in ('', ' ', None, [], {}):
                    return True
            return False
        except:
            exc()
            return True  

    @staticmethod
    def _is_num(address):
        try:
            if isinstance(int(address), (int, long, Int64)):
                return True
            return False
        except:
            return False

class SMSParser_no_tar(SMSParser):
    ''' 处理逻辑提取案例, 非 tar 包, sms/sms.db$ '''

    def __init__(self, node, extract_deleted, extract_source):
        super(SMSParser_no_tar, self).__init__(node, extract_deleted, extract_source)
    
    def parse(self):
        if DEBUG:
            test_p('短信: 处理逻辑提取, sms node path:', self.root.AbsolutePath)
        if DEBUG or self.m_sms.need_parse(self.cache_db, VERSION_APP_VALUE):
            node = self.root
            self.db = SQLiteParser.Database.FromNode(node, canceller)
            if self.db is None:
                return []

            self.m_sms.db_create(self.cache_db)
            self.source_sms_db = node.AbsolutePath
            self.parse_sms()

            # 数据库填充完毕，请将中间数据库版本和app数据库版本插入数据库，用来检测app是否需要重新解析
            if not canceller.IsCancellationRequested:
                self.m_sms.db_insert_table_version(VERSION_KEY_DB, VERSION_VALUE_DB)
                self.m_sms.db_insert_table_version(VERSION_KEY_APP, VERSION_APP_VALUE)
                self.m_sms.db_commit()
            self.m_sms.db_close() 

        models = GenerateModel(self.cache_db, self.cachepath).get_models()
        return models   

    def parse_sms(self):
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
        for rec in self._read_table(table_name='SMS'):
            if canceller.IsCancellationRequested:
                return            
            if self._is_empty(rec, 'body', 'phoneNumber') or rec['isMms'].Value == 1:
                continue
            sms = SMS()
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

            sms.source  = self.source_sms_db
            sms.deleted = 1 if rec.IsDeleted else 0
            try:
                self.m_sms.db_insert_table_sms(sms)
            except:
                exc()
        try:
            self.m_sms.db_commit()
        except:
            exc()

    @staticmethod
    def _convert_2_timestamp(format_time):
        ''' '2013-10-10 23:40:00' => 10位 时间戳
        '''
        try:
            ts = time.strptime(format_time, "%Y-%m-%d %H:%M:%S")
            return time.mktime(ts)
        except:
            return 

