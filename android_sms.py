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
import bcp_basic
import model_sms
from collections import OrderedDict
from ScriptUtils import DEBUG, CASE_NAME, exc, tp, BaseAndroidParser, parse_decorator

VERSION_APP_VALUE = 6

MSG_TYPE_ALL    = 0
MSG_TYPE_INBOX  = 1
MSG_TYPE_SENT   = 2
MSG_TYPE_DRAFT  = 3
MSG_TYPE_OUTBOX = 4
MSG_TYPE_FAILED = 5
MSG_TYPE_QUEUED = 6

# 彩信地址类型
PDUHEADERS_BCC  = 129
PDUHEADERS_CC   = 130
PDUHEADERS_FROM = 137
PDUHEADERS_TO   = 151

VS_FLAG = False

@parse_decorator
def analyze_sms(node, extract_deleted, extract_source):
    # 首先匹配 icing_mmssms, 然后剩下的只返回最先匹配到的, 不重复匹配
    SMS_PATTERNS = OrderedDict([
        (r'(?i)/com\.google\.android\.gms/databases/icing_mmssms\.db$', AndroidIcingParser),
        (r'(?i)/com.sec.android.provider.logsprovider/databases/logs\.db$', OldSamsungSMSMMSParser),
        (r'(?i)/com.android.providers.telephony/databases/mmssms\.db$', AndroidSMSParser),
        (r'(?i)/com.android.mms/databases/mmssms\.db$', AndroidSMSParser),
        (r'(?i)/sms/sms\.db$', AndroidSMSParserFsLogic),
        (r'(?i)/sms\.vmsg$', VMSGParser),                   # AutoBackup OPPO MEIZU
        (r'(?i)/sms\.db$', AutoBackupHuaweiSMSParser),      # AutoBackup HuaWei
    ])
    res = []
    hit_nodes = []
    pr = ParserResults()
    BCP_TYPE = bcp_basic.BASIC_SMS_INFORMATION
    db_name = 'AndroidSMS'
    try:
        for _pattern, _parser in SMS_PATTERNS.items():
            _nodes = node.FileSystem.Search(_pattern)
            if len(list(_nodes)) != 0:
                hit_nodes.append((_parser, _nodes))
                if _parser not in (AndroidIcingParser, OldSamsungSMSMMSParser, AndroidSMSParser):
                    break

        if hit_nodes:
            progress.Start()
        else:
            progress.Skip()
            return pr

        for _parser_nodes in hit_nodes:
            _parser = _parser_nodes[0]
            for node in _parser_nodes[1]:
                if 'media' in node.AbsolutePath:
                    continue
                res.extend(_parser(node, db_name).parse(BCP_TYPE, VERSION_APP_VALUE))

        if res:
            pr.Models.AddRange(res)
            pr.Build('短信')
    except:
        if DEBUG:
            msg = '{} 解析新案例 <{}> 出错: {}'.format(db_name, CASE_NAME, traceback.format_exc())
            TraceService.Trace(TraceLevel.Debug, msg)
    return pr


@parse_decorator
def analyze_mms(node, extract_deleted, extract_source):
    MMS_PATTERNS = OrderedDict([
        (r'(?i)com.android.providers.telephony/databases/mmssms\.db$', AndroidMMSParser),
        (r'(?i)/com.android.mms/databases/mmssms\.db$', AndroidMMSParser),
        (r'(?i)/sms.db', AndroidMMSParser,),  # AutoBackup HuaWei
    ])
    res = []
    hit_nodes = []
    pr = ParserResults()
    BCP_TYPE = bcp_basic.BASIC_MMS_INFORMATION
    db_name = 'AndroidMMS'
    try:
        for _pattern, _parser in MMS_PATTERNS.items():
            _nodes = node.FileSystem.Search(_pattern)
            if len(list(_nodes)) != 0:
                hit_nodes.append([_parser, _nodes])

        if hit_nodes:
            progress.Start()
        else:
            progress.Skip()
            return pr

        for _parser_nodes in hit_nodes:
            _parser = _parser_nodes[0]
            for node in _parser_nodes[1]:
                if node.AbsolutePath.endswith('/sms/sms.db'):
                    continue
                if node.AbsolutePath.endswith('/sms.db'):
                    cur_db = SQLiteParser.Database.FromNode(node, canceller)
                    if 'pdu_tb' not in cur_db.Tables:
                        continue
                res.extend(_parser(node, db_name).parse(BCP_TYPE, VERSION_APP_VALUE))
        if res:
            pr.Models.AddRange(res)
            pr.Build('彩信')
    except:
        if DEBUG:
            msg = '{} 解析新案例 <{}> 出错: {}'.format(db_name, CASE_NAME, traceback.format_exc())
            TraceService.Trace(TraceLevel.Debug, msg)

    return pr


class AndroidSMSParser(BaseAndroidParser):
    def __init__(self, node, db_name):
        super(AndroidSMSParser, self).__init__(node, db_name)
        self.VERSION_VALUE_DB = model_sms.VERSION_VALUE_DB
        self.root = node.Parent
        self.csm = model_sms.ModelSMS()
        self.Generate = model_sms.GenerateSMSModel

        self.contacts = {}
        self.sim_phonenumber = {}

    def parse_main(self):
        self.pre_parse_calls()
        if self._read_db('mmssms.db'):
            if 'sim_cards' in self.cur_db.Tables:
                self.parse_sim_cards('sim_cards')
            self.parse_sms('sms')

    def parse_sim_cards(self, table_name):
        """ 
            sms - 短信
        """
        for rec in self._read_table(table_name):
            try:
                if self._is_empty(rec, 'number'):
                    continue
                sim = model_sms.SimCard()
                sim.sim_id = rec['sim_id'].Value
                sim.number = rec['number'].Value
                sim.sync_enabled = rec['sync_enabled'].Value
                sim.source = self.cur_db_source
                sim.deleted = 1 if rec.IsDeleted else 0
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
        BASE_DIR = os.path.dirname(self.cachepath)
        # tp(BASE_DIR)
        calls_path = os.path.join(BASE_DIR, 'Contact')
        # tp(calls_path)
        try:
            if not os.listdir(calls_path):
                exc('####### android_sms.py: Contact 目录下没有 db')
                return
                # tp('calls_path', calls_path)
            for f in os.listdir(calls_path):
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
        pk_name = '_id'
        if table_name == 'sms_tb':
            pk_name = 'group_id'
        for rec in self._read_table(table_name):
            try:
                if (self._is_empty(rec, 'type', 'body')
                        or self._is_duplicate(rec, pk_name)
                        or self._include_garbled_str(rec['body'].Value)):
                    continue
                if (rec['address'].Value and
                        not self._is_num(rec['address'].Value)):
                    continue
                sms = model_sms.SMS()
                # 华为没有的字段
                sms.sim_id  = rec['sim_id'].Value if rec.ContainsKey('sim_id') else None
                sms.deleted = rec['deleted'].Value if rec.ContainsKey('deleted') else 0
                sms.smsc    = rec['service_center'].Value if rec.ContainsKey('service_center') else None

                sms._id            = rec[pk_name].Value
                sms.read_status    = rec['read'].Value
                sms.type           = rec['type'].Value    # MSG_TYPE
                sms.subject        = rec['subject'].Value
                sms.body           = rec['body'].Value
                sms.delivered_date = rec['date'].Value
                sms.send_time      = rec['date_sent'].Value if 'date_sent' in rec.Keys else sms.delivered_date
                sms.is_sender = 1 if sms.type in (MSG_TYPE_SENT, MSG_TYPE_OUTBOX, MSG_TYPE_DRAFT) else 0
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
                self.csm.db_insert_table_sms(sms)
            except:
                exc()
        self.csm.db_commit()

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

    def _is_duplicate(self, rec=None, pk_name='', pk_value=None):
        if VS_FLAG:
            return False
        return super(AndroidSMSParser, self)._is_duplicate(rec, pk_name, pk_value)



class AndroidMMSParser(AndroidSMSParser):
    def __init__(self, node, db_name):
        super(AndroidMMSParser, self).__init__(node, db_name)
        self.csm = model_sms.ModelMMS()
        self.Generate = model_sms.GenerateMMSModel

    def parse_main(self):
        if ('com.android.providers.telephony' in self.root.AbsolutePath
                or 'com.android.mms' in self.root.AbsolutePath):
            if self._read_db('mmssms.db'):
                addr_dict = self.preparse_addr('addr')
                mms_mid_text_dict = self.parse_part('part')
                self.parse_mms('pdu', addr_dict, mms_mid_text_dict)

        elif (not self.root.AbsolutePath.endswith('sms/sms.db')
              and self.root.AbsolutePath.endswith('/sms.db')):
            if self._read_db(node=self.root):
                addr_dict = self.preparse_addr('addr_tb')
                mms_mid_text_dict = self.parse_part('part_tb')
                self.parse_mms('pdu_tb', addr_dict, mms_mid_text_dict)

    def preparse_addr(self, table_name):
        ''' addr

            FieldName	SQLType   	
            _id	            INTEGER
            msg_id	        INTEGER     # 
            contact_id	    INTEGER
            address	        TEXT        # 电话号码，如果为 insert-address-token 且 type 为 151，说明为本机号码
            type	        INTEGER     # 电话号码的类型，必须为  PduHeaders.BCC-129，
                                                              PduHeaders.CC-130，
                                                              PduHeaders.FROM-137，
                                                              PduHeaders.TO-151       之一
            charset	        INTEGER
        '''
        addr_dict = {}
        for rec in self._read_table(table_name):
            try:
                if rec.ContainsKey('_id') and self._is_duplicate(rec, '_id'):
                    continue
                mms_id = rec['msg_id'].Value
                if not addr_dict.has_key(mms_id):
                    addr_dict[mms_id] = {}
                _address = rec['address'].Value.replace('insert-address-token', '')
                _type = rec['type'].Value

                if _type == PDUHEADERS_FROM:
                    addr_dict[mms_id]['from_address'] = _address
                elif _type == PDUHEADERS_TO:
                    addr_dict[mms_id]['to_address'] = _address
            except:
                exc()
        return addr_dict

    def parse_part(self, table_name):
        ''' part 

            FieldName	SQLType    
            _id	            INTEGER 
            mid	            INTEGER 
            seq	            INTEGER 
            ct	            TEXT    
            name	        TEXT    
            chset	        INTEGER     # UTF-8为106
            cd	            TEXT        # CONTENT_DISPOSITION
            fn	            TEXT    
            cid	            TEXT    
            cl	            TEXT        # 华为 autobackup 为文件名
            ct	            TEXT        # 华为 autobackup 独有为文件类型
            ctt_s	        INTEGER 
            ctt_t	        TEXT        # CONTENT_TYPE 
            _data	        TEXT    
            text	        TEXT        # 如果是彩信始末，为彩信的SMIL内容；如果是文本附件，为附件内容；如果是视频、音频附件，此参数为空
        '''
        mms_mid_text_dict = {}
        for rec in self._read_table(table_name):
            try:
                if (rec.ContainsKey('_id') and self._is_duplicate(rec, '_id')
                        or self._is_empty(rec, 'ct')):
                    continue
                part = model_sms.MMSPart()
                if rec.ContainsKey('_id'):
                    part._id = rec['_id'].Value 
                part.mms_id = rec['mid'].Value
                # part.sim_id
                part.part_filename = rec['cl'].Value if rec.ContainsKey('cl') else rec['name'].Value
                part.part_local_path = self._convert_nodepath(rec['_data'].Value)
                # part.part_text = 
                # part.part_charset
                if '/' in rec['ct'].Value:
                    part.part_contenttype = rec['ct'].Value.split('/')[0]

                if rec['text'].Value and part.part_contenttype == 'text':
                    mms_mid_text_dict[part.mms_id] = rec['text'].Value
                self.csm.db_insert_table_mms_part(part)
            except:
                exc()
        self.csm.db_commit()
        return mms_mid_text_dict


    def parse_mms(self, table_name, addr_dict, mms_mid_text_dict):
        """ pdu - 彩信

            FieldName	SQLType	
            _id	            INTEGER
            thread_id	    INTEGER
            date	        INTEGER
            date_sent	    INTEGER
            msg_box	        INTEGER     # MSG_TYPE
            read	        INTEGER
            m_id	        TEXT
            sub	            TEXT
            sub_cs	        INTEGER     # 主题所用字符集
            ct_t	        TEXT        # 彩信对应的Content-Type是application/vnd.wap.multipart.related
            ct_l	        TEXT        # X-Mms-Content-Location
            exp	            INTEGER     # 过期时间
            m_cls	        TEXT        # X-Mms-Message-Class，此条彩信的用途：auto，advertisement，personal，informational
            m_type	        INTEGER     # X-Mms-Message-Type，由MMS协议定义的彩信类型，、、
                                            128 if sent             send-req:128
                                            130 if to send again    notification-ind:130
                                            132 if resent           retrieve-conf为132
            v	            INTEGER
            m_size	        INTEGER
            pri	            INTEGER     # X-Mms-Priority，此条彩信的优先级，normal 129，low 128，high 130
            rr	            INTEGER
            rpt_a	        INTEGER
            resp_st	        INTEGER
            st	            INTEGER     # 该彩信的下载状态，未启动-128，下载中-129，传输失败-130，保存失败-135
            tr_id	        TEXT
            retr_st	        INTEGER
            retr_txt	    TEXT
            retr_txt_cs	    INTEGER
            read_status	    INTEGER
            ct_cls	        INTEGER
            resp_txt	    TEXT
            d_tm	        INTEGER
            d_rpt	        INTEGER
            locked	        INTEGER
            seen	        INTEGER
            sub_id	        INTEGER
            network_type	INTEGER
            creator	        TEXT
            text_only	    INTEGER
            privacy_mode	integer
            is_secret	    INTEGER
        """
        for rec in self._read_table(table_name):
            try:
                mms = model_sms.SMS()
                if (rec.ContainsKey('_id') and 
                    self._is_duplicate(rec, '_id')):
                    continue
                mms.is_mms             = 1
                mms._id                = rec['_id'].Value
                mms.sender_phonenumber = addr_dict.get(mms._id, {}).get('from_address')
                mms.recv_phonenumber   = addr_dict.get(mms._id, {}).get('to_address')
                mms.subject            = self._decode_mms_subject(rec['sub'].Value)
                mms.read_status        = rec['read'].Value
                mms.body               = mms_mid_text_dict.get(mms._id)
                mms.delivered_date     = rec['date'].Value
                mms.send_time          = rec['date_sent'].Value if 'date_sent' in rec.Keys else mms.delivered_date
                mms.type               = rec['msg_box'].Value        # MSG_TYPE
                mms.is_sender          = 1 if mms.type in (MSG_TYPE_SENT, MSG_TYPE_OUTBOX) else 0
                mms.deleted            = 1 if rec.IsDeleted else 0
                mms.source             = self.cur_db_source
                self.csm.db_insert_table_mms(mms)
            except:
                exc()
        self.csm.db_commit()

    def _convert_nodepath(self, raw_path):
        '''
        /data/user_de/0/com.android.providers.telephony/app_parts/PART_1548924720904_1545360899806joint_15453608969.mp4
        '''
        try:
            if not raw_path:
                return
            _path = raw_path.split('com.android.providers.telephony')
            if len(_path) == 2:
                local_path_node = self.root.Parent.GetByPath(_path[1])
                if local_path_node and local_path_node.Type == NodeType.File:
                    return local_path_node.AbsolutePath

            if self.rename_file_path:
                raw_path = raw_path.replace(self.rename_file_path[0], self.rename_file_path[1])
            fs = self.root.FileSystem
            for prefix in ['', '/data', ]:
                file_node = fs.GetByPath(prefix + raw_path)
                if file_node and file_node.Type == NodeType.File:
                    return file_node.AbsolutePath
                invalid_path = re.search(r'[\\:*?"<>|\r\n]+', raw_path)
                if invalid_path:
                    return
                nodes = list(fs.Search(raw_path))
                if nodes and nodes[0].Type == NodeType.File:
                    return nodes[0].AbsolutePath
            return raw_path
        except:
            exc()

    @staticmethod
    def _decode_mms_subject(_rec_value):
        ''' handle mms subject garbled '''
        try:
            if IsDBNull(_rec_value):
                return
            _list = list(bytearray(_rec_value, 'utf8', 'ignore'))
            if _list.count(0) < 2 or _list[:2] != [0xff, 0xfe]:
                _res = _rec_value.decode('utf8', 'ignore')
            else:
                # 去 0
                stripped_list = _list[2:-1:2]
                _res = str(bytearray(stripped_list, 'utf8', 'ignore')).decode('utf8')
            return _res
        except:
            tp('_decode_mms_subject'+_rec_value)
            return _rec_value


class AndroidSMSParserFsLogic(AndroidSMSParser):
    ''' 处理逻辑提取案例, 非 tar 包, sms/sms.db$ '''

    def __init__(self, node, db_name):
        super(AndroidSMSParserFsLogic, self).__init__(node, db_name)
        self.root = node

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
                        rec['isMms'].Value == 1):
                    continue
                sms = model_sms.SMS()
                try:
                    sms.sms_id = rec['phoneNumber'].Value + rec['time'].Value
                    if (self._is_duplicate(pk_value=sms.sms_id)
                            or self._include_garbled_str(rec['body'].Value)):
                        continue
                except:
                    exc()
                    continue
                # sms.sender_phonenumber = rec['phoneNumber'].Value if rec['phoneNumber'].Value != 'insert-address-token' else None
                # sms.sender_name        = self.contacts.get(sms.sender_phonenumber, None)
                if rec['shortType'].Value == 1:
                    sms.type = MSG_TYPE_INBOX 
                elif rec['shortType'].Value == 2:
                    sms.type = MSG_TYPE_OUTBOX
                else:
                    sms.type = MSG_TYPE_ALL
                sms.read_status    = rec['shortRead'].Value
                sms.subject        = rec['theme'].Value
                sms.body           = rec['body'].Value.replace('\0', '')
                sms.send_time      = self._convert_strtime_2_ts(rec['time'].Value)
                sms.delivered_date = sms.send_time
                sms.is_sender      = 1 if sms.type == MSG_TYPE_OUTBOX else 0

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


class VMSGParser(AndroidSMSParser):
    def __init__(self, node, db_name):
        super(VMSGParser, self).__init__(node, db_name)
        self.root = node

    def parse_main(self):
        self.pre_parse_calls()
        vmsg_node = self.root
        self.vmsg_source = vmsg_node.AbsolutePath
        self.parse_sms(vmsg_node)

    def parse_sms(self, node):
        res = model_sms.VMSG(node).dict_from_vmsg()
        for record in res:
            sms = model_sms.SMS()
            # sms.sim_id      = record.get('')
            # sms.smsc        = record.get('')
            # sms._id         = record.get('')
            sms.read_status = record.get('read_status')
            sms.type        = record.get('box')
            # sms.subject     = record.get('')
            sms.body        = record.get('content')
            sms.source      = self.vmsg_source
            _date = record.get('date')
            if isinstance(_date, (int, long)):
                sms.delivered_date = _date
            elif isinstance(_date, str):
                sms.delivered_date = self._convert_strtime_2_ts(_date)
            #sms.delivered_date = sms.send_time
            sms.is_sender = 1 if sms.type in (MSG_TYPE_SENT, MSG_TYPE_OUTBOX, MSG_TYPE_DRAFT) else 0
            if sms.is_sender == 1:  # 发
                sms.sender_phonenumber = self.sim_phonenumber.get(sms.sim_id, None) if sms.sim_id else None
                sms.sender_name        = self._get_contacts(sms.sender_phonenumber)
                sms.recv_phonenumber   = record.get('tel')
                sms.recv_name          = self._get_contacts(sms.recv_phonenumber)
            else:                   # 收
                sms.sender_phonenumber = record.get('tel')
                sms.sender_name        = self._get_contacts(sms.sender_phonenumber)
                sms.recv_phonenumber   = self.sim_phonenumber.get(sms.sim_id, None) if sms.sim_id else None
                sms.recv_name          = self._get_contacts(sms.recv_phonenumber)
            self.csm.db_insert_table_sms(sms)
        self.csm.db_commit()


class AutoBackupHuaweiSMSParser(AndroidSMSParser):
    def __init__(self, node, db_name):
        super(AutoBackupHuaweiSMSParser, self).__init__(node, db_name)
        self.root = node

    def parse_main(self):
        """ /sms.db - sms_tb no _id field

            FieldName	        SQLType	             	
            group_id	            INTEGER
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
            service_center	        TEXT
            error_code	            INTEGER
            locked	                INTEGER
            network_type	        INTEGER
            status	                INTEGER
        """
        self.pre_parse_calls()
        if self._read_db(node=self.root) and 'sms_tb' in self.cur_db.Tables:
            self.parse_sms('sms_tb')


class AndroidIcingParser(AndroidSMSParser):
    def __init__(self, node, db_name):
        super(AndroidIcingParser, self).__init__(node, db_name)
        self.root = node
        self.csm = model_sms.ModelSMS()
        self.Generate = model_sms.GenerateSMSModel

        self.csm_mms = model_sms.ModelMMS()
        self.mms_cache_db = self.cache_db.replace('.db', '_mms.db')
        self.csm_mms.db_create(self.mms_cache_db)

    def parse(self, BCP_TYPE, VERSION_APP_VALUE):
        models = super(AndroidIcingParser, self).parse(BCP_TYPE, VERSION_APP_VALUE)
        self.csm_mms.db_close()
        browser_models = model_sms.GenerateMMSModel(self.mms_cache_db).get_models()
        models.extend(browser_models)
        return models

    def parse_main(self):
        self.pre_parse_calls()
        if self._read_db(node=self.root):
            self.parse_icing('mmssms')

    def parse_icing(self, table_name):
        '''com.google.android.gms/databases/icing_mmssms.db

            FieldName	SQLType     	
            _id	            INTEGER
            msg_type	    TEXT
            uri	            TEXT
            type	        INTEGER
            thread_id	    INTEGER
            address	        TEXT
            date	        INTEGER
            subject	        TEXT
            body	        TEXT
            score	        INTEGER
            content_type	TEXT
            media_uri	    TEXT
            read	        INTEGER
        '''
        _sms_list = []
        for rec in self._read_table(table_name):
            try:
                pk_name = '_id'
                if (self._is_empty(rec, 'type', 'body')
                        or self._is_duplicate(rec, pk_name)
                        or self._include_garbled_str(rec['body'].Value)):
                    continue

                if rec['msg_type'].Value == 'sms':
                    self._parse_icing_sms(rec)
                elif rec['msg_type'].Value == 'mms':
                    self._parse_icing_mms(rec)
            except:
                exc()
        self.csm.db_commit()
        self.csm_mms.db_commit()

    def _parse_icing_sms(self, rec):
        sms = model_sms.SMS()
        # content://sms/7
        # sms_id = rec['uri'].Value.replace('content://sms/', '')                
        sms.sim_id  = rec['_id'].Value if rec.ContainsKey('_id') else None
        sms.deleted = rec['deleted'].Value if rec.ContainsKey('deleted') else 0
        sms.smsc    = rec['service_center'].Value if rec.ContainsKey('service_center') else None
        sms._id            = rec['_id'].Value
        # sms.read_status    = rec['read'].Value
        sms.type           = rec['type'].Value    # MSG_TYPE
        sms.subject        = rec['subject'].Value 
        sms.body           = rec['body'].Value
        sms.send_time      = rec['date'].Value
        sms.delivered_date = rec['date'].Value if sms.type in [MSG_TYPE_INBOX, MSG_TYPE_SENT] else None
        sms.is_sender = 1 if sms.type in (MSG_TYPE_SENT, MSG_TYPE_OUTBOX, MSG_TYPE_DRAFT) else 0
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
        self.csm.db_insert_table_sms(sms)        

    def _parse_icing_mms(self, rec):
        ''' FieldName	SQLType     	
            _id	            INTEGER
            msg_type	    TEXT
            uri	            TEXT
            type	        INTEGER
            thread_id	    INTEGER
            address	        TEXT
            date	        INTEGER
            subject	        TEXT
            body	        TEXT
            score	        INTEGER
            content_type	TEXT
            media_uri	    TEXT
            read	        INTEGER
        '''
        try:
            mms = model_sms.SMS()
            mms.is_mms         = 1
            mms._id            = rec['_id'].Value
            mms.subject        = AndroidMMSParser._decode_mms_subject(rec['subject'].Value)
            mms.read_status    = rec['read'].Value
            # mms.body           = rec['body'].Value
            mms.send_time      = rec['date'].Value
            mms.delivered_date = rec['date'].Value if mms.type in [MSG_TYPE_INBOX, MSG_TYPE_SENT] else None
            mms.type           = rec['type'].Value        # MSG_TYPE
            mms.is_sender      = 1 if mms.type in (MSG_TYPE_SENT, MSG_TYPE_OUTBOX) else 0

            if mms.is_sender == 1:  # 发
                mms.sender_phonenumber = self.sim_phonenumber.get(mms.sim_id, None) if mms.sim_id else None
                mms.sender_name        = self._get_contacts(mms.sender_phonenumber)
                mms.recv_phonenumber   = rec['address'].Value
                mms.recv_name          = self._get_contacts(mms.recv_phonenumber)
            else:                   # 收
                mms.sender_phonenumber = rec['address'].Value
                mms.sender_name        = self._get_contacts(mms.sender_phonenumber)
                mms.recv_phonenumber   = self.sim_phonenumber.get(mms.sim_id, None) if mms.sim_id else None
                mms.recv_name          = self._get_contacts(mms.recv_phonenumber)

            mms.deleted = 1 if rec.IsDeleted else 0
            mms.source  = self.cur_db_source
            self.csm_mms.db_insert_table_mms(mms)
        except:
            exc()


class OldSamsungSMSMMSParser(AndroidSMSParser):
    def __init__(self, node, db_name):
        super(OldSamsungSMSMMSParser, self).__init__(node, db_name)
        self.root = node
        self.csm = model_sms.ModelSMS()
        self.Generate = model_sms.GenerateSMSModel

        self.csm_mms = model_sms.ModelMMS()
        self.mms_cache_db = self.cache_db.replace('.db', '_mms.db')
        self.csm_mms.db_create(self.mms_cache_db)

    def parse(self, BCP_TYPE, VERSION_APP_VALUE):
        models = super(OldSamsungSMSMMSParser, self).parse(BCP_TYPE, VERSION_APP_VALUE)
        self.csm_mms.db_close()
        browser_models = model_sms.GenerateMMSModel(self.mms_cache_db).get_models()
        models.extend(browser_models)
        return models

    def parse_main(self):
        self.pre_parse_calls()
        if self._read_db(node=self.root):
            self.parse_logs('logs')

    def parse_logs(self, table_name):
        '''
            FieldName	SQLType
            _id
            number	              TEXT
            address	              TEXT
            date	              INTEGER
            duration	          INTEGER
            type	              INTEGER
            new
            simnum	              INTEGER
            name	              TEXT
            name_reversed	      TEXT
            numbertype	          INTEGER
            numberlabel	          TEXT
            messageid	          TEXT
            threadid	          TEXT
            logtype	              INTEGER
            frequent	          INTEGER
            contactid	          INTEGER
            raw_contact_id	      INTEGER
            m_subject	          TEXT
            m_content	          TEXT
            sns_tid	              TEXT
            sns_pkey	          TEXT
            account_name	      TEXT
            account_id	          TEXT
            sns_receiver_coun     TEXT
            sp_type	              TEXT
            cnap_name	          TEXT
            cdnip_number	      TEXT
            service_type	      INTEGER
            sdn_alpha_id	      TEXT
            real_phone_number     TEXT
            call_out_duration     INTEGER
            reject_flag	          INTEGER
        '''
        _sms_list = []
        for rec in self._read_table(table_name):
            try:
                pk_name = '_id'
                if (self._is_empty(rec, 'number', 'date')
                        or self._is_duplicate(rec, pk_name)
                        or self._include_garbled_str(rec['m_content'].Value)):
                    continue

                # logtype 200 是彩信， 300 是短信， 100 是通话记录
                if rec['logtype'].Value == 300:
                    self._parse_logs_sms(rec)
                elif rec['logtype'].Value == 200:
                    self._parse_logs_mms(rec)
            except:
                exc()
        self.csm.db_commit()
        self.csm_mms.db_commit()

    def _parse_logs_sms(self, rec):
        sms = model_sms.SMS()
        sms._id = rec['_id'].Value
        sms.sim_id = rec['simnum'].Value if rec.ContainsKey('simnum') else None
        if rec.ContainsKey('is_read'):
            sms.read_status = rec['is_read'].Value
        sms.type           = rec['type'].Value    # MSG_TYPE
        sms.body           = rec['m_content'].Value
        sms.send_time      = rec['date'].Value
        sms.delivered_date = rec['date'].Value if sms.type in [MSG_TYPE_INBOX, MSG_TYPE_SENT] else None
        sms.is_sender = 1 if sms.type in (MSG_TYPE_SENT, MSG_TYPE_OUTBOX, MSG_TYPE_DRAFT) else 0
        _number = rec['normalized_number'].Value if rec.ContainsKey('normalized_number') else rec['number'].Value
        if sms.is_sender == 1:  # 发
            sms.sender_phonenumber = self.sim_phonenumber.get(sms.sim_id, None) if sms.sim_id else None
            sms.sender_name        = self._get_contacts(sms.sender_phonenumber)
            sms.recv_phonenumber   = _number
            sms.recv_name          = rec['name'].Value
        else:                   # 收
            sms.sender_phonenumber = _number
            sms.sender_name        = rec['name'].Value
            sms.recv_phonenumber   = self.sim_phonenumber.get(sms.sim_id, None) if sms.sim_id else None
            sms.recv_name          = self._get_contacts(sms.recv_phonenumber)

        sms.deleted = 1 if rec.IsDeleted else 0
        sms.source = self.cur_db_source
        self.csm.db_insert_table_sms(sms)

    def _parse_logs_mms(self, rec):
        try:
            mms = model_sms.SMS()
            mms.is_mms = 1
            mms._id = rec['_id'].Value
            mms.sim_id = rec['simnum'].Value if rec.ContainsKey('simnum') else None
            mms.subject = rec['m_subject'].Value
            if rec.ContainsKey('is_read'):
                mms.read_status = rec['is_read'].Value
            mms.type = rec['type'].Value  # MSG_TYPE
            mms.body = rec['m_content'].Value
            mms.send_time = rec['date'].Value
            mms.delivered_date = rec['date'].Value if mms.type in [MSG_TYPE_INBOX, MSG_TYPE_SENT] else None
            mms.is_sender = 1 if mms.type in (MSG_TYPE_SENT, MSG_TYPE_OUTBOX, MSG_TYPE_DRAFT) else 0
            _number = rec['normalized_number'].Value if rec.ContainsKey('normalized_number') else rec['number'].Value
            if mms.is_sender == 1:  # 发
                mms.sender_phonenumber = self.sim_phonenumber.get(mms.sim_id, None) if mms.sim_id else None
                mms.sender_name = self._get_contacts(mms.sender_phonenumber)
                mms.recv_phonenumber = _number
                mms.recv_name = rec['name'].Value
            else:  # 收
                mms.sender_phonenumber = _number
                mms.sender_name = rec['name'].Value
                mms.recv_phonenumber = self.sim_phonenumber.get(mms.sim_id, None) if mms.sim_id else None
                mms.recv_name = self._get_contacts(mms.recv_phonenumber)

            mms.deleted = 1 if rec.IsDeleted else 0
            mms.source = self.cur_db_source
            self.csm_mms.db_insert_table_mms(mms)
        except:
            exc()
