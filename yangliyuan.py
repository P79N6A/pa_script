# coding=utf-8

__author__ = "YangLiyuan"

from PA_runtime import *
import hashlib
import json


######### SETTING #########
CASE_NAME = ds.ProjectState.ProjectDir.Name
DEBUG = True
DEBUG = False
DEBUG_RUN_TIME = True
DEBUG_RUN_TIME = False


######### LOG FUNC #########
def exc(e=''):
    ''' Exception log output '''
    try:
        if DEBUG:
            py_name = os.path.basename(__file__)
            msg = 'DEBUG {} case:<{}> :'.format(py_name, CASE_NAME)
            TraceService.Trace(TraceLevel.Warning,
                               (msg+'{}{}').format(traceback.format_exc(), e))
    except:
        pass


def tp(*e):
    ''' Highlight log output in vs '''
    if DEBUG:
        TraceService.Trace(TraceLevel.Warning, '{}'.format(e))
    else:
        pass


def print_run_time(func):
    def wrapper(*args, **kw):
        local_time = time.time()
        res = func(*args, **kw)
        if DEBUG_RUN_TIME:
            msg = 'Current Function <{}> run time is {:.2} s'.format(
                func.__name__, time.time() - local_time)
            TraceService.Trace(TraceLevel.Warning, '{}'.format(msg))
        if res:
            return res
    return wrapper


def parse_decorator(func):
    def wrapper(*args, **kw):
        tp('{} is running ...'.format(func.__name__,))
        res = func(*args, **kw)
        tp('{} is finished !'.format(func.__name__,))
        return res
    return wrapper        


######### Base Class #########

def base_analyze(Parser, node, extract_deleted, extract_source, BCP_TYPE, VERSION_APP_VALUE, bulid_name, db_name):
    '''
    Args:
        Parser (Parser):
        node (node): 
        BCP_TYPE: 
        VERSION_APP_VALUE (int): VERSION_APP_VALUE
        bulid_name (str): pr.build
        db_name (str): 中间数据库名称
    Returns:
        pr
    '''
    if 'media' in node.AbsolutePath:
        return 
    res = []
    pr = ParserResults()
    try:
        parser = Parser(node, extract_deleted, extract_source, db_name)
        res = parser.parse(BCP_TYPE, VERSION_APP_VALUE)
    except:
        msg = 'analyze_browser.py-{} 解析新案例 <{}> 出错: {}'.format(db_name, CASE_NAME, traceback.format_exc())
        TraceService.Trace(TraceLevel.Debug, msg)
    if res:
        pr.Models.AddRange(res)
        pr.Build(bulid_name)
    return pr

class BaseParser(object):
    ''' common func:
            _read_db
            _read_table
            _read_json
            _read_xml
            _is_url
            _is_empty
            _is_duplicate
                    
        Instances need to be implemented : 
            attribute:
                self.root
                self.csm        # c sharp model 
                self.Generate   # e.g. model_browser.Generate
                self.VERSION_KEY_DB
                self.VERSION_VALUE_DB
                self.VERSION_KEY_APP 
            func:
                parse
                _convert_nodepath
                update_version
    '''
    def __init__(self, node, extract_deleted, extract_source, db_name=''):
        self.root = node
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.csm = None
        self.Generate = None
        hash_str = hashlib.md5(node.AbsolutePath).hexdigest()[8:-8]
        self.cachepath = ds.OpenCachePath(db_name)
        self.cache_db = self.cachepath + '\\{}_{}.db'.format(db_name, hash_str)
        self.VERSION_KEY_DB = ''
        self.VERSION_VALUE_DB = 0
        self.VERSION_KEY_APP = ''

    def parse(self, BCP_TYPE, VERSION_APP_VALUE):
        if DEBUG or self.csm.need_parse(self.cache_db, VERSION_APP_VALUE):
            self.csm.db_create(self.cache_db)
            self.parse_main()
            if not canceller.IsCancellationRequested:
                self.csm.db_insert_table_version(self.VERSION_KEY_DB, self.VERSION_VALUE_DB)
                self.csm.db_insert_table_version(self.VERSION_KEY_APP, VERSION_APP_VALUE)
                self.csm.db_commit()        
            self.csm.db_close()
            tmp_dir = ds.OpenCachePath('tmp')
            save_cache_path(BCP_TYPE, self.cache_db, tmp_dir)   
        models = self.Generate(self.cache_db).get_models()
        return models     

    def parse_main(self):
        pass           

    def _convert_nodepath(self, raw_path):
        pass

    def _read_db(self, db_path, node=None):
        ''' and set self.cur_db, self.cur_db_source
        
        Args:
            db_path (str): 
        Returns:
            bool: is valid db
        '''
        try:
            if node is None:
                node = self.root.GetByPath(db_path)
            self.cur_db = SQLiteParser.Database.FromNode(node, canceller)
            if self.cur_db is None:
                return False
            self.cur_db_source = node.AbsolutePath
            return True
        except:
            exc()
            return False

    def _read_table(self, table_name, read_delete=None):
        ''' read_table
        
        Args:
            table_name (str): 
        Returns:
            (iterable): self.cur_db.ReadTableDeletedRecords(tb, ...)
        '''
        # 每次读表清空并初始化 self._PK_LIST
        self._PK_LIST = []
        if read_delete is None:
            read_delete = self.extract_deleted
        try:
            tb = SQLiteParser.TableSignature(table_name)
            return self.cur_db.ReadTableRecords(tb, read_delete, True)
        except:
            exc()
            return []

    def _read_json(self, json_path):
        ''' read_json set self.cur_json_source

        Args: 
            json_path (str)
        Returns:
            (bool)
        '''
        try:
            json_node = self.root.GetByPath(json_path)
            if not json_node:
                return False
            file = json_node.Data.read().decode('utf-8')
            json_data = json.loads(file)
            self.cur_json_source = json_node.AbsolutePath
            return json_data
        except:
            exc()
            return False

    def _is_duplicate(self, rec, pk_name):
        ''' filter duplicate record

        Args:
            rec (record): 
            pk_name (str): 
        Returns:
            bool: rec[pk_name].Value in self._PK_LIST
        '''
        try:
            pk_value = rec[pk_name].Value
            if IsDBNull(pk_value) or pk_value in self._PK_LIST:
                return True
            self._PK_LIST.append(pk_value)
            return False
        except:
            exc()
            return True

    @staticmethod
    def _is_empty(rec, *args):
        ''' 过滤 DBNull 空数据, 有一空值就跳过
        
        Args:
            rec (rec): 
            args (str): fields
        Returns:
            book:
        '''
        try:
            for i in args:
                value = rec[i].Value
                if IsDBNull(value) or value in ('', ' ', None, [], {}):
                    return True
                if isinstance(value, str) and re.search(r'[\x00-\x08\x0b-\x0c\x0e-\x1f]', str(value)):
                    return True
            return False
        except:
            exc()
            tp(*args)
            return True

    def _is_contains(self, rec, *keys):
        return False  not in [rec.ContainsKey(key) for key in keys]

    @staticmethod
    def _is_url(rec, *args):
        ''' 匹配 URL IP
        
        Args:
            rec (rec): 
            *args (tuple<str>):
        Returns:
            bool: 
        '''
        URL_PATTERN = r'((http|ftp|https)://)(([a-zA-Z0-9\._-]+\.[a-zA-Z]{2,6})|([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}))(:[0-9]{1,4})*(/[a-zA-Z0-9\&%_\./-~-]*)?'
        IP_PATTERN = r'^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$'

        for i in args:
            try:
                match_url = re.match(URL_PATTERN, rec[i].Value)
                match_ip = re.match(IP_PATTERN, rec[i].Value)
                if not match_url and not match_ip:
                    return False
            except:
                exc()
                return False
        return True

    @staticmethod
    def _is_phone_number(rec, args):
        # 验证手机号, 包含 +86, 86 开头
        if 'subscriber_mdn' in args:
            s = rec['subscriber_mdn'].Value
            try:
                reg_str = r'^((\+86)|(86))?(1)\d{10}$'
                match_obj = re.match(reg_str, s)
                if match_obj is None:
                    return False      
            except:
                exc()
                return False    

    @staticmethod
    def _is_email_format(rec=None, key=None, email_str=None):
        """ 匹配邮箱地址 

        Args:
            rec (rec): 
            key (str): 
            email_str (str): 
        Returns:
            bool: is valid email address      
        """
        try:
            if email_str is None:
                if IsDBNull(rec[key].Value) or len(rec[key].Value.strip()) < 5:
                    return False
                email_str = rec[key].Value
            reg_str = r'^\w+([-+.]\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$'
            match_obj = re.match(reg_str, email_str)
            if match_obj is None:
                return False      
            return True      
        except:
            exc()
            return False


class BaseAndroidParser(BaseParser):
    def __init__(self, node, extract_deleted, extract_source, db_name=''):
        super(BaseAndroidParser, self).__init__(node, extract_deleted, extract_source, db_name)
        if node.FileSystem.Name == 'data.tar':
            self.rename_file_path = ['/storage/emulated', '/data/media'] 
        else:
            self.rename_file_path = None


class BaseAppleParser(object):
    def __init__(self, node, extract_deleted, extract_source, db_name=''):
        super(BaseAppleParser, self).__init__(node, extract_deleted, extract_source, db_name)
