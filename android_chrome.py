# coding=utf-8
__author__ = 'YangLiyuan'

import datetime
import hashlib
import json

import clr
try:
    clr.AddReference('model_browser')
    clr.AddReference('bcp_browser')
    clr.AddReference('apple_chrome')
except:
    pass
del clr

from PA_runtime import *
import model_browser
import bcp_browser
from apple_chrome import exc, tp, print_run_time, BaseChromeParser


DEBUG = True
DEBUG = False

# app数据库版本
VERSION_APP_VALUE = 2

CASE_NAME = ds.ProjectState.ProjectDir.Name


def analyze_chrome(node, extract_deleted, extract_source):
    ''' android: com.android.chrome/databases/WXStorage$ 
        apple:   /Library/Application Support/Google/Chrome/Default/History$
    '''
    tp('android_chrome.py is running ...')
    ''' Patterns:string>/Library/Application Support/Google/Chrome/Default/History$  '''
    res = []
    pr = ParserResults()
    try:
        res = AndroidChromeParser(node, extract_deleted, extract_source).parse()
    except:
        TraceService.Trace(TraceLevel.Debug,
                           'analyze_chrome 解析新案例 <{}> 出错: {}'.format(CASE_NAME, traceback.format_exc()))
    if res:
        pr.Models.AddRange(res)
        pr.Build('Chrome浏览器')
        tp('android_chrome.py is finished !')
    return pr

class AndroidChromeParser(BaseChromeParser):
    def __init__(self, node, extract_deleted, extract_source):
        ''' Patterns: /com\.android\.chrome/app_chrome/Default/History$ 
            self.root: /com.android.chrome/app_chrome 
        '''
        super(AndroidChromeParser, self).__init__(node, extract_deleted, extract_source)
        hash_str = hashlib.md5(node.AbsolutePath).hexdigest()[8:-8]
        self.cache_db = self.cachepath + '\\a_chrome_{}.db'.format(hash_str)

        if self.root.FileSystem.Name == 'data.tar':
            self.rename_file_path = ['/storage/emulated', '/data/media'] 
        else:
            self.rename_file_path = None
    
    def _2_nodepath(self, raw_path):
        ''' huawei: /data/user/0/com.baidu.searchbox/files/template/profile.zip
        '''
        try:
            if not raw_path:
                return
            if self.rename_file_path: 
                # replace: '/storage/emulated', '/data/media'
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
        except:
            tp('android_chrome.py _conver_2_nodeapth error, raw_path:', raw_path)
            exc()    

