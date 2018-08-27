#coding=utf-8
import os
import inspect
def SafeLoadAssembly(asm):
    try:
        clr.AddReference(asm)
    except:
        pass

import clr
SafeLoadAssembly('System.Core')
SafeLoadAssembly('PA_runtime')
SafeLoadAssembly('apple_ab')
SafeLoadAssembly('apple_notes')
SafeLoadAssembly('apple_locations')
SafeLoadAssembly('apple_calenders')
SafeLoadAssembly('apple_safari')
SafeLoadAssembly('apple_mails')
SafeLoadAssembly('apple_recents')
SafeLoadAssembly('apple_cookies')
SafeLoadAssembly('apple_calls')
SafeLoadAssembly('apple_sms')
SafeLoadAssembly('apple_wechat')
SafeLoadAssembly('apple_qq')
SafeLoadAssembly('apple_skype')
SafeLoadAssembly('apple_exts')
del clr

#导入app4tests模块,测试时用来指定只跑那些模块
try:
    #app4tests导入在正式版中记得注释掉(想念c/c++/c#的条件编译!!!)
    import app4tests    
    from app4tests import *
except:
    pass

APP_FILTERS =[]
if 'TestNodes' in locals():
    APP_FILTERS.extend(TestNodes)

from System.Linq import Enumerable
from PA_runtime import *
from PA.InfraLib.Services import IApplicationService,ServiceGetter
from android_qqmail import analyze_qqmail_android
"""
根据正则表达式匹配解析的应用请在此节点下配置
"""
FIND_BY_RGX_NODES = [
    # ('/MicroMsg/.+/EnMicroMsg.db$', analyze_wechat, "Wechat","微信",DescripCategories.Wechat),
    #('/data/com.tencent.mm$', analyze_wechat, "Wechat","微信",DescripCategories.Wechat),
    ("com.tencent.androidqqmail", analyze_qqmail_android, "qqMail", "QQ邮箱", DescripCategories.BaiduMap),
]

FIND_BY_APPS_NODES = [
    ("com.tencent.androidqqmail", analyze_qqmail_android, "qqMail", "QQ邮箱", DescripCategories.BaiduMap),
]

def decode_nodes(fs, extract_deleted, extract_source, installed_apps):
    apps = {
        # Todo : 增加应用和标识的映射
        "Facebook": "com.facebook.Messenger",
        "AddressBook": "com.apple.MobileAddressBook",
        "Notes": "com.apple.mobilenotes",
        "Maps": "com.apple.Maps",
        "Safari": "com.apple.mobilesafari",
        "Calendar": "com.apple.mobilecal",
        "PassBook": "com.apple.PassbookUIService",
        "Emails": "com.apple.mobilemail",
        "VoiceMail": "com.apple.AppStore",
        "Line": "jp.naver.line",
        "Wechat": "com.tencent.xin",
        "Copy": "com.copy.agent",
        "GoChat": "com.3g.gochat",
        "VBrowse": "uk.co.bewhere.vbrowse",
        "Tumblr": "com.tumblr.tumblr",
        "Navitel": "su.navitel.app",
		"baiduMap":"com.baidu.map",
        "qqMail":"com.tencent.androidqqmail"
    }
    results = ParserResults()
    fsIdentifer = fs.GetExtraValue[String]('Identifier', '')
    if not fs.IsTopLevel and (len(fsIdentifer) == 0):
        return results #如果不是顶级文件系统,但是没有任何额外属性,则不符合条件(不是顶级文件系统,也不是应用文件系统)
    
    for pattern, func, name,descrip,categories in FIND_BY_RGX_NODES:
        if len(APP_FILTERS) > 0 and not name in APP_FILTERS:
            TraceService.Trace(TraceLevel.Debug, "由于app4tests.py配置策略,应用{0}将不会被解析".format(name))
            continue
        app_id = apps.get(name, '')
        if not fs.IsTopLevel: #这不是顶级文件系统,那么这是个应用文件系统, 应用文件系统根据Identifier来匹配
            if app_id == '': 
                continue
            if fsIdentifer != app_id:
                continue
            ds.ApplicationsManager.AddTag(name, app_id)  # 更新应用管理器的标记
        nodes = fs.Search(pattern)  # 根据正则表达式,在文件系统节点,查找匹配的子节点
        nodes = Enumerable.ToList[Node](nodes) #这是c#的泛型List<T>
        if nodes.Count > 0:
            firstTime = True
            prog = progress.GetSubProgress(categories.ToString())
            if prog == None:
                prog = TaskProgress(categories.ToString(),categories)
                progress.AddSubTask(prog)
            prog.Reset()
            prog.Report(1,'正在分析{0}'.format(descrip))        
            for node in list(nodes):
                if firstTime == True:
                    TraceService.Trace(TraceLevel.Info, "[FS:{0}]正在解析{1}".format(fs.Name, descrip))
                    firstTime = False
                try:
                    time_start = time.time()
                    TraceService.Trace(TraceLevel.Debug, "开始解析: {0}  节点: {1}".format(descrip, node.AbsolutePath))
                    parser_results = func(node, extract_deleted, extract_source)
                    parser_results.Categories = categories
                    TraceService.Trace(TraceLevel.Debug, "解析完毕: {0}  耗时: {1}秒  节点: {2}".format(descrip, time.time() - time_start, node.AbsolutePath))
                    results += parser_results
                    if len(app_id) > 0:
                        update_app_model(parser_results, installed_apps, app_id)
                    ds.Add(parser_results)
                except:
                    traceback.print_exc()
                    
                    TraceService.Trace(TraceLevel.Error, "解析出错: {0}".format(descrip))
            prog.Report(100,'分析{0}完成'.format(descrip))
            prog.Done()
    return results

def run(ds,extract_deleted,progress,canceller): 
    """
    插件的主入口
    ds: DataStore : 对象实例,包含当前宿主进程中.Net代码方所有的数据模型和对象
    extract_deleted : 是否解析删除记录
    extract_source : 是否释放源数据(保留)
    progress : IDescriptiveProgress 进度反馈和控制
    canceller : 取消操作
    """
    results = ParserResults()

    if not ds:
        ds = DataStore()
    if not progress:
        progress = TaskProgress('',DescripCategories.None,1)
    apps_by_identity = create_apps_dictionary(ds)
    for fs in list(ds.GetAllFileSystems()):
        results += decode_nodes(fs, extract_deleted,False, apps_by_identity)
    return results




