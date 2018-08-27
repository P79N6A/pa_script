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
try:
    import apple_exts    
    from apple_exts import FIND_BY_APPS_NODES_EXTS,FIND_BY_RGX_NODES_EXTS,run_apple_exts
except:
    pass

APP_FILTERS =[]
if 'TestNodes' in locals():
    APP_FILTERS.extend(TestNodes)

from System.Linq import Enumerable
from PA_runtime import *
from apple_ab import analyze_addressbook
from apple_notes import analyze_notes,analyze_old_notes
from apple_locations import analyze_locations,analyze_frequent_locations,analyze_apple_maps
from apple_locations import analyze_locations_from_deleted_photos
from apple_calenders import analyze_calender
from apple_safari import analyze_safari
from apple_mails import analyze_emails
from apple_recents import analyze_recents
from apple_cookies import analyze_cookies
from apple_calls import analyze_call_history
from apple_sms import analyze_smss
from apple_wechat import analyze_wechat
from apple_qq import analyze_qq
from apple_qqmail import analyze_qqmail
from apple_sogoumap import analyze_sogoumap
from apple_baidumap import analyze_baidumap
from apple_gaodemap import analyze_gaodemap
from apple_tencentmap import analyze_tencentmap
from apple_neteasemail import analyze_neteasemail
from PA.InfraLib.Services import IApplicationService,ServiceGetter


"""
根据正则表达式匹配解析的应用请在此节点下配置
"""
FIND_BY_RGX_NODES = [
    ('/DB/MM\.sqlite$', analyze_wechat, "Wechat","微信",DescripCategories.Wechat),
    ("/Library/CallHistoryDB/CallHistory\.storedata$", analyze_call_history, "Calls", "通话记录(系统)",DescripCategories.Calls),#新版本数据库兼容,别忘了老版本数据库!
    ('/PhotoData/Photos\.sqlite$', analyze_locations_from_deleted_photos, "PhotoDB","地理位置信息(已删除照片)",DescripCategories.Locations), #这里只处理照片(已删除)的地理位置信息
    ("/Library/Cookies$", analyze_cookies, "Cookies","Cookies",DescripCategories.Cookies), 
    ("/Mail$", analyze_emails, "Mails","邮件(系统)",DescripCategories.Mails),
    ("/Library/Mail/Recents$", analyze_recents,"Recents","通讯录(Recents)",DescripCategories.Recents),	
    ("/Library/Safari$", analyze_safari, "Safari","Safari",DescripCategories.Safari),
    ("/Library/Calendar/Calendar\.sqlitedb$", analyze_calender, "Calendar","日历",DescripCategories.Calenders),
    ("/Library/Maps$", analyze_apple_maps, "Maps","地图(系统)",DescripCategories.Maps),
    ("/Library/Caches/com\.apple\.routined$", analyze_frequent_locations, "Locations","常去地点信息(系统)",DescripCategories.Locations),
    ("/Library/Caches/locationd/consolidated\.db$", analyze_locations, "Locations","地理位置信息(系统)",DescripCategories.Locations),
    ("/Library/Caches/locationd/cache\.db$", analyze_locations, "Locations","地理位置信息(缓存)",DescripCategories.Locations),
    ("/Library/Caches/locationd/cache_encryptedA\.db$", analyze_locations, "Locations","地理位置信息(缓存)",DescripCategories.Locations),
    ("/Library/Notes/notes\.sqlite$", analyze_notes, "Notes","备忘录",DescripCategories.Notes),
    ("/Library/Notes/notes\.db$", analyze_old_notes, "Notes","备忘录",DescripCategories.Notes),
    ("/AddressBook$", analyze_addressbook, "AddressBook","通讯录(系统)",DescripCategories.Contacts),
]

"""
根据应用的标识ID来匹配对应的解析函数
"""
FIND_BY_APPS_NODES = [
    ("com.tencent.mqq", analyze_qq, "QQ","QQ(简体)" ,DescripCategories.QQ),
    ("com.tencent.mqqjp", analyze_qq,"QQ", "QQ(日本)" ,DescripCategories.QQ),
    ("com.tencent.mqqi", analyze_qq, "QQ","QQ(国际)" ,DescripCategories.QQ),
    ("com.tencent.qqmail", analyze_qqmail, "qqMail", "QQ邮箱", DescripCategories.BaiduMap),
    ("com.sogou.map.app.Map", analyze_sogoumap, "SogouMap", "搜狗地图", DescripCategories.SogouMap),
    ("com.baidu.map", analyze_baidumap, "BaiduMap", "百度地图", DescripCategories.BaiduMap),
    ("com.tencent.sosomap", analyze_tencentmap, "TencentMap", "腾讯地图", DescripCategories.TencentMap),
    ("com.autonavi.amap", analyze_gaodemap, "AMap", "高德地图", DescripCategories.AMap),
    ("com.netease.mailmaster", analyze_neteasemail, "mailMaster", "网易邮箱大师", DescripCategories.BaiduMap),

]

if 'FIND_BY_APPS_NODES_EXTS' in locals():
    FIND_BY_APPS_NODES.extend(FIND_BY_APPS_NODES_EXTS)
if 'FIND_BY_RGX_NODES_EXTS' in locals():
    FIND_BY_RGX_NODES.extend(FIND_BY_APPS_NODES_EXTS)

def decode_apps(extract_deleted, extract_source, installed_apps):
    results = ParserResults()
    for app_id, func, name,descrip,categories in FIND_BY_APPS_NODES:
        if len(APP_FILTERS) > 0 and not name in APP_FILTERS:
            TraceService.Trace(TraceLevel.Debug, "由于app4tests.py配置策略,应用{0}将不会被解析".format(name))
            
            continue
        if app_id in installed_apps:
            prog = progress.GetSubProgress(categories.ToString())
            if prog == None:
                prog = TaskProgress(categories.ToString(),categories)
                progress.AddSubTask(prog)
            prog.Reset()
            prog.Report(1,'正在分析{0}'.format(descrip))        
            try:
                app = installed_apps[app_id]
                node = app.AppFileSysNode
                ds.ApplicationsManager.AddTag(name, app_id)
                time_start = time.time()
                TraceService.Trace(TraceLevel.Info, "正在解析应用{0}({1})  节点{2}".format(name, app_id,node.AbsolutePath))
                if len(inspect.getargspec(func).args) == 4:
                    groupPathNodes =  ds.GroupContainers
                    parser_results = func(node,groupPathNodes, extract_deleted, extract_source)
                else:
                    parser_results = func(node, extract_deleted, extract_source)
                parser_results.Categories = categories
                TraceService.Trace(TraceLevel.Debug, "解析完毕: {0}  耗时: {1}秒  节点: {2}".format(name, time.time() - time_start, node.AbsolutePath))
                results += parser_results
                update_app_model(parser_results, installed_apps, app_id)
                ds.Add(parser_results)
            except:
                traceback.print_exc()
            prog.Report(100,'分析{0}完成'.format(descrip))
            prog.Done()
    return results

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
        "Navitel": "su.navitel.app"
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
    
    # try:
    #     results += run_apple_exts(ds,extract_deleted,progress,canceller,apps_by_identity)
    # except:
    #     pass
        

    results += decode_apps(extract_deleted,False,apps_by_identity)
    for fs in list(ds.GetAllFileSystems()):
        if len(APP_FILTERS) > 0 and  "SMS" in APP_FILTERS:
            results += analyze_smss(fs,extract_deleted,False,apps_by_identity)
        results += decode_nodes(fs, extract_deleted,False, apps_by_identity)
    return results




