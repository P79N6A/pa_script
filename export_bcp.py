#coding=utf-8

import clr
try:
    clr.AddReference('bcp_gis')
    clr.AddReference('bcp_im')
    clr.AddReference('bcp_mail')
    clr.AddReference('bcp_weibo')
    clr.AddReference('bcp_browser')
    clr.AddReference('bcp_basic')
except:
    pass
del clr
import bcp_gis
import bcp_im
import bcp_mail
import bcp_weibo
import bcp_browser
import bcp_basic
import hashlib
import os
import traceback

IM_LIST    = ['1030001','1030027','1030028','1030035','1030036','1030038','1030043','1030044','1030045','1030046','1030047','1030048','1030049','1030050','1030051','1030052','1030053','1039999']
WEIBO_LIST = ['1330001','1330002','1330003','1330004','1330005','1330006','1339999']
MAP_LIST   = ['1440001','1440002','1440003','1440004','1440005','1449999','1440009','1449999']
MAIL_LIST  = ['01001','01002','01003','01004','01005','01006','01007','01999']
BROWER_LIST = ['1560001','1560002','1560003','1560004','1560005','1560006','1560007','1560008','1560009','1560010','1560011','1560012','1560013','1560014','1560015','1560016','1569999','1560017','1560018','1560019','1560020','1560021','1560022','1560023','1560024','1560025']


"""
bcp_path -- 文件拷贝的路径
ts_path -- bcp数据库暂存的目录
mountDir -- 挂载的位置
app_lists -- 需要生产bcp数据库应用的集合
"""

# caseDir = r"E:\iPhone 6_11.1.2_133217541373990_full(1)_0817\caches"

def get_support_apps(caseDirs):
    lists = []
    if caseDirs:
        for case in caseDirs:
            path = case + "\\caches\\tmp"
            if os.path.exists(path):
                apps = [ f for f in os.listdir(path) if os.path.isfile(os.path.join(path,f))]
                lists.append(apps)
            else:
                return None
        return lists
    return None

def run(target_id, bcp_path, case_path, mountDir, software_type):
    tmp_dir = case_path + "\\caches\\tmp\\"  
    ts_path = case_path + "\\caches\\tmp"   # bcp数据库生成位置
    software_path = tmp_dir + software_type
    if os.path.exists(software_path):
        ts_db = None
        bcp_path_list = []
        # IM类生产bcp数据库
        if software_type in IM_LIST:
            path_lists = read_path(software_path)
            if path_lists:
                for path in path_lists:
                    try:
                        ts_db = md5(path, ts_path)    
                        bcp_im.GenerateBcp(bcp_path, mountDir, path, ts_db, target_id, software_type).generate()
                        bcp_path_list.append(ts_db)
                    except Exception as e:
                        print(traceback.print_exc())
                return bcp_path_list

        # 地理位置类生产bcp数据库
        elif software_type in MAP_LIST:
            path_lists = read_path(software_path)
            if path_lists:
                for path in path_lists:
                    try:
                        ts_db = md5(path, ts_path)    
                        bcp_gis.BuildBCP(path, ts_db, target_id, software_type).genetate()
                        bcp_path_list.append(ts_db)
                    except Exception as e:
                        print(traceback.print_exc())
                return bcp_path_list

        # # 浏览器类生产bcp数据库
        elif software_type in BROWER_LIST:
            path_lists = read_path(software_path)
            if path_lists:
                for path in path_lists:
                    try:
                        ts_db = md5(path, ts_path)    
                        bcp_browser.GenerateBcp(bcp_path, path, ts_db, target_id, software_type).generate()
                        bcp_path_list.append(ts_db)
                    except Exception as e:
                        print(traceback.print_exc())
                return bcp_path_list

        # 邮件类生产bcp数据库
        elif software_type in MAIL_LIST:
            path_lists = read_path(software_path)
            if path_lists:
                for path in path_lists:
                    try:
                        ts_db = md5(path, ts_path)    
                        bcp_mail.GenerateBcp(bcp_path, path, ts_db, target_id, software_type, mountDir).generate()
                        bcp_path_list.append(ts_db)
                    except Exception as e:
                        print(traceback.print_exc())
                return bcp_path_list

        # 微博类生产bcp数据库
        elif software_type in WEIBO_LIST:
            path_lists = read_path(software_path)
            if path_lists:
                for path in path_lists: 
                    try:
                        ts_db = md5(path, ts_path)    
                        bcp_weibo.GenerateBcp(path, ts_db, target_id, software_type).generate()
                        bcp_path_list.append(ts_db)
                    except Exception as e:
                        print(traceback.print_exc())
                return bcp_path_list

    return None

def md5(cache_path, ts_path):
    m = hashlib.md5()   
    m.update(cache_path.encode(encoding = 'utf-8'))
    db_path = ts_path + "\\" + m.hexdigest() + ".db"
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except Exception as e:
            print("{0} remove failed!".format(db_path))
    return db_path

def read_path(path):
    lists = []
    with open(path, "r") as f:
        tmp_lists = f.readlines()
        lists = map(func, tmp_lists)
    return lists

def func(item):
    if item is not None:
        return item.replace("\n", "")

    

