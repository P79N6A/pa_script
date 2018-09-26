#coding=utf-8

from PA_runtime import *
import clr
try:
    clr.AddReference('bcp_gis')
    clr.AddReference('bcp_im')
    clr.AddReference('bcp_mail')
    clr.AddReference('bcp_weibo')
    clr.AddReference('bcp_extra')
except:
    pass
del clr
import bcp_gis
import bcp_im
import bcp_mail
import bcp_weibo
import uuid
import bcp_extra
from collections import defaultdict

IM_LIST    = ['1030001','1030027','1030028','1030035','1030036','1030038','1030043','1030044','1030045','1030046','1030047','1030048','1030049','1030050','1030051','1030052','1030053','1039999']
WEIBO_LIST = ['1330001','1330002','1330003','1330004','1330005','1330006','1339999']
MAP_LIST   = ['1440001','1440002','1440003','1440004','1440005','1449999','1440009','1449999']
MAIL_LIST  = ['01001','01002','01003','01004','01005','01006','01007','01999']
BROWER_LIST = ['1560001','1560002','1560003','1560004','1560005','1560006','1560007','1560008','1560009','1560010','1560011','1560012','1560013','1560014','1560015','1560016','1569999','1560017','1560018','1560019','1560020','1560021','1560022','1560023','1560024','1560025']


def run(target_id, bcp_path, ts_path, mountDir, app_lists):
    all_paths = bcp_extra.BCP.get_paths()
    for app in app_lists:
        if app in IM_LIST:
            for info in all_paths.get(app):
                try:
                    cache_db, software_type = info
                    ts_db = ts_path + "/" +str(uuid.uuid1()) + ".db"
                    bcp_im.GenerateBcp(bcp_path, mountDir, cache_db, ts_db, target_id, software_type)
                except Exception as e:
                    pass
            
        elif app in MAP_LIST:
            for info in all_paths.get(app):
                try:
                    cache_db, software_type = info
                    ts_db = ts_path + "/" +str(uuid.uuid1()) + ".db"
                    bcp_gis.BuildBCP(cache_db, ts_db, target_id, software_type)
                except Exception as e:
                    pass

        elif app in BROWER_LIST:
            for info in all_paths.get(app):
                cache_db, software_type = info
                ts_db = ts_path + "/" +str(uuid.uuid1()) + ".db"

        elif app in MAIL_LIST:
            for info in all_paths.get(app):
                cache_db, software_type = info
                ts_db = ts_path + "/" +str(uuid.uuid1()) + ".db"


        
            
    