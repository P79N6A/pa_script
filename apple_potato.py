#coding:utf-8
#   Author:C37R
#   脚本分析了淘宝app的账号、聊天、app日志、搜索和APP推荐内容
#   具体分析详见分析说明
#   
import clr
clr.AddReference('System.Data.SQLite')
clr.AddReference('Base3264-UrlEncoder')
try:
    clr.AddReference('model_im')
    clr.AddReference('unity_c37r')
    clr.AddReference('apple_telegram')
except:
    pass
del clr
from PA_runtime import *
import System.Data.SQLite as sql
import apple_telegram
import model_im
import unity_c37r
import System.Data.SQLite as sql

POTATO_V = 1

def judge_node(root):
    node = root.Parent.Parent.Parent
    print node.PathWithMountPoint
    v_node = node.GetByPath('Documents/version.txt')
    if v_node is None:
        print('wrong node!')
        return None
    return node

def parse_potato(root, extract_deleted, extract_source):
    node = judge_node(root)
    container_node = node.Parent.Parent.Parent
    container_node = container_node.GetByPath('Data/Application')
    try:
        if container_node is None:
            print('lost container node')
            raise IOError('E')
        nodes = container_node.Search('Library/Preferences/org.potatochat.PotatoEnterprise.plist$')
        if len(nodes) > 1:
            print('multi target hitted,parser exits')
            raise IOError('E')
        container_node = nodes[0].Parent.Parent.Parent
        if node is None:
            print('''can't find group node''')
            raise IOError('E')
        res = []
        try:
            t = apple_telegram.Telegram(node, container_node, False, False, 1)
            if t.need_parse:
                result = t.parse()
                t.im.db_insert_table_version(model_im.VERSION_KEY_DB, model_im.VERSION_VALUE_DB)
                t.im.db_insert_table_version(model_im.VERSION_KEY_APP, 1)
                t.im.db_commit()
                t.im.db_close()
            models = model_im.GenerateModel(t.cache + '/{}.C37R'.format(t.hash_code)).get_models()
            nameValues.SafeAddValue('1030063', t.cache + '/{}.C37R'.format(t.hash_code))
            res.extend(models)
        except:
            traceback.print_exc()
            if canceller.IsCancellationRequested:
                raise IOError('E')
        mlm = ModelListMerger()
        pr = ParserResults()
        pr.Categories = DescripCategories.Potato
        pr.Models.AddRange(list(mlm.GetUnique(res)))
        pr.Build('Potato')
    except Exception as e:
        print(e)
        pr = ParserResults()
    return pr