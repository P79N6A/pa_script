#coding=utf-8
__author__ = 'YangLiyuan'

import PA_runtime
import hashlib
import time as py_time

import clr
try:
    clr.AddReference('model_browser')
    clr.AddReference('bcp_browser')
except:
    pass

import model_browser
import bcp_browser

from PA_runtime import *

DEBUG = True
DEBUG = False

class SafariParser(object):
    ''' node: /Library/Safari$ '''
    def __init__(self, node, extractDeleted, extractSource):
        self.extractDeleted = extractDeleted
        self.extractSource = extractSource
        self.source = 'Safari'
        self.directory = node     
        self.SEARCHED_ITEMS_TS = []
 
    def parse(self):
        results = []
        results.extend(self.analyze_bookmarks())
        results.extend(self.searchedItems())
        results.extend(self.read_history_db())

        tempResults = self.read_history_plist()
        results.extend(self.history_readState(tempResults))
        

        return results		

    def analyze_bookmarks(self):
        db = SQLiteParser.Tools.GetDatabaseByPath(self.directory, 'Bookmarks.db')
        if db is None:
            return []
        results = []         # Intact & Deleted
        deleted_results = [] # Deleted
        ts = SQLiteParser.TableSignature('bookmarks')
        if self.extractDeleted:
            ts['title'] = ts['url'] = TextNotNull
            ts['type'] = SQLiteParser.Signatures.NumericSet(1,8,9)

        table = list(db.ReadTableRecords(ts, self.extractDeleted))
        table.sort()

        # 书签文件夹
        dirs = {}   # {dir_id: (bookmark_title, bookmark_title.source)}
        for record in table:
            if record['id'].Value == -1:
                continue
            if record['type'].Value == 1 and not IsDBNull(record['title'].Value):
                if not IsDBNull(record['parent'].Value) and record['parent'].Value in dirs:
                    parent = dirs[record['parent'].Value]
                    path = ("/".join([parent[0], record['title'].Value]), MemoryRange(list(record['title'].Source) + list(parent[1].Chunks)))
                else:
                    path = (record['title'].Value, MemoryRange(record['title'].Source))
                dirs[record['id'].Value] = path

        # 书签
        for record in table:
            if IsDBNull(record['title'].Value):
                continue
            if record['type'].Value == 0:
                if not self._is_url(record, 'url'):
                    continue
                bookmark = WebBookmark()
                bookmark.Deleted = record.Deleted                        
                bookmark.Source.Value = self.source
                if '\x00' in record['title'].Value:
                    continue            
                bookmark.Title.Value = record['title'].Value
                if self.extractSource:
                    bookmark.Title.Source = MemoryRange(record['title'].Source)
                if not IsDBNull(record['url'].Value):
                    bookmark.Url.Value = record['url'].Value
                    if self.extractSource:
                        bookmark.Url.Source = MemoryRange(record['url'].Source)
                if not IsDBNull(record['parent'].Value) and record['parent'].Value in dirs:
                    bookmark.Path.Value = dirs[record['parent'].Value][0]
                    if self.extractSource:
                        bookmark.Path.Source = dirs[record['parent'].Value][1]
                        
                if bookmark.Deleted == DeletedState.Intact and bookmark not in results:
                    results.append(bookmark)
                elif bookmark.Deleted == DeletedState.Deleted and bookmark not in deleted_results:
                    deleted_results.append(bookmark)
                    
        for del_bookmark in deleted_results:
            if del_bookmark not in results:
                results.append(del_bookmark)
        return results

    def read_history_plist(self):
        # 保存记录到字典(避免重复) - { url : VisitedPage }
        results = {}
        nodes = self.directory.Search("/History\\.plist$")
        for node in nodes:
            if node is None or node.Data is None or node.Data.Length <= 0:
                continue
            try:
                bp = BPReader(node.Data).top
            except:
                bp = None
            if bp is None:
                continue

            for i in range(bp['WebHistoryDates'].Length):
                page = bp['WebHistoryDates'][i]
                res = VisitedPage()
                res.Source.Value = self.source
                res.Deleted = DeletedState.Intact
                if 'title' in page.Keys:
                    res.Title.Value = page['title'].Value
                    if self.extractSource:
                        res.Title.Source = MemoryRange(page['title'].Source)
                res.Url.Value = page[''].Value
                if self.extractSource:
                    res.Url.Source = MemoryRange(page[''].Source)
                res.VisitCount.Value = page['visitCount'].Value
                if self.extractSource:
                    res.VisitCount.Source = MemoryRange(page['visitCount'].Source)
                res.LastVisited.Value = TimeStamp(epoch.AddSeconds(float(page['lastVisitedDate'].Value)), True)
                if self.extractSource:
                     res.LastVisited.Source = MemoryRange(page['lastVisitedDate'].Source)

                results[res.Url.Value] = res
        return results

    def history_readState(self, results):
        '''
        解析访问历史记录,打开的Tab
        ''' 
        node = self.directory.GetByPath('SuspendState.plist')
        if node is None or node.Data is None or node.Data.Length <= 0:
            return results.values()
        try:
            bp = BPReader(node.Data).top
        except:
            bp = None

        self.history_readState_helper(results, bp, 'SafariStateDocuments')
        self.history_readState_helper(results, bp, 'SafariStatePrivateDocuments')
        return results.values()

    def history_readState_helper(self, results, bp, SafariState):
        # bp: SuspendState.plist
        if bp is None or bp[SafariState] is None:
            return

        for i in range(bp[SafariState].Length):        
            page = bp[SafariState][i]
            res = VisitedPage()
            res.Source.Value = self.source
            res.Deleted = DeletedState.Intact

            if 'SafariStateDocumentTitle' in page.Keys:
                res.Title.Value = page['SafariStateDocumentTitle'].Value
                if self.extractSource:
                    res.Title.Source = MemoryRange(page['SafariStateDocumentTitle'].Source)

            if 'SafariStateDocumentURL' in page.Keys:
                res.Url.Value = page['SafariStateDocumentURL'].Value
                if self.extractSource:
                    res.Url.Source = MemoryRange(page['SafariStateDocumentURL'].Source)

            if 'SafariStateDocumentLastViewedTime' in page.Keys:
                res.LastVisited.Value = TimeStamp(epoch.AddSeconds(float(page['SafariStateDocumentLastViewedTime'].Value)), True)
                if self.extractSource:
                    res.Url.Source = MemoryRange(page['SafariStateDocumentLastViewedTime'].Source)

            if not results.has_key(res.Url.Value):
                results[res.Url.Value] = res

            if 'SafariStateDocumentBackForwardList' in page.Keys:
                BFList = page['SafariStateDocumentBackForwardList']
                if 'entries' in BFList.Keys and BFList['entries'] is not None:
                    for j in range(BFList['entries'].Length):
                        subPage = BFList['entries'][j]

                        subRes = VisitedPage()
                        subRes.Source.Value = self.source
                        subRes.Deleted = DeletedState.Intact

                        if '' in subPage.Keys:
                            subRes.Url.Value = subPage[''].Value
                            if self.extractSource:
                                subRes.Url.Source = MemoryRange(subPage[''].Source)

                        if 'title' in subPage.Keys:
                            subRes.Title.Value = subPage['title'].Value
                            if self.extractSource:
                                subRes.Title.Source = MemoryRange(subPage['title'].Source)

                        if not results.has_key(subRes.Url.Value):
                            results[subRes.Url.Value] = subRes

    def createIdToUrlDict(self,histDb):
        ''' 关联 '''
        IdToUrlDict = {}
        ts = SQLiteParser.TableSignature('history_items')
        if self.extractDeleted:
            SQLiteParser.Tools.AddSignatureToTable(ts, 'visit_count', SQLiteParser.Tools.SignatureType.Byte, SQLiteParser.Tools.SignatureType.Const1)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'should_recompute_derived_visit_counts', SQLiteParser.Tools.SignatureType.Const0, SQLiteParser.Tools.SignatureType.Const1, SQLiteParser.Tools.SignatureType.Byte)
        for rec in histDb.ReadTableRecords(ts, self.extractDeleted, True):
            if rec['id'].IsDBNull or rec['url'].IsDBNull or not self._is_url(rec, 'url'):
                continue
            id = rec['id'].Value
            if IdToUrlDict.has_key(id):
                continue
            IdToUrlDict[id] = rec['url'], rec.Deleted
        return IdToUrlDict

    def read_history_db(self):
        histDb = SQLiteParser.Tools.GetDatabaseByPath(self.directory, 'History.db')
        if histDb is None:
            return []
        IdToUrlDict = self.createIdToUrlDict(histDb)   

        visitsAndSearched = []
        ts = SQLiteParser.TableSignature('history_visits')
        if self.extractDeleted:
            SQLiteParser.Tools.AddSignatureToTable(ts, 'visit_time', SQLiteParser.Tools.SignatureType.Float)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'load_successful', SQLiteParser.Tools.SignatureType.Const0, SQLiteParser.Tools.SignatureType.Const1, SQLiteParser.Tools.SignatureType.Byte)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'http_non_get', SQLiteParser.Tools.SignatureType.Const0, SQLiteParser.Tools.SignatureType.Const1, SQLiteParser.Tools.SignatureType.Byte)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'synthesized', SQLiteParser.Tools.SignatureType.Const0, SQLiteParser.Tools.SignatureType.Const1, SQLiteParser.Tools.SignatureType.Byte)
        used_ids = set()
        for rec in histDb.ReadTableRecords(ts, self.extractDeleted, True):
            if IsDBNull(rec['history_item'].Value):
                continue
            histItemId = rec['history_item'].Value

            vp = VisitedPage()
            vp.Deleted = rec.Deleted
            vp.Source.Value = self.source

            if histItemId in IdToUrlDict:
                used_ids.add(histItemId)
                vp.Url.Init(IdToUrlDict[histItemId][0].Value,MemoryRange(IdToUrlDict[histItemId][0].Source) if self.extractSource else None)   
            if (rec['title'].Type == KType.String):
                SQLiteParser.Tools.ReadColumnToField(rec, 'title', vp.Title, self.extractSource)         
            
            vp.VisitCount.Value = 1
            ts = None
            if not rec['visit_time'].IsDBNull:
                try:
                    ts = TimeStamp(DateTimeOffset(TimeStampFormats.GetTimeStampEpoch1Jan2001Double(rec['visit_time'].Value)),True)                    
                    if ts.IsValidForSmartphone():
                        vp.LastVisited.Init(ts,MemoryRange(rec['visit_time'].Source) if self.extractSource else None)
                except:
                    pass

            if histItemId in IdToUrlDict:
                fieldUrl = Field[String](IdToUrlDict[histItemId][0].Value)
                fieldUrl.Init(IdToUrlDict[histItemId][0].Value,MemoryRange(IdToUrlDict[histItemId][0].Source))
                fieldTimeStamp = Field[TimeStamp](ts, None)
                fieldTimeStamp.Init(ts,MemoryRange(rec['visit_time'].Source) if self.extractSource else None)
                u = UrlParser(fieldUrl,fieldTimeStamp,rec.Deleted,self.source)

                searchedItems = u.GetSearchedItem()

                for s in searchedItems:
                    LinkModels(s,vp)
                    visitsAndSearched.append(s) 
                # 提取非 Safari  搜索框输入的 搜索关键字(SearchedItem) 
                _SearchedItem = self._convert_2_SearchedItem(vp)
                if _SearchedItem:
                    visitsAndSearched.append(_SearchedItem)

                visitsAndSearched.append(vp)
        # 删除数据
        for url, deleted in [IdToUrlDict[id] for id in IdToUrlDict if id not in used_ids]:
            vp = VisitedPage()
            vp.Deleted = deleted
            vp.Source.Value = self.source
            vp.VisitCount.Value = 1
            if type(url.Value) is str:
                vp.Url.Init(url.Value, MemoryRange(url.Source) if self.extractSource else None)
            visitsAndSearched.append(vp)
        return visitsAndSearched


    def _convert_2_SearchedItem(self, vp):
        ''' 提取非 Safari  搜索框输入的 搜索关键字(SearchedItem) '''
        if vp.LastVisited.Value and vp.LastVisited.Value not in self.SEARCHED_ITEMS_TS:
            _SearchedItem = model_browser.convert_2_SearchedItem(vp)
            if _SearchedItem:
                self.SEARCHED_ITEMS_TS.append(_SearchedItem.TimeStamp.Value)
            return _SearchedItem
                      
    def getHistoryItems(self, histDb, visits):
        
        histItems = []

        ts = SQLiteParser.TableSignature('history_items')
        if self.extractDeleted:
            SQLiteParser.Tools.AddSignatureToTable(ts, 'visit_count', SQLiteParser.Tools.SignatureType.Byte, SQLiteParser.Tools.SignatureType.Const1)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'daily_visit_counts', SQLiteParser.Tools.SignatureType.Blob)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'should_recompute_derived_visit_counts', SQLiteParser.Tools.SignatureType.Const0, SQLiteParser.Tools.SignatureType.Const1, SQLiteParser.Tools.SignatureType.Byte)
        for rec in histDb.ReadTableRecords(ts, self.extractDeleted, True):
            if not self._is_url(rec, 'url'):
                continue
            vp = VisitedPage()
            vp.Deleted = rec.Deleted
            vp.Source.Value = self.source

            SQLiteParser.Tools.ReadColumnToField(rec, 'url', vp.Url, self.extractSource)
            SQLiteParser.Tools.ReadColumnToField(rec, 'visit_count', vp.VisitCount, self.extractSource)
            
            id = rec['id'].Value
            if not IsDBNull(id) and id in visits:
                SQLiteParser.Tools.ReadColumnToField(visits[id], 'title', vp.Title, self.extractSource)
                if 'visit_time' in visits[id] and not IsDBNull(visits[id]['visit_time'].Value):
                    try:
                        ts = TimeStamp(epoch.AddSeconds(visits[id]['visit_time'].Value))
                        if ts.IsValidForSmartphone():
                            SQLiteParser.Tools.ReadColumnToField[TimeStamp](visits[id], 'visit_time', vp.LastVisited, self.extractSource, lambda ts: TimeStamp(epoch.AddSeconds(ts), True))
                    except:
                        pass
            
            histItems.append(vp)

        return histItems

    def getVisitsDict(self, histDb):
        visits = {}

        ts = SQLiteParser.TableSignature('history_visits')
        if self.extractDeleted:
            SQLiteParser.Tools.AddSignatureToTable(ts, 'visit_time', SQLiteParser.Tools.SignatureType.Float)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'load_successful', SQLiteParser.Tools.SignatureType.Const0, SQLiteParser.Tools.SignatureType.Const1, SQLiteParser.Tools.SignatureType.Byte)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'http_non_get', SQLiteParser.Tools.SignatureType.Const0, SQLiteParser.Tools.SignatureType.Const1, SQLiteParser.Tools.SignatureType.Byte)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'synthesized', SQLiteParser.Tools.SignatureType.Const0, SQLiteParser.Tools.SignatureType.Const1, SQLiteParser.Tools.SignatureType.Byte)
        for rec in histDb.ReadTableRecords(ts, self.extractDeleted, True):
            if IsDBNull(rec['history_item'].Value):
                continue

            histItemId = rec['history_item'].Value

            # 访问记录
            if histItemId not in visits or ('visit_time' in rec and rec['visit_time'].Value > visits[histItemId]['visit_time'].Value):
                visits[histItemId] = rec
        return visits

    def searchedItems(self):
        searchedItems = []
        node = self.directory.GetByPath('../Preferences/com.apple.mobilesafari.plist')
        if node is None or node.Data is None or node.Data.Length <= 0:           
            return searchedItems
        bp = BPReader.GetTree(node)
        if bp is None:
            return searchedItems

        if not bp.ContainsKey('RecentWebSearches'):
            return searchedItems
        for b in bp['RecentWebSearches'].Value.GetEnumerator():
            if b.ContainsKey('SearchString'):
                s = SearchedItem()
                s.Source.Value = self.source
                s.Deleted = b['SearchString'].Deleted
                s.Value.Init(b['SearchString'].Value,MemoryRange(b['SearchString'].Source) if self.extractSource else None)
                if b.ContainsKey('Date'):
                    s.TimeStamp.Init(TimeStamp(b['Date'].Value,True), MemoryRange(b['Date'].Source) if self.extractSource else None)
                    # SEARCHED_ITEMS_TS 用来去重
                    if s.TimeStamp.Value:
                        self.SEARCHED_ITEMS_TS.append(s.TimeStamp.Value)
                searchedItems.append(s)
        return searchedItems

    @staticmethod
    def _is_url(rec, *args):
        ''' 匹配 URL IP

        严格匹配
        :type rec:   rec
        :type *args: str
        :rtype: bool
        '''
        NON_PATTERN = r'[\x00-\x08\x0b-\x0c\x0e-\x1f]'
        URL_PATTERN = r'((http|ftp|https)://)(([a-zA-Z0-9\._-]+\.[a-zA-Z]{2,6})|([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}))(:[0-9]{1,4})*(/[a-zA-Z0-9\&%_\./-~-]*)?'
        IP_PATTERN  = r'^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$'
        for i in args:
            try:
                raw_str = rec[i].Value

                if re.search(NON_PATTERN, raw_str):
                    return False
                match_url = re.match(URL_PATTERN, raw_str)
                match_ip  = re.match(IP_PATTERN, raw_str)
                if not match_url and not match_ip:
                    return False
            except:
                exc()
                return False  
        return True    

def analyze_safari(node, extractDeleted, extractSource):
    """
	合并Safari解析
    解析 History.db 文件 (iOS8+)
    旧版本 history.plist通过 read_history_plist 来解析.
    """
    pr = ParserResults()
    res = SafariParser(node, extractDeleted, extractSource).parse()

    # 保存到中间数据库, 导出 BCP 
    Export2db(node, results_model = res).parse()     

    pr.Models.AddRange(res)
    pr.Build('Safari')
    if DEBUG:
        TraceService.Trace(TraceLevel.Warning, 'safari is finished !')
    return pr


#################################################
##                添加中间数据库                 ##
#################################################
VERSION_APP_VALUE = 1

def exc():
    if DEBUG:
        traceback.print_exc()
    else:
        pass

class Export2db(object):
    def __init__(self, node, results_model):
        self.mb = model_browser.MB()
        self.results_model = results_model
        self.cachepath = ds.OpenCachePath("Safari")
        hash_str = hashlib.md5(node.AbsolutePath.encode('utf8')).hexdigest()
        self.cache_db = self.cachepath + '\\{}.db'.format(hash_str)

        self.bookmark_id = 0

    def parse(self):
        try:
            if DEBUG or self.mb.need_parse(self.cache_db, VERSION_APP_VALUE):
                self.mb.db_create(self.cache_db) 
                self.parse_model()
                if not canceller.IsCancellationRequested:
                    self.mb.db_insert_table_version(model_browser.VERSION_KEY_DB, model_browser.VERSION_VALUE_DB)
                    self.mb.db_insert_table_version(model_browser.VERSION_KEY_APP, VERSION_APP_VALUE)
                self.mb.db_commit()
                self.mb.db_close()
            tmp_dir = ds.OpenCachePath('tmp')
            save_cache_path(bcp_browser.NETWORK_APP_OTHER, self.cache_db, tmp_dir)
        except:
            exc()

    def parse_model(self):
        '''  WebBookmark
                Title
                Url
                Path
                # TimeStamp
                # Position
                # PositionAddress 
                # LastVisited (timestamp)
                Deleted
                Source.Value

            VisitedPage
                Title.Value
                Url.Value
                VisitCount.Value
                LastVisited.Value 

            SearchedItem
                TimeStamp
                Value

                # Position
                # PositionAddress
                # ItemType
                # SearchResultCount
                # OwnerUserID
                # SearchResults
        '''
        for item in self.results_model:
            if item.I18N == "书签":      # WebBookmark
                bookmark = model_browser.Bookmark()
                bookmark.id         = self.bookmark_id
                self.bookmark_id += 1
                # bookmark.owneruser  = 
                # bookmark.time       =
                bookmark.title      = item.Title.Value
                bookmark.url        = item.Url.Value
                bookmark.source     = item.Source.Value
                bookmark.deleted    = 0 if item.Deleted == DeletedState.Intact else 1
                self.mb.db_insert_table_bookmarks(bookmark)

            elif item.I18N == "浏览记录":    # VisitedPage
                browser_record = model_browser.Browserecord()
                # browser_record.id       = 
                browser_record.name     = item.Title.Value
                browser_record.url      = item.Url.Value
                browser_record.datetime = self._convert_2_timestamp(item.LastVisited)
                browser_record.source   = item.Source.Value  
                browser_record.deleted  = 0 if item.Deleted == DeletedState.Intact else 1

                self.mb.db_insert_table_browserecords(browser_record)

            elif item.I18N == "搜索项":     # SearchedItem
                search_history = model_browser.SearchHistory()
                # search_history.id       = 
                search_history.name     = item.Value
                # search_history.url      = 
                search_history.datetime = self._convert_2_timestamp(item.TimeStamp)
                search_history.source   = item.Source.Value
                search_history.deleted  = 0 if item.Deleted == DeletedState.Intact else 1

                self.mb.db_insert_table_searchhistory(search_history)

    @staticmethod
    def _convert_2_timestamp(_timestamp):
        ''' "2018/8/15 15:27:20" -> 10位 时间戳 1534318040.0
            
        Args:
            _timestamp (str): email.TimeStamp.Value.Value.LocalDateTime
        Returns:
            (int/float): timastamp e.g. 1534318040.0
        '''
        try:
            if _timestamp.Value:
                format_time = _timestamp.Value.Value.LocalDateTime
                div_str = str(format_time)[4]
                if re.match(r'\d', div_str):
                    div_str = ''
                time_pattren = "%Y{div}%m{div}%d %H:%M:%S".format(div=div_str)
                ts = py_time.strptime(str(format_time), time_pattren)
                return py_time.mktime(ts)
            return 0
        except:
            exc()
            return 0   