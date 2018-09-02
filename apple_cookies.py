#coding=utf-8
import os
import PA_runtime
from PA_runtime import *

"""
解析二进制Cookie文件(*.binarycookies)
"""
def analyze_cookies(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(CookieParser(node, extractDeleted, extractSource).Parse())
    return pr

def execute(node,extracteDeleted):
    return analyze_cookies(node,extracteDeleted,False)

class CookieParser(object):
    def __init__(self, node, extractDeleted, extractSource):        
        self.node = node
        self.extractDeleted = extractDeleted
        self.extractSource = extractSource
    
    def Parse(self):                
        results = []
        results += self.analyze_cookiesDB()
        results += self.analyze_cookiesPlist()
        results += self.analyze_binarycookies()
        return results

    def analyze_cookiesDB(self):        
        db = SQLiteParser.Tools.GetDatabaseByPath(self.node, "com.apple.itunesstored.2.sqlitedb", "cookies")
        if db is None:
            return []
        results = []
        ts = SQLiteParser.TableSignature("cookies")
        if self.extractDeleted:
            SQLiteParser.Tools.AddSignatureToTable(ts, "expire_time", SQLiteParser.Tools.SignatureType.Int, SQLiteParser.Tools.SignatureType.Byte)
            SQLiteParser.Tools.AddSignatureToTable(ts, "user", SQLiteParser.Tools.SignatureType.Int, SQLiteParser.Tools.SignatureType.Byte)
            ts["domain"] = ts["value"] = ts["name"] = SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
        for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
            c = Cookie()
            c.RelatedApplication.Value = "iTunes"
            c.Deleted = rec.Deleted
            SQLiteParser.Tools.ReadColumnToField(rec, "name", c.Name, self.extractSource)
            SQLiteParser.Tools.ReadColumnToField(rec, "value", c.Value, self.extractSource)
            SQLiteParser.Tools.ReadColumnToField(rec, "domain", c.Domain, self.extractSource)                    
            try:
                if not IsDBNull(rec["expire_time"].Value) and rec["expire_time"].Value > 0:            
                    SQLiteParser.Tools.ReadColumnToField[TimeStamp](rec, "expire_time", c.Expiry, self.extractSource, lambda x: TimeStamp(TimeStampFormats.GetTimeStampEpoch1Jan2001(x),True))            
            except:
                pass
            if c not in results:
                results.append(c)
        return results

    def analyze_cookiesPlist(self):        
        files = [self.node.GetByPath("Cookies.plist"), self.node.GetByPath("com.apple.itunesstored.plist")]
        results = []  
        for f in files:
            if f is None or f.Data is None:
                continue
            try:
                plist = PList()
                p = plist.Parse(f.Data)
            except SystemError:
                continue
            if p is None:
                continue                
            for cookieEntry in p[0].Value:
                c = Cookie()
                related_app = self.find_RelatedApplication(f)
                if related_app is not None:
                    c.RelatedApplication.Value = related_app
                c.Deleted = DeletedState.Intact
                if 'Name' in cookieEntry:
                    c.Name.Value = cookieEntry['Name'].Value
                    if self.extractSource:
                        c.Name.Source = MemoryRange(cookieEntry['Name'].Source)
                if 'Value' in cookieEntry:
                    c.Value.Value = cookieEntry['Value'].Value
                    if self.extractSource:
                        c.Value.Source = MemoryRange(cookieEntry['Value'].Source)
                if 'Domain' in cookieEntry:
                    c.Domain.Value = cookieEntry['Domain'].Value
                    if self.extractSource:
                        c.Domain.Source = MemoryRange(cookieEntry['Domain'].Source)
                if 'Path' in cookieEntry:
                    c.Path.Value = cookieEntry['Path'].Value
                    if self.extractSource:
                        c.Path.Source = MemoryRange(cookieEntry['Path'].Source)
                if 'Expires' in cookieEntry:
                    c.Expiry.Value = TimeStamp(cookieEntry['Expires'].Value, True)
                    if self.extractSource:
                        c.Expiry.Source = MemoryRange(cookieEntry['Expires'].Source)    
                if c not in results:        
                    results.append(c)        
        return results

    def binarycookies_get_cstring(self, string_buffer, string_offset = 0):
        return string_buffer[string_offset : string_buffer.index('\x00', string_offset)]

    def binarycookies_parse_cookie(self, f, c_offset, cookie_buffer):
        EXP_DATE_OFFSET         = 0x28
        LAST_ACCESS_DATE_OFFSET = 0x30
        SIZE_OF_TS              = 8

        url_offset, name_offset, path_offset, value_offset = struct.unpack('<iiii', cookie_buffer[0x10:0x20])    

        url              = (c_offset + url_offset, self.binarycookies_get_cstring(cookie_buffer[url_offset:]))
        name             = (c_offset + name_offset, self.binarycookies_get_cstring(cookie_buffer[name_offset:]))
        path             = (c_offset + path_offset, self.binarycookies_get_cstring(cookie_buffer[path_offset:]))
        value            = (c_offset + value_offset, self.binarycookies_get_cstring(cookie_buffer[value_offset:]))
        expiry_date = None
        last_access_date = None
        try:
            expiry_date  = (c_offset + EXP_DATE_OFFSET, TimeStamp(epoch.AddSeconds(struct.unpack('d', cookie_buffer[EXP_DATE_OFFSET: EXP_DATE_OFFSET + SIZE_OF_TS])[0]), True))
        except:
            TraceService.Trace(TraceLevel.Info, "invalid expiry date in cookies: "+ str(struct.unpack('d', cookie_buffer[EXP_DATE_OFFSET: EXP_DATE_OFFSET + SIZE_OF_TS])[0]));
        try:
            last_access_date = (c_offset + LAST_ACCESS_DATE_OFFSET, TimeStamp(epoch.AddSeconds(struct.unpack('d',cookie_buffer[LAST_ACCESS_DATE_OFFSET: LAST_ACCESS_DATE_OFFSET + SIZE_OF_TS])[0]), True))
        except:
            TraceService.Trace(TraceLevel.Info, "invalid last access date in cookies: "+ str(struct.unpack('d', cookie_buffer[LAST_ACCESS_DATE_OFFSET: LAST_ACCESS_DATE_OFFSET + SIZE_OF_TS])[0]))
       
        mem = f.Data    
        c = Cookie()
        related_app = self.find_RelatedApplication(f)
        if related_app is not None:
            c.RelatedApplication.Value = related_app
        c.Deleted = f.Deleted    
        c.Domain.Value          = url[1]    
        cloudURLs =  ['.login.live.com', 'i.instagram.com', 'instagram.com']
        if c.Domain.Value in cloudURLs:
            c.ModelLabels = Labels.CloudAccountPackage;
        c.Name.Value            = name[1]    
        c.Path.Value            = path[1]    
        c.Value.Value           = value[1]
        if expiry_date != None:
            c.Expiry.Value          = expiry_date[1]  
        if last_access_date != None:
            c.LastAccessTime.Value  = last_access_date[1]  

        if self.extractSource:
            c.Domain.Source         = mem.GetSubRange(url[0], len (url[1]))
            c.Name.Source           = mem.GetSubRange(name[0], len (name[1]))
            c.Path.Source           = mem.GetSubRange(path[0], len (path[1]))
            c.Value.Source          = mem.GetSubRange(value[0], len (value[1]))
            if expiry_date != None:
                c.Expiry.Source         = mem.GetSubRange(expiry_date[0], SIZE_OF_TS)
            if last_access_date != None:
                c.LastAccessTime.Source = mem.GetSubRange(last_access_date[0], SIZE_OF_TS)
        return c

    def find_RelatedApplication(self, f):
        curr = f.Parent
        while curr is not None:
            app_name = self.to_app_name(curr.Name)
            if app_name is not None:
                return app_name
            curr = curr.Parent
        return None

    def to_app_name(self, s):
        for ia in ds.Models[InstalledApplication]:
            if ia.Identifier.Value == s:
                if ia.Name.Value is not None:
                    return ia.Name.Value
                else:
                    return  ia.Identifier.Value
        return None
    
    def binarycookies_parse_page(self, page_offset, page_buffer, f):
        PAGE_TAG                = '\x00\x00\x01\x00'
        SIZE_OF_INT             = 4
        #check tag
        if page_buffer[:SIZE_OF_INT] != PAGE_TAG:
            return None

        cookies = []
        num_of_cookies = int(struct.unpack('<i', page_buffer[4:4 + SIZE_OF_INT])[0])
        for i in range(num_of_cookies):
            cookie_offset, = struct.unpack('<i', page_buffer[8 + (i * SIZE_OF_INT):8 + ((i + 1) * SIZE_OF_INT)])
            cookie_length, = struct.unpack('<q', page_buffer[cookie_offset:cookie_offset + 8])

            cookies.append(self.binarycookies_parse_cookie(f, page_offset + cookie_offset, page_buffer[cookie_offset:cookie_offset+cookie_length]))

        return cookies

    def analyze_binarycookies(self):
        MAGIC                   = 'cook'
        SIZE_OF_INT             = 4        
        SIZE_OF_LONG            = 8

        cookies = []
        
        for f in self.node.SearchNodesExactPath("Cookies.binarycookies"):            
            if f is None or f.Data is None:
                continue            
            f.Data.seek(0)
            file_buffer = f.Data.read()

            if file_buffer[:SIZE_OF_INT] != MAGIC:                
                continue
    
            num_of_pages = int(struct.unpack('>i', file_buffer[4:4 + SIZE_OF_INT])[0])
            page_offset = 8 + (SIZE_OF_INT * num_of_pages)
            for i in range(num_of_pages):
                pageLength = int(struct.unpack('>i', file_buffer[8 + (i * SIZE_OF_INT):8 + ((i + 1) * SIZE_OF_INT)])[0])
                cookies += self.binarycookies_parse_page(page_offset, file_buffer[page_offset:page_offset + pageLength], f)
                page_offset += pageLength        
        return cookies