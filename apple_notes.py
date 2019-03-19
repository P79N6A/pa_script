#coding=utf-8
import os
import PA_runtime
from PA_runtime import *
from PA.InfraLib.Utils import ConvertHelper
import re
import zlib

def analyze_notes(node, extractDeleted, extractSource):
    pr = ParserResults()
    db = SQLiteParser.Database.FromNode(node)
    if db is None:
        return
    ts = SQLiteParser.TableSignature('ZNOTEBODY')
    if extractDeleted:
        ts['ZCONTENT'] = TextNotNull
        ts['Z_OPT'] = SQLiteParser.Signatures.NumericSet(1)

    body_dic = {}
    
    for record in db.ReadTableRecords(ts, extractDeleted):
        if IsDBNull(record['ZCONTENT'].Value):
            continue
        
        if record.Deleted == DeletedState.Intact:
            key = record['Z_PK'].Value
            if key not in body_dic:
                body_dic[key] = record['ZCONTENT']
        else:
            res = Note()
            res.Deleted = record.Deleted
            SQLiteParser.Tools.ReadColumnToField(record, "ZCONTENT", res.Body, extractSource)
            pr.Models.Add(res)

    attach_node = node.Parent.GetByPath('/attachments')
    attach_dic = {}
    ts = SQLiteParser.TableSignature('ZNOTEATTACHMENT')
    for rec in db.ReadTableRecords(ts, extractDeleted, True):
        try:
            cid = rec['ZCONTENTID'].Value if 'ZCONTENTID' in rec and not IsDBNull(rec['ZCONTENTID'].Value) else ''
            filename = rec['ZFILENAME'].Value if 'ZFILENAME' in rec and not IsDBNull(rec['ZFILENAME'].Value) else ''
            mimetype = rec['ZMIMETYPE'].Value if 'ZMIMETYPE' in rec and not IsDBNull(rec['ZMIMETYPE'].Value) else ''
            attach_id = rec['Z_PK'].Value if 'Z_PK' in rec and not IsDBNull(rec['Z_PK'].Value) else 0
            if cid is '' or attach_id == 0 or filename is '' or mimetype is '':
                continue
            if cid not in attach_dic.keys():
                attach_dic[cid] = ''
                dir_node = attach_node.Search('.*' + str(attach_id) + '$')
                if len(list(dir_node)) != 0:
                    file_node = list(dir_node)[0].Search('.*\..*')
                    if len(list(file_node)) != 0:
                        if re.findall('image', mimetype):
                            src = file_node[0].PathWithMountPoint
                            src = get_uri(src)
                            attach_dic[cid] = "<img src = '" + src.ToString() + "' width='100%'/>"
                        elif re.findall('video', mimetype):
                            src = file_node[0].PathWithMountPoint
                            src = get_uri(src)
                            attach_dic[cid] = "<video src = '" + src.ToString() + "' width='100%'/>"
            else:
                attach_dic[cid].append(attach_id)
        except:
            pass

    ts = SQLiteParser.TableSignature('ZNOTE')
    if extractDeleted:
        ts['ZTITLE'] = TextNotNull
        ts['ZBODY'] = IntNotNull
        ts['ZMODIFICATIONDATE'] = ts['ZCREATIONDATE'] = SQLiteParser.Signatures.NumericSet(4, 7)
        ts['Z_OPT'] = ts['ZCONTAINSCJK'] = ts['ZEXTERNALFLAGS'] = ts['ZDELETEDFLAG'] = SQLiteParser.Signatures.NumericSet(1)

    
    for record in db.ReadTableRecords(ts, extractDeleted, True):
        if not record['ZBODY'].Value in body_dic:
            continue
        if record['ZCREATIONDATE'].Value == 0 or record['ZMODIFICATIONDATE'].Value == 0:
            continue

        res = Note()
        res.Deleted = record.Deleted
        if not IsDBNull(record['ZTITLE'].Value):
            res.Title.Value = record['ZTITLE'].Value
            if extractSource:
                res.Title.Source = MemoryRange(record['ZTITLE'].Source)
        if not IsDBNull(record['ZSUMMARY'].Value):        
            res.Summary.Value = record['ZSUMMARY'].Value
            if extractSource:
                res.Summary.Source = MemoryRange(record['ZSUMMARY'].Source)
        if not IsDBNull(body_dic[record['ZBODY'].Value].Value):
            body = body_dic[record['ZBODY'].Value].Value
            cids = re.findall('cid:(.*?)"', body)
            for cid in cids:
                src = attach_dic[cid] if cid in attach_dic else ''
                pattern = '<object.*'+cid+'.*/object>'
                body = re.sub(pattern, src, body)
            res.Body.Value = body
            #res.Body.Value = body_dic[record['ZBODY'].Value].Value
            if extractSource:
                res.Body.Source = MemoryRange(body_dic[record['ZBODY'].Value].Source)
        if not IsDBNull(record['ZCREATIONDATE'].Value):
            try:
                res.Creation.Value = TimeStamp(epoch.AddSeconds(record['ZCREATIONDATE'].Value), True)
                if extractSource:
                    res.Creation.Source = MemoryRange(record['ZCREATIONDATE'].Source)
            except:
                pass
        if not IsDBNull(record['ZMODIFICATIONDATE'].Value):
            try:
                res.Modification.Value = TimeStamp(epoch.AddSeconds(record['ZMODIFICATIONDATE'].Value), True)
                if extractSource:
                    res.Modification.Source = MemoryRange(record['ZMODIFICATIONDATE'].Source)
            except:
                pass
        pr.Models.Add(res)
    try:
        ress = analyze_notestore(node, extractDeleted)
        for res in ress:
            pr.Models.Add(res)
    except:
        pass
    pr.Build('备忘录')
    return pr

def get_uri(path):
        if path.startswith('http') or len(path) == 0:
            return ConvertHelper.ToUri(path)
        else:
            return ConvertHelper.ToUri(path)

def analyze_old_notes(node, extractDeleted, extractSource):
    db = SQLiteParser.Database.FromNode(node)
    if db is None:
        return

    ts = SQLiteParser.TableSignature('note_bodies')
    if extractDeleted:
        ts['data'] = TextNotNull 

    body_dic = {}
    for record in db.ReadTableRecords(ts, extractDeleted):
        if IsDBNull(record['data'].Value):
            continue
        key = record['note_id'].Value        
        body_dic[key] = record['data']

    ts = SQLiteParser.TableSignature('Note')
    if extractDeleted:
        ts['summary'] = ts['title'] = TextNotNull
        ts['modification_date'] = ts['creation_date'] = SQLiteParser.Signatures.NumericSet(7)

    pr = ParserResults()

    for record in db.ReadTableRecords(ts, extractDeleted, True):
        if len(record) != 8:
            continue
        res = Note()
        res.Deleted = record.Deleted
        if not IsDBNull(record['title'].Value):
            res.Title.Value = record['title'].Value
            if extractSource:
                res.Title.Source = MemoryRange(record['title'].Source)
        if not IsDBNull(record['summary'].Value):
            res.Summary.Value = record['summary'].Value
            if extractSource:
                res.Summary.Source = MemoryRange(record['summary'].Source)
        if record['ROWID'].Value in body_dic:
            res.Body.Value = body_dic[record['ROWID'].Value].Value
            if extractSource:
                res.Body.Source = MemoryRange(body_dic[record['ROWID'].Value].Source)
        if not IsDBNull(record['creation_date'].Value) and record['creation_date'].Value > 0:
            res.Creation.Value = TimeStamp(epoch.AddSeconds(record['creation_date'].Value), True)
            if extractSource:
                res.Creation.Source = MemoryRange(record['creation_date'].Source)
        if not IsDBNull(record['modification_date'].Value) and record['modification_date'].Value > 0:
            res.Modification.Value = TimeStamp(epoch.AddSeconds(record['modification_date'].Value), True)
            if extractSource:
                res.Modification.Source = MemoryRange(record['modification_date'].Source)
        pr.Models.Add(res)
    try:
        ress = analyze_notestore(node, extractDeleted)
        for res in ress:
            pr.Models.Add(res)
    except:
        pass
    pr.Build('备忘录')
    return pr

def analyze_notestore(node, extractDeleted):
    '''
    ZICNOTEDATA记录备忘录的正文内容与备忘id，正文内容以十六进制保存为zip
    ZICCLOUDSYNCINGOBJECT记录备忘录的标题、附件、图片、文件夹等数据
    在ZICCLOUDSYNCINGOBJECT表中：
        ZACCOUNTTYPE字段不为空的数据为备忘录的用户数据
        ZFOLDERTYPE字段不为空的数据为文件夹数据
        ZSTATEMODIFICATIONDATE字段不为空的数据为设备迁移状态的数据（未解析）
        ZATTACHMENT字段不为空的数据为备忘录的附件数据
    '''
    try:
        notestore_node = node.FileSystem.Search('/group.com.apple.notes/NoteStore.sqlite$')
        if len(list(notestore_node)) == 0:
            notestore_node = node.FileSystem.Search('/NoteStore.sqlite$')
            if len(list(notestore_node)) == 0:
                return
        db = SQLiteParser.Database.FromNode(notestore_node[0])
        if db is None:
            return
        ts = SQLiteParser.TableSignature('ZICNOTEDATA')
        #建立pk与内容的映射关系
        pk2data = {}
        #建立id与内容的映射关系
        id2data = {}
        #建立pk与id的映射关系
        pk2id = {}
        #建立id与pk的映射关系
        id2pk = {}
        for record in db.ReadTableRecords(ts, extractDeleted, True):
            try:
                pk = _db_record_get_int_value(record, 'Z_PK')
                if pk == 0:
                    continue
                note_id = _db_record_get_int_value(record, 'ZNOTE')
                data = record['ZDATA'].Value if not IsDBNull(record['ZDATA'].Value) else []
                data = parse_gzip(data)
                pk2data[pk] = data
                id2data[note_id] = data
                pk2id[pk] = note_id
                id2pk[note_id] = pk
            except:
                pass
        ts = SQLiteParser.TableSignature('ZICCLOUDSYNCINGOBJECT')
        #建立note_id与备忘录信息的对应关系{noteid:{folder:folderid, create_date:data, modify_date:data, title:data}, ...}
        id2info = {}
        #获取附件id与附件信息的对应关系{attachid:data, ...}
        attachid2attachinfo = {}
        #获取附件id与noteid的对应关系{noteid:[attachid1, attachid2, ...], ...}
        noteid2attachid = {}
        #建立note与folder之间的对应{folderid:foldername, ...}
        folder_info = {}
        for record in db.ReadTableRecords(ts, extractDeleted, True):
            try:
                if not IsDBNull(record['ZNOTEDATA'].Value):
                    note_data = {}
                    note_id = _db_record_get_int_value(record, 'ZNOTEDATA')
                    folder_id = _db_record_get_int_value(record, 'ZFOLDER')
                    create_date = _db_record_get_int_value(record, 'ZCREATIONDATE1')
                    modify_date = _db_record_get_int_value(record, 'ZMODIFICATIONDATE1')
                    title = _db_record_get_string_value(record, 'ZTITLE1')
                    subject = _db_record_get_string_value(record, 'ZSNIPPET')
                    note_data['folder'] = folder_id
                    note_data['create_date'] = format_mac_timestamp(create_date)
                    note_data['modify_date'] = format_mac_timestamp(modify_date)
                    note_data['title'] = title
                    note_data['subject'] = subject
                    note_data['deleted'] = record.IsDeleted
                    if note_id in id2info:
                        id2info[note_id] = dict(id2info[note_id], **note_data)
                    else:
                        id2info[note_id] = note_data
                elif not IsDBNull(record['ZATTACHMENT'].Value):
                    attachment_id = _db_record_get_int_value(record, 'ZATTACHMENT')
                    attachment_name = _db_record_get_string_value(record, 'ZIDENTIFIER')
                    attachment_nodes = notestore_node[0].Parent.Search('/'+ attachment_name + '\..*$')
                    if len(list(attachment_nodes)) != 0 and attachment_id not in attachid2attachinfo:
                        attachid2attachinfo[attachment_id] = [attachment_nodes[0].AbsolutePath, attachment_nodes[0], attachment_name]
                elif not IsDBNull(record['ZFOLDERTYPE'].Value):
                    folder_pk = _db_record_get_int_value(record, 'Z_PK')
                    folder_name = _db_record_get_string_value(record, 'ZNESTEDTITLEFORSORTING')
                    folder_info[folder_pk] = folder_name
                elif not IsDBNull(record['ZNOTE'].Value):
                    attach_id = _db_record_get_int_value(record, 'Z_PK')
                    note_id = _db_record_get_int_value(record, 'ZNOTE')
                    if note_id in noteid2attachid:
                        noteid2attachid[note_id].append(attach_id)
                    else:
                        noteid2attachid[note_id] = [attach_id,]
            except Exception as e:
                print(e)
        models = []
        for key, value in id2info.items():
            note_id = pk2id[key]
            res = Note()
            res.Title.Value = value['title'] if 'title' in value else None
            res.Summary.Value = value['subject'] if 'subject' in value else None
            res.Body.Value = pk2data[key]
            res.Creation.Value = _get_timestamp(value['create_date'])
            res.Modification.Value = _get_timestamp(value['modify_date'])
            res.Folder.Value = folder_info[value['folder']]
            res.SourceFile.Value = notestore_node[0].AbsolutePath.replace('/', '\\')
            res.Deleted = _convert_deleted_status(value['deleted'])
            try:
                if note_id in noteid2attachid:
                    for attach_id in noteid2attachid[note_id]:
                        if attach_id in attachid2attachinfo:
                            attachment = Attachment()
                            attachment.Uri.Value = ConvertHelper.ToUri(attachid2attachinfo[attach_id][0])
                            attach_node = attachid2attachinfo[attach_id][1]
                            attach_name = attachid2attachinfo[attach_id][2]
                            attachment.Filename.Value = attach_name
                            src = '<img src="' + attach_node.PathWithMountPoint + '" width="80%"  height="80%" />'
                            res.Attachments.Add(attachment)
                            res.Body.Value += src
            except Exception as e:
                pass
            models.append(res)
        return models
    except Exception as e:
        print(e)

def parse_gzip(data):
    try:
        if not data:
            return None
        decompressed_data=zlib.decompress(data, 16+zlib.MAX_WBITS)
        start = decompressed_data.find('\x08\x00\x10\x00\x1a')+5
        end = decompressed_data.find('\x1a\x10\x0a\x04\x08')+1
        content = decompressed_data[start: end]
        start = content.find('\x12')+2
        content = content[start: end].replace('\xef\xbf\xbc', '')
        content = content.decode("utf-8", "ignore")
        return re.sub('[\\x00-\\x08\\x0b-\\x0c\\x0e-\\x1f]', '', content)
    except:
        return None

def _db_record_get_value(record, column, default_value=None):
    if not record[column].IsDBNull:
        return record[column].Value
    return default_value

def _db_record_get_string_value(record, column, default_value=''):
    if not record[column].IsDBNull:
        try:
            value = str(record[column].Value)
            #if record.Deleted != DeletedState.Intact:
            #    value = filter(lambda x: x in string.printable, value)
            return value
        except Exception as e:
            return default_value
    return default_value

def _db_record_get_int_value(record, column, default_value=0):
    if not record[column].IsDBNull:
        try:
            return int(record[column].Value)
        except Exception as e:
            return default_value
    return default_value

def _db_record_get_blob_value(record, column, default_value=None):
    if not record[column].IsDBNull:
        try:
            value = record[column].Value
            return bytearray(value)
        except Exception as e:
            return default_value
    return default_value

def format_mac_timestamp(mac_time, v = 10):
    """
    from mac-timestamp generate unix time stamp
    """
    try:
        date = 0
        date_2 = mac_time
        if mac_time < 1000000000:
            date = mac_time + 978307200
        else:
            date = mac_time
            date_2 = date_2 - 978278400 - 8 * 3600
        s_ret = date if v > 5 else date_2
        return int(s_ret)
    except:
        return None

def _convert_deleted_status(deleted):
    if deleted is None:
        return DeletedState.Unknown
    else:
        return DeletedState.Intact if deleted == 0 else DeletedState.Deleted

def _get_timestamp(timestamp):
    try:
        if isinstance(timestamp, (long, float, str, Int64)) and len(str(timestamp)) > 10:
            timestamp = int(str(timestamp)[:10])
        if isinstance(timestamp, int) and len(str(timestamp)) == 10:
            ts = TimeStamp.FromUnixTime(timestamp, False)
            if not ts.IsValidForSmartphone():
                ts = None
            return ts
    except:
        return None