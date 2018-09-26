#coding:utf-8

import struct
import traceback
import re
from PA_runtime import *
from System import Convert

QT_HEADER = '\x00\x00\x00\x14ftypqt  \x00\x00\x00\x00qt  '
QT_HEADER_LEN = 0x14
MD_HDLR_ATOM = '\x00'*8 + 'mdta' + '\x00'*12
UTF_8_TYPES = [1,4]
UTF_16_TYPES = [2,5]
INT_TYPES = [21,22]
FLOAT_TYPES = [23,24]
ATOM_HEADER_SIZE = 8 
ATOM_SIZE_FIELD_LENGTH = 4
LOCATION_RE_PATTERN = '([+-][0-9.]+)([+-][0-9.]+)?([+-][0-9.]+)?'
UDTA_TAG_TO_META_KEY = {"\xa9mak":"com.apple.quicktime.make", "\xa9day":"com.apple.quicktime.creationdate", "\xa9xyz":'com.apple.quicktime.location.ISO6709', "\xa9mod":'com.apple.quicktime.model', "\xa9swr":'com.apple.quicktime.software'}

def get_quicktime_md(fNode):
    if fNode.Data is None or fNode.Data.Length == 0 :
        return {}
    datamem = fNode.Data
    datamem.seek(0)
    fHeader = datamem.read(QT_HEADER_LEN)
    
    if not is_QT_file(fHeader):
        return {}
    offset = QT_HEADER_LEN
    moovOffset = QT_HEADER_LEN
    moovDataLen = 0
    moovDataOff = 0
    while True:
        datamem.seek(offset)
        atomHeadBuf = datamem.read(0x10)
        (atomSize, atomType, atomDataOffset) = parse_atom_header(atomHeadBuf)
        if atomSize == -1:
            return {}
        if atomType == 'moov':
            moovOffset = offset
            moovDataLen = atomSize
            moovDataOff = atomDataOffset
            break
        offset += atomSize + atomDataOffset

    if moovDataLen == 0:
        return {}
    buffer_index = moovOffset + moovDataOff
    datamem.seek(buffer_index)
    buf = datamem.read(moovDataLen)
    
    md_index = 0
    md = {}
    while md_index >= 0 and md_index < len(buf):
        meta_find = buf.find('meta',md_index+ATOM_HEADER_SIZE)-ATOM_SIZE_FIELD_LENGTH
        udta_find = buf.find('udta',md_index + ATOM_HEADER_SIZE) - ATOM_SIZE_FIELD_LENGTH
        if meta_find > 0  and (meta_find < udta_find or udta_find < 0):
            md_index = meta_find
        elif udta_find > 0 and (meta_find > udta_find or meta_find <0): 
            parse_udta(buf, udta_find, md, buffer_index)
            md_index = udta_find
            continue
        elif udta_find < 0 and meta_find < 0:
            break
        if md_index == -1-ATOM_SIZE_FIELD_LENGTH:
            break
        (md_size,t,ind) = parse_atom_header(buf, md_index)
        (isMD,ind) = verify_md(buf, ind)
        (s,t,ind) = find_next_atom_by_type(buf, ind, 'keys', md_index+md_size)
        if t=='not found':
            continue
        (keys,ind) = parse_key_list(buf, ind)
        (s,t,ind) = find_next_atom_by_type(buf, ind, 'ilst', md_index+md_size)
        if t == 'not found':
            continue
        ilst_index = ind
        ilst_size = s
        while ind < ilst_index+ilst_size:
            (val_size,key_index,val_ind) = parse_atom_header(buf, ind, True)
            (s,t,ind)=find_next_atom_by_type(buf,val_ind,'data',val_ind+val_size)
            if t=='not found':
                ind=val_ind+val_size
                continue
            if not buf[ind]=='\x00':
                ind=val_ind+val_size
                continue
            val_type=to_int(buf,ind)
            ind += 8
            s= val_ind+val_size-ind
            if (val_type in INT_TYPES or val_type in FLOAT_TYPES):
                if s<=8:
                    val=str(to_int(buf,ind,s,val_type in FLOAT_TYPES))
                else:
                    val=str (0)
            elif val_type in UTF_16_TYPES:
                val=buf[ind:ind+s].decode('utf-16')
            else:
                val=buf[ind:ind+s]
            if key_index > len(keys):
                key='no key'
            else:
                key=keys[key_index-1]
            add_val_to_dict(md,key,val)
            add_val_to_dict(md,key + '_source', (buffer_index + ind,s))
            ind += s
    return md

def parse_udta(buf, md_index, md, buffer_index):
    (udta_total_len, t, start_ind) = parse_atom_header(buf, md_index)
    ind = start_ind
    if t != 'udta' or udta_total_len == -1 or (udta_total_len + md_index) > len(buf):
        return
    while md_index + udta_total_len > ind:
        (a_len, t, ind) = parse_atom_header(buf, ind)
        if a_len == -1 or a_len >= udta_total_len:
            return
        key = t
        if UDTA_TAG_TO_META_KEY.has_key(t):
            key = UDTA_TAG_TO_META_KEY[t]
        val = buf[ind + 4: ind + a_len]
        add_val_to_dict(md, key, val)
        add_val_to_dict(md, key + '_source', (buffer_index + ind + 4, a_len - 4))
        ind = ind + a_len

def is_QT_file(filebuf):
    return filebuf[:20]==QT_HEADER

def add_val_to_dict(md,key,val):
    if not key in md.keys():
        md[key]=val

def find_next_atom_by_type(buf,index,typ,limit=-1):
    t=""
    s=0
    while not (t==typ or s==-1 or (limit>-1 and index>=limit) or index>=len(buf)):
        (s,t,index)=parse_atom_header(buf,index)
    if not t==typ:
        index=limit
        t='not found'
    return (s,t,index)

def parse_atom_header(buf,index = 0,type_as_int=False):
    if len(buf) < index + 8:
        return (-1, '', index)
    s = struct.unpack (">I", buf [index : index + 4]) [0]
    t=buf[index+4:index+8]
    if type_as_int:
        t = struct.unpack (">I", t) [0]
    if s<8:
        if s==1:
            newLen = struct.unpack (">Q", buf [index+ATOM_HEADER_SIZE : index+ATOM_HEADER_SIZE + 8]) [0]
            return (newLen-16,t,index+16)
        else:
            return (-1,'',index)
    return (s-8,t,index+8)

def to_int(buf,index=0,size=4,is_float=False):
    if len(buf)<index+size:
        return -1
    if is_float:
        return float(buf[index:index+size].encode('hex'),16)
    return int(buf[index:index+size].encode('hex'),16)

def verify_md(buf,index):
    (s,t,ind)=parse_atom_header(buf,index)
    if not t=='hdlr':
        return (False,index)
    return (buf[ind:ind+24]==MD_HDLR_ATOM,ind+s)

def parse_key_list(buf, ind):
    ind += 4
    keycount = to_int(buf,ind)
    ind += 4
    keys = []
    for i in xrange(keycount):
        key_size = to_int(buf,ind)
        if buf[ind+4:ind+8] == 'mdta':
           keys.append(buf[ind+8 : ind+key_size])
        ind += key_size
    return (keys,ind)

def parseLocation(loc):
    loc_match = re.match(LOCATION_RE_PATTERN, loc)
    if loc_match == None:
        return None
    return [x or '0' for x in loc_match.groups()]

def getFileLocation(f):
    filemd = get_quicktime_md(f)
    for key in filemd:
        if not key[-7:] == '_source':
            f.MetaData.Add(key, filemd [key], "Quicktime Metadata")
            mdDefGroup = MetaData.EXPORT_METADATA_GROUP_NAME
        if key == 'com.apple.quicktime.creationdate':
            f.MetaData.Add("Record Time", TimeStamp(Convert.ToDateTime(filemd[key]), True).ToString(), mdDefGroup)
        elif key == 'com.apple.quicktime.make':
            f.MetaData.Add ("Camera Make", filemd [key], mdDefGroup)
        elif key == 'com.apple.quicktime.model':
            f.MetaData.Add ("Camera Model", filemd [key], mdDefGroup)
        elif key == 'com.apple.quicktime.software':
            f.MetaData.Add ("Camera Software", filemd [key], mdDefGroup)
        elif key == 'com.apple.quicktime.camera.identifier':
            f.MetaData.Add ("Camera Identifier", filemd [key], mdDefGroup)
        elif key == 'com.apple.quicktime.comment':
            f.MetaData.Add ("User Comment", filemd[key], mdDefGroup)
        elif key == 'com.apple.quicktime.location.name':
            f.MetaData.Add ("Location Name", filemd[key], mdDefGroup)
        if key == 'com.apple.quicktime.location.ISO6709':
            locval = parseLocation(filemd[key])
            if locval is None:
                continue

            if 'com.apple.quicktime.location.ISO6709_source' in filemd:
                src_ind = filemd['com.apple.quicktime.location.ISO6709_source'][0]
                mdSrc = f.Data.GetSubRange(src_ind, sum(map(len, locval)))
            if mdSrc is None:
                msSrc = MemoryRange()
            f.MetaData.Add("Lat/Lon/Elev", "%s/%s/%s" % tuple(locval), mdSrc, mdDefGroup)

            cLoc = Location()
            cLoc.Deleted = f.Deleted
            cLoc.Category.Value = LocationCategories.MEDIA
            cLoc.Position.Value = Coordinate(float(locval[0]), float(locval[1]), float(locval[2]))
            cLoc.Name.Value = f.Name
            cLoc.Description.Value = key
            cLoc.SourceNode =  f #节点溯源
            cLoc.SourceFile.Value = f.AbsolutePath
            LinkModels(cLoc, f)
            
            if 'com.apple.quicktime.location.ISO6709_source' in filemd:
                src_ind = filemd['com.apple.quicktime.location.ISO6709_source'][0]
                cLoc.Position.Value.Latitude.Source = f.Data.GetSubRange(src_ind, len(locval[0]))
                src_ind += len(locval[0])
                cLoc.Position.Value.Longitude.Source = f.Data.GetSubRange(src_ind, len(locval[1]))
                if locval[2] is not None and len(locval[2]) > 1:
                    src_ind += len(locval[1])
                    cLoc.Position.Value.Elevation.Source = f.Data.GetSubRange(src_ind, len(locval[2]))

            if 'com.apple.quicktime.creationdate' in filemd:
                tsStr = filemd['com.apple.quicktime.creationdate']
                cLoc.TimeStamp.Value = TimeStamp(Convert.ToDateTime(tsStr), True)
                if 'com.apple.quicktime.creationdate' in filemd:
                    src = filemd['com.apple.quicktime.creationdate_source']
                    cLoc.TimeStamp.Source = f.Data.GetSubRange(src[0],src[1])

            return cLoc
    return None

def handleFile(f, lPALocations) :
    filemd = get_quicktime_md(f)
    for key in filemd:
        if not key[-7:] == '_source':
            f.MetaData.Add(key, filemd [key], "Quicktime Metadata")
            mdDefGroup = MetaData.EXPORT_METADATA_GROUP_NAME
        if key == 'com.apple.quicktime.creationdate':
            f.MetaData.Add("Record Time", TimeStamp(Convert.ToDateTime(filemd[key]), True).ToString(), mdDefGroup)
        elif key == 'com.apple.quicktime.make':
            f.MetaData.Add ("Camera Make", filemd [key], mdDefGroup)
        elif key == 'com.apple.quicktime.model':
            f.MetaData.Add ("Camera Model", filemd [key], mdDefGroup)
        elif key == 'com.apple.quicktime.software':
            f.MetaData.Add ("Camera Software", filemd [key], mdDefGroup)
        elif key == 'com.apple.quicktime.camera.identifier':
            f.MetaData.Add ("Camera Identifier", filemd [key], mdDefGroup)
        elif key == 'com.apple.quicktime.comment':
            f.MetaData.Add ("User Comment", filemd[key], mdDefGroup)
        elif key == 'com.apple.quicktime.location.name':
            f.MetaData.Add ("Location Name", filemd[key], mdDefGroup)
        if key == 'com.apple.quicktime.location.ISO6709':
            locval = parseLocation(filemd[key])
            if locval is None:
                continue

            if 'com.apple.quicktime.location.ISO6709_source' in filemd:
                src_ind = filemd['com.apple.quicktime.location.ISO6709_source'][0]
                mdSrc = f.Data.GetSubRange(src_ind, sum(map(len, locval)))
            if mdSrc is None:
                msSrc = MemoryRange()
            f.MetaData.Add("Lat/Lon/Elev", "%s/%s/%s" % tuple(locval), mdSrc, mdDefGroup)

            cLoc = Location()
            cLoc.Deleted = f.Deleted
            cLoc.Category.Value = LocationCategories.MEDIA;
            cLoc.Position.Value = Coordinate(float(locval[0]), float(locval[1]), float(locval[2]))
            cLoc.Name.Value = f.Name
            cLoc.Description.Value = key
            cLoc.SourceNode =  f #节点溯源
            LinkModels(cLoc, f)
            
            if 'com.apple.quicktime.location.ISO6709_source' in filemd:
                src_ind = filemd['com.apple.quicktime.location.ISO6709_source'][0]
                cLoc.Position.Value.Latitude.Source = f.Data.GetSubRange(src_ind, len(locval[0]))
                src_ind += len(locval[0])
                cLoc.Position.Value.Longitude.Source = f.Data.GetSubRange(src_ind, len(locval[1]))
                if locval[2] is not None and len(locval[2]) > 1:
                    src_ind += len(locval[1])
                    cLoc.Position.Value.Elevation.Source = f.Data.GetSubRange(src_ind, len(locval[2]))

            if 'com.apple.quicktime.creationdate' in filemd:
                tsStr = filemd['com.apple.quicktime.creationdate']
                cLoc.TimeStamp.Value = TimeStamp(Convert.ToDateTime(tsStr), True)
                if 'com.apple.quicktime.creationdate' in filemd:
                    src = filemd['com.apple.quicktime.creationdate_source']
                    cLoc.TimeStamp.Source = f.Data.GetSubRange(src[0],src[1])

            lPALocations.append(cLoc)

def analyze_quicktime_meta(node,extractDeleted,extractSource):
    pr = ParserResults()
    qtLocations = []
    res = node.Search('((.qt)|(.MOV))$')
    try:
        for f in res:
            canceller.ThrowIfCancellationRequested()
            if f.Type == NodeType.File:
                try: 
                    handleFile(f, qtLocations)
                except:
                    TraceService.Trace(TraceLevel.Error, traceback.format_exc())
    except:
        pass
        
    pr.Models.AddRange(qtLocations)
    pr.Build('地理位置')
    return pr

