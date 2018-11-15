#coding=utf-8
import os
import re
import PA_runtime
from PA_runtime import *
from System.Globalization import *
import clr
clr.AddReference('PNFA.iPhoneApps')
del clr

from PA.iPhoneApps.Parsers import IthmbParser 


def analyze_connections_from_plist(f, extractDeleted, extractSource):
    pr = ParserResults()
    results = set([])
    res = None    
    if f is None or f.Data is None:
        return
    try:
        p = PList.Parse(f.Data)    
        if p != None:
            top = p[0].Value
            if top == None:
                return
            if not 'Signatures' in top:
                return
            
            for signature in top['Signatures'].Value:            
                res = IPConnection ()
                res.Deleted = DeletedState.Intact
                if 'Timestamp' in signature:
                    res.TimeStamp.Value = TimeStamp(signature['Timestamp'].Value, True)
                    if extractSource:
                        res.TimeStamp.Source = MemoryRange(signature['Timestamp'].Source)
                if 'Identifier' in signature:                                
                    if 'RouterHardwareAddress' in signature['Identifier'].Value:
                        res.MACAddress.Value = signature['Identifier'].Value[-17:].upper()
                        if extractSource:
                            res.MACAddress.Source = MemoryRange(signature['Identifier'].Source)
                    elif 'RouterHardwareAddress' in signature['Signature'].Value:
                        res.MACAddress.Value = signature['Signature'].Value[-17:]
                        if extractSource:
                            res.MACAddress.Source = MemoryRange(signature['Signature'].Source)
                    if 'Cellular WAN' in signature['Identifier'].Value:
                        res.CellularWAN.Value = signature['Identifier'].Value[14:]
                        if extractSource:
                            res.CellularWAN.Source = MemoryRange(signature['Identifier'].Source)
                    elif 'Cellular WAN' in signature['Signature'].Value:
                        res.CellularWAN.Value = signature['Signature'].Value[14:]
                        if extractSource:
                            res.CellularWAN.Source = MemoryRange(signature['Signature'].Source)
                if 'Services' in signature:                
                    for service in signature['Services'].Value:                    
                        if 'IPv4' in service:
                            if 'Router' in service['IPv4'].Value:                                                        
                                res.RouterAddress.Value = service['IPv4'].Value['Router'].Value
                                if extractSource:
                                    res.RouterAddress.Source = MemoryRange(service['IPv4'].Value['Router'].Source)
                            if 'Addresses' in service['IPv4'].Value:                            
                                for address in service['IPv4'].Value['Addresses'].Value:
                                    if address.Value not in res.DeviceIP.Values:                                
                                        if extractSource:
                                            res.DeviceIP.Add (address.Value, MemoryRange(address.Source))
                                        else:
                                            res.DeviceIP.Add (address.Value)
                        if 'DNS' in service.Value:
                            if 'DomainName' in service['DNS'].Value:                            
                                res.Domain.Value = service['DNS'].Value['DomainName'].Value
                                if extractSource:   
                                    res.Domain.Source = MemoryRange(service['DNS'].Value['DomainName'].Source)
                            if 'ServerAddresses' in service['DNS'].Value:                             
                                for address in service['DNS'].Value['ServerAddresses'].Value:
                                    if address.Value not in res.DNSAddresses.Values:
                                        if extractSource:
                                            res.DNSAddresses.Add (address.Value, MemoryRange(address.Source))
                                        else:
                                            res.DNSAddresses.Add (address.Value)
                results.add(res)

        else:
            top = BPReader(f.Data).top
            if top == None:
                return

            if not 'Signatures' in top.Keys:
                    return                
            for signature in range(top['Signatures'].Length):            
                signature = top['Signatures'][signature]
                res = IPConnection ()
                res.Deleted = DeletedState.Intact
                if 'Timestamp' in signature.Keys:                
                    res.TimeStamp.Value = TimeStamp(signature['Timestamp'].Value, True)
                    if extractSource:
                        res.TimeStamp.Source = MemoryRange(signature['Timestamp'].Source)
                if 'Identifier' in signature.Keys:                
                    if 'RouterHardwareAddress' in signature['Identifier'].Value:
                        res.MACAddress.Value = signature['Identifier'].Value[-17:].upper()
                        if extractSource:
                            res.MACAddress.Source = MemoryRange(signature['Identifier'].Source)
                    elif 'RouterHardwareAddress' in signature['Signature'].Value:
                        res.MACAddress.Value = signature['Signature'].Value[-17:]
                        if extractSource:
                            res.MACAddress.Source = MemoryRange(signature['Signature'].Source)
                    if 'Cellular WAN' in signature['Identifier'].Value:
                        res.CellularWAN.Value = signature['Identifier'].Value[14:]
                        if extractSource:
                            res.CellularWAN.Source = MemoryRange(signature['Identifier'].Source)
                    elif 'Cellular WAN' in signature['Signature'].Value:
                        res.CellularWAN.Value = signature['Signature'].Value[14:]
                        if extractSource:
                            res.CellularWAN.Source = MemoryRange(signature['Signature'].Source)
                   

                if 'Services' in signature.Keys:                
                    for service in range(signature['Services'].Length):                    
                        service = signature['Services'][service]
                        if 'IPv4' in service.Keys:
                            if 'Router' in service['IPv4'].Keys:                            
                                res.RouterAddress.Value = service['IPv4']['Router'].Value
                                if extractSource:
                                    res.RouterAddress.Source = MemoryRange(service['IPv4']['Router'].Source)
                            if 'Addresses' in service['IPv4'].Keys:                            
                                for address in range(service['IPv4']['Addresses'].Length):                                
                                    address = service['IPv4']['Addresses'][address]
                                    if address.Value not in res.DeviceIP.Values:
                                        if extractSource:
                                            res.DeviceIP.Add (address.Value, MemoryRange(address.Source))
                                        else:
                                            res.DeviceIP.Add (address.Value)
                        if 'DNS' in service.Keys:
                            if 'DomainName' in service['DNS'].Keys:                            
                                res.Domain.Value = service['DNS']['DomainName'].Value
                                if extractSource:
                                    res.Domain.Source = MemoryRange(service['DNS']['DomainName'].Source)
                            if 'ServerAddresses' in service['DNS'].Keys:                            
                                for address in range(service['DNS']['ServerAddresses'].Length):                                
                                    address = service['DNS']['ServerAddresses'][address]                                
                                    if address.Value not in res.DNSAddresses.Values:
                                        if extractSource:
                                            res.DNSAddresses.Add (address.Value, MemoryRange(address.Source))
                                        else:
                                            res.DNSAddresses.Add (address.Value)
                results.add(res)

    except SystemError:
        bptree = BPReader.GetTree(f)
        if bptree and bptree.ContainsKey('Signatures'):
            for signature in bptree['Signatures'].Value:                
                res = analyze_signature(signature,extractDeleted,extractSource)
                res.Deleted = f.Deleted
                results.add(res)
    pr.Models.AddRange(results)
    pr.Build('网络连接')
    return pr

def analyze_startup_time(node, extract_deleted, extract_source):
    if node.Data is None:
        return
    FORMAT = "ddd MMM d HH:mm:ss yyyy"
    PATTERN = r"^(?:.*?)([0-9a-zA-Z :]*?) pid=.*?main: Starting Up$"
    node.Data.seek(0)
    if node.Deleted == DeletedState.Deleted and not extract_deleted:
        return
    data = node.Data.read()
    pr = ParserResults()
    for match in re.finditer(PATTERN, data, re.MULTILINE):
        ts_str = match.group(1) 
        ts_str = ts_str.replace('  ', ' ')                
        ts = DateTime.ParseExact(ts_str, FORMAT, CultureInfo.InvariantCulture)
        ts = TimeStamp(ts)
        if ts < TimeStamp.FromUnixTime(0):
            continue

        pe = PoweringEvent()
        pe.Deleted = node.Deleted
        pe.Event.Value = pe.PowerEventType.On
        pe.Element.Value = pe.PowerElementType.Device
        if extract_source:
            pe.TimeStamp.Init(ts, node.Data.GetSubRange(match.start(1), len(ts_str)))
        else:
            pe.TimeStamp.Value = ts
        pr.Models.Add(pe)
    pr.Build('开机记录')
    return pr

def analyze_permissions(node, extract_deleted, extract_source):
    pr = ParserResults()
    try:
        db = SQLiteParser.Tools.GetDatabaseByPath(node, '', 'access')
        if db is None:
            return
        installed_apps = ds.InstalledApps
        permissions_dict = {
            'kTCCServiceCalendar': '日历',
            'kTCCServicePhotos': '照片',
            'kTCCServiceAddressBook': '通讯录',
            'kTCCServiceMicrophone': '麦克风',
            'kTCCServiceReminders': '提醒',
            'kTCCServiceBluetooth': '蓝牙',
        }

        apps_dict = {}
        for app in installed_apps:
            if app.Identifier is not None:
                apps_dict[app.Identifier.Value] = app

        ts = SQLiteParser.TableSignature('access')
        if extract_deleted:
            ts['prompt_count'] = SQLiteParser.Signatures.NumericSet(8, 9)
            ts['client_type'] = SQLiteParser.Signatures.NumericSet(8)
            ts['allowed'] = SQLiteParser.Signatures.NumericSet(9)

        for rec in db.ReadTableRecords(ts, extract_deleted, True):
            if 'client' not in rec or 'service' not in rec or 'allowed' not in rec or rec['allowed'].Value != 1:
                continue
            app_id = rec['client'].Value if not IsDBNull(rec['client'])else None
            if app_id:
                if app_id not in apps_dict:
                    ServiceLog.Warning("Application %s in permission file %s was not found in installed applications" %
                                    (app_id, node.AbsolutePath))
                    continue
                if not IsDBNull(rec['service']):
                    perm = permissions_dict.get(rec['service'].Value)
                    if perm is not None:
                        source = MemoryRange(rec['service'].Source) if extract_source else None
                        apps_dict[app_id].Permissions.Add(perm)
    except:
        traceback.print_exc()
    return pr

def analyze_tethering(node, extract_deleted, extract_source):
    pr = ParserResults()
    if node is not None and node.ModifyTime:
        pr.DeviceInfoFields.Add(MetaDataField('LastActivationTime', str(node.ModifyTime),None,'Tethering'))
    pr.Build('个人热点')
    return pr

def GetIthmbWidth(name):
    pixMap = {
        "3303":(24,22),
        "3306":(39,39),
        "3309":(64,64),
        "3314":(125,125),
        "3319":(160,157),
        "3141":(160,158),
        "3041":(80,79),
        "4131":(240,240),
        "4031":(120,120),
        "4132":(64,64),
        "4032":(32,32),
        "4140":(336,332),
        "4040":(168,166)
    }
    return pixMap.get(name,(110,110))

def analyze_ithmb(node, extract_deleted, extract_source):
    pr = ParserResults()
    if node.Type != NodeType.File or node.Data is None or node.Data.Length <= 0:
        return pr
    m = re.match(r'(\d+)x(\d+).ithmb$', node.Name)
    if m is None:
        rows,cols = GetIthmbWidth(node.Name.split(".")[0])
    else:
        rowStr,colStr = m.groups()
        rows, cols = int(rowStr),int(colStr)
    ithmbParser = IthmbParser(node,rows,cols)
    pr = ithmbParser.Parse()
    pr.Build('缩略图')
    return pr

def analyze_bluetooth_from_plist(node, extractDeleted, extractSource):
    if node.Data is None or node.Data.Length <= 0:
        return 
    try:
        bp = BPReader(node.Data).top
    except:
        bp = None
    if bp is None:
        return
    pr = ParserResults()
    for mac in bp.Keys:        
        res = BluetoothDevice()
        res.Deleted = DeletedState.Intact if \
            (re.match('.*\.plist\.(.*)', node.AbsolutePath)) is None \
            else DeletedState.Deleted
        res.MACAddress.Value = mac
        if 'Name' in bp[mac].Keys:
            res.Name.Value = bp[mac]['Name'].Value
            if extractSource:
                res.Name.Source = MemoryRange(bp[mac]['Name'].Source)
                                
        pr.Models.Add(res)
    pr.Build('蓝牙信息')
    return pr