# coding=utf-8

from PA_runtime import *


def convert_to_timestamp(timestamp):
    if len(str(timestamp)) == 13:
        timestamp = int(str(timestamp)[0:10])
    elif len(str(timestamp)) != 13 and len(str(timestamp)) != 10:
        timestamp = 0
    elif len(str(timestamp)) == 10:
        timestamp = timestamp
    ts = TimeStamp.FromUnixTime(timestamp, False)
    if not ts.IsValidForSmartphone():
        ts = None
    return ts


def analyze_startup_time(node, extract_deleted, extract_source):
    models = []
    if node is None:
        return
    for item in node.Children:
        try:
            name = "SYSTEM_BOOT@"
            if not item.Name.startswith(name):
                continue
            index = len(name)
            pe = PoweringEvent()
            pe.Deleted = item.Deleted
            pe.Event.Value = pe.PowerEventType.On
            pe.Element.Value = pe.PowerElementType.Device
            ts = convert_to_timestamp(item.Name[index: item.Name.index(".")])
            pe.TimeStamp.Value = ts
            pe.Source.Value = "开机记录"
            models.append(pe)
        except:
            pass
    return models


def analyze_restart_time(node, extract_deleted, extract_source):
    models = []
    if node is None:
        return
    for item in node.Children:
        try:
            name = "SYSTEM_RESTART@"
            if not item.Name.startswith(name):
                continue
            index = len(name)
            pe = PoweringEvent()
            pe.Deleted = item.Deleted
            pe.Event.Value = pe.PowerEventType.Reset
            pe.Element.Value = pe.PowerElementType.Device
            ts = convert_to_timestamp(item.Name[index: item.Name.index(".")])
            pe.TimeStamp.Value = ts
            pe.Source.Value = "关机记录"
            models.append(pe)
        except:
            pass
    return models


def analyze_recovey_time(node, extract_deleted, extract_source):
    models = []
    if node is None:
        return
    for item in node.Children:
        try:
            name = "SYSTEM_RECOVERY_LOG@"
            if not item.Name.startswith(name):
                continue
            index = len(name)
            pe = PoweringEvent()
            pe.Deleted = item.Deleted
            pe.Event.Value = pe.PowerEventType.Recovery
            pe.Element.Value = pe.PowerElementType.Device
            ts = convert_to_timestamp(item.Name[index: item.Name.index(".")])
            pe.TimeStamp.Value = ts
            pe.Source.Value = "系统恢复记录"
            models.append(pe)
        except:
            pass
    return models


def analyze_system_log(node, extract_deleted, extract_source):
    pr = ParserResults()
    results = []
    results.extend(analyze_startup_time(node, extract_deleted, extract_source))
    results.extend(analyze_restart_time(node, extract_deleted, extract_source))
    results.extend(analyze_recovey_time(node, extract_deleted, extract_source))
    if results:
        pr.Models.AddRange(results)
    return pr
