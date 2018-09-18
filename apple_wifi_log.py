#coding=utf-8
from collections import defaultdict
import time
import os
import PA_runtime
from PA_runtime import *
import linecache
import clr
try:
    clr.AddReference('model_wifi')
except:
    pass
del clr
import model_wifi 


def analyze_wifilog(node, extractDeleted, extractSource):
    """
    解析wifi日志文件
    """
    pr = ParserResults()
    cache = ds.OpenCachePath("WiFiLogs")
    db_path = cache + "/wifilog.db"
    wifi_log = model_wifi.WIFILog()
    wifi_log.db_create(db_path)
    files_list = node.Children
    for file in files_list:
        if file.Type == NodeType.File:
            parse(file, wifi_log)
    wifi_log.db_close()
    generate = model_wifi.Generate(db_path)
    results = generate.get_models()
    if results:
        pr.Models.AddRange(results)
    pr.Build("WiFi")
    return pr


def parse(node, wifi_log):
    data = defaultdict(list)
    models = []

    file = correct_isvilid_path(node)
    line_lists = linecache.getlines(file)
    for i in range(len(line_lists)):
        line = linecache.getline(file, i)
        
        if line.find("didUpdateLocations") != -1:
            dicts = {}
            wifilog = model_wifi.WIFI()
            wifilog.sourceFile = node.AbsolutePath
            lat_index = line.find("latitude=")
            lon_index = line.find("longitude=")
            source_index = line.find("source=")

            if lat_index != -1:
                dicts["latitude"] = line[lat_index+10:lat_index+19]
            if lon_index != -1:
                dicts["longitude"] = line[lon_index+11:lon_index+20]
            dicts["time"] = _toUnixTime(line[:23].rstrip())
            if dicts["longitude"]:
                wifilog.Longitude = float(dicts["longitude"])
            if dicts["latitude"]:
                wifilog.Latitude = float(dicts["latitude"])
            if dicts["time"]:
                wifilog.Time = dicts["time"]
          
            if source_index != -1:
                dicts["source"] = line[source_index+7:].strip()
                if dicts["source"]  == "Cell":
                    wifilog.Type = dicts["source"] 
                    try:
                        wifi_log.db_insert_table_wifilog(wifilog)
                    except Exception as e:
                        print(e)
                    continue
                elif dicts["source"] == "GPS":
                    wifilog.Type = dicts["source"] 
                    try:
                        wifi_log.db_insert_table_wifilog(wifilog)
                    except Exception as e:
                        print(e)
                    continue
                elif dicts["source"] == "WiFi":
                    wifilog.Type = dicts["source"]
                    
                    k = i
                    while k >= 0:
                        k -= 1
                        find_line = linecache.getline(file, k)
                        if find_line.find("didUpdateLocations") != -1:
                            index = line.find("source=")
                            if index != -1:
                                dicts["source"] = line[index+7:].strip()
                                if dicts["source"] in ["Cell", "GPS"]:
                                    try:
                                        wifi_log.db_insert_table_wifilog(wifilog)
                                    except Exception as e:
                                        print(e)
                                    break
                        elif find_line.find("Already connected to ") != -1:
                            index = find_line.find("Already connected to ")
                            if index != -1 and wifilog.Network is None:
                                wifilog.Network = find_line.rstrip()[index+len("Already connected to "):-1]
                        
                        elif find_line.find("IP Address:") != -1:
                            if find_line.find("Default") == -1 and find_line.find("Route") == -1:
                                index = find_line.find("IP Address:")
                                if index != -1 and wifilog.IP_Address is None:
                                    wifilog.IP_Address = find_line[index+11:].strip()
                            
                        elif find_line.find("Router IP Address:") != -1:
                            index = find_line.find("Router IP Address:")
                            if index != -1 and wifilog.Router_IP_Address is None:
                                wifilog.Router_IP_Address = find_line[index+18:].strip()
                            
                        elif find_line.find("Default Gateway IP Address:") != -1:
                            index = find_line.find("Default Gateway IP Address:")
                            if index != -1 and wifilog.Default_Gateway_IP_Address is None:
                                wifilog.Default_Gateway_IP_Address = find_line[index+27:].strip()
                    
                        elif find_line.find("Router MAC Address:") != -1:
                            index = find_line.find("Router MAC Address:")
                            if index != -1 and wifilog.Router_MAC_Address is None:
                                wifilog.Router_MAC_Address = find_line[index+len("Router MAC Address:"):].strip()

                    try:
                        wifi_log.db_insert_table_wifilog(wifilog)
                    except Exception as e:
                        print(e)
                    continue
    wifi_log.db_commit()  



def _toUnixTime(stringtime):
    if stringtime:
        try:
            ts = time.strptime(stringtime, "%m/%d/%Y %H:%M:%S.%f")
            utime = time.mktime(ts)
            return utime
        except Exception as e:
            pass

def correct_isvilid_path(src_node):
    if src_node is None:
        return
    file_path, file_name = os.path.split(src_node.PathWithMountPoint)
    isvalid_string = ["\/", "\\", ":", "*", "?", "<", ">", "|"]
    if [s for s in isvalid_string if s in file_name]:
        cache = ds.OpenCachePath("Logs")
        des_file = os.path.join(cache, file_name.replace(":","_"))
        f = open(des_file, 'wb+')
        data = src_node.Data
        sz = src_node.Size
        f.write(bytes(data.read(sz)))
        f.close()
        return des_file
    else:
        return src_node.PathWithMountPoint