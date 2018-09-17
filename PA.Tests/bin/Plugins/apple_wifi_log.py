#coding=utf-8
from collections import defaultdict
import time
import os
import PA_runtime
from PA_runtime import *
import linecache 


def analyze_wifilog(node, extractDeleted, extractSource):
    """
    解析wifi日志文件
    """
    pr = ParserResults()
    files_list = node.Children
    for   file in files_list:
        if file.Type == NodeType.File:
            results = parse(file)
            if results:
                pr.Models.AddRange(results)
    pr.Build("WiFi")
    return pr


def parse(node): 
    data = defaultdict(list)
    models = []

    file = correct_isvilid_path(node)
    line_lists = linecache.getlines(file)
    for i in range(len(line_lists)):
        line = linecache.getline(file, i)
        
        if line.find("didUpdateLocations") != -1:
            dicts = {}
            wireless = WirelessConnection()
            coord = Coordinate()
            lat_index = line.find("latitude=")
            lon_index = line.find("longitude=")
            source_index = line.find("source=")

            if lat_index != -1:
                dicts["latitude"] = line[lat_index+10:lat_index+19]
            if lon_index != -1:
                dicts["longitude"] = line[lon_index+11:lon_index+20]
            dicts["time"] = _toUnixTime(line[:23].rstrip())
            if dicts["longitude"]:
                coord.Longitude.Value = float(dicts["longitude"])
            if dicts["latitude"]:
                coord.Latitude.Value = float(dicts["latitude"])
            if dicts["time"]:
                wireless.Time.Value = TimeStamp.FromUnixTime(dicts["time"])
                wireless.Position.Value = coord

            if source_index != -1:
                dicts["source"] = line[source_index+7:].strip()
                if dicts["source"]  == "Cell":
                    wireless.WirelessType.Value = WirelessType.MobileNetwork 
                    models.append(wireless)
                    continue
                elif dicts["source"] == "GPS":
                    wireless.WirelessType.Value = WirelessType.GPS
                    models.append(wireless)
                    continue
                elif dicts["source"] == "WiFi":
                    wireless.WirelessType.Value = WirelessType.Wifi 
                    
                    k = i
                    while k >= 0:
                        k -= 1
                        find_line = linecache.getline(file, k)
                        if find_line.find("didUpdateLocations") != -1:
                            index = line.find("source=")
                            if index != -1:
                                dicts["source"] = line[index+7:].strip()
                                if dicts["source"] in ["Cell", "GPS"]:
                                    models.append(wireless)
                                    break
                        elif find_line.find("Already connected to ") != -1:
                            index = find_line.find("Already connected to ")
                            if index != -1 and wireless.ConnectionName.Value is None:
                                wireless.ConnectionName.Value = find_line[index+len("Already connected to"):-1]
                        
                        elif find_line.find("IP Address:") != -1:
                            if find_line.find("Default") == -1 and find_line.find("Route") == -1:
                                index = find_line.find("IP Address:")
                                if index != -1 and wireless.IPAddress.Value is None:
                                    wireless.IPAddress.Value = find_line[index+11:].strip()
                            
                        elif find_line.find("Router IP Address:") != -1:
                            index = find_line.find("Router IP Address:")
                            if index != -1 and wireless.RouterIPAddress.Value is None:
                                wireless.RouterIPAddress.Value = find_line[index+18:].strip()
                            
                        elif find_line.find("Default Gateway IP Address:") != -1:
                            index = find_line.find("Default Gateway IP Address:")
                            if index != -1 and wireless.DefaultGateway.Value is None:
                                wireless.DefaultGateway.Value = find_line[index+27:].strip()
                    
                        elif find_line.find("Router MAC Address:") != -1:
                            index = find_line.find("Router MAC Address:")
                            if index != -1 and wireless.RouterMacAddress.Value is None:
                                wireless.RouterMacAddress.Value = find_line[index+len("Router MAC Address:"):].strip()

                    models.append(wireless)
                    continue
    return models  



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