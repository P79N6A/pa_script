#coding:utf-8
from collections import defaultdict
import json

"""
每个key的value都是一个list, list每个元素类型是dict
"""

def readwifilog(path):
    data = defaultdict(list)
    with open(path, "r") as f:
        for line in f:
            if line.find("IP Address:") != -1:
                dicts = {}
                index = line.find("IP Address:")
                dicts["time"] = line[:23].rstrip()
                dicts["IP Address"] = line[index+11:].strip()
                data["IPAddress"].append(dicts)

            elif line.find("Router IP Address:") != -1:
                dicts = {}
                index = line.find("Router IP Address:")
                dicts["time"] = line[:23].rstrip()
                dicts["Router IP Address"] = line[index+18:].strip()
                data["RouterIPAddress"].append(dicts)

            elif line.find("Default Gateway IP Address:") != -1:
                dicts = {}
                index = line.find("Default Gateway IP Address:")
                dicts["time"] = line[:23].rstrip()
                dicts["Default Gateway IP Address"] = line[index+27:].strip()
                data["DefaultGatewayIPAddress"].append(dicts)

            elif line.find("Router MAC Address:") != -1:
                dicts = {}
                index = line.find("Router MAC Address:")
                date = line[:23]
                dicts["time"] = date.rstrip()
                dicts["Router MAC Address"] = line[index+19:].strip()
                data["RouterMACAddress"].append(dicts)

            elif line.find("didUpdateLocations") != -1:
                dicts = {}
                lat_index = line.find("latitude=")
                lon_index = line.find("longitude=")
                acc_index = line.find("Accuracy=")
                tisn_index = line.find("timeIntervalSinceNow=")
                source_index = line.find("source=")
                dicts["latitude"] = line[lat_index+10:lat_index+19]
                dicts["longitude"] = line[lon_index+11:lon_index+20]
                dicts["source"] = line[source_index+7:].strip()
                dicts["time"] = line[:23].rstrip()
                data["Locations"].append(dicts)
            
            elif line.find("Update network") != -1:
                dicts = {}
                start_index = line.find("<")
                end_index = line.find(">")
                dicts["network"] = line[start_index+1:end_index].strip()
                dicts["time"] = line[:23].rstrip()
                data["NetWork"].append(dicts)

            elif line.find("__WiFiDeviceManagerAutoAssociate") != -1:
                dicts = {}
                dicts["time"] = line[:23]
                index = line.find("__WiFiDeviceManagerAutoAssociate:")
                dicts["stasus"] = line[index+33:].strip()
                data["AutoAssociateStatus"].append(dicts)
                
    return json.dumps(data)

#path = r"C:\Users\xutao\Desktop\wifilog\WiFiManager\WiFiManager\wifi-buf-10-05-2017__092744.log"
#print readwifilog(path)