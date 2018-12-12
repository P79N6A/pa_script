#coding=utf-8

__author__ = "Xu Tao"

import PA_runtime
from PA_runtime import *


class WIFIPWD(object):

    def __init__(self, node, extract_deleted, extract_source):
        self.root = node
        self.extract_deleted = False
        self.extract_source = extract_source


    def parse(self):
        models = [] 
        models.extend(self.get_data())
        return models

    def get_data(self):
        models = []
        if self.root is None:
            return
        with open(self.root.PathWithMountPoint, "r") as f:
            for line in f:
                if line.find("network={") != -1:
                    try:
                        keychain = Generic.Keychain()
                        keychain.Type.Value = KeychainType.WIFI
                    except Exception as e:
                        pass
                # wifi ssid
                elif line.find("ssid") != -1:
                    try:
                        ssid_pattern = re.compile('.*ssid="(.*)".*')
                        results = re.search(ssid_pattern, line)
                        if results:
                            ssid = results.groups()[0]
                            print(ssid)
                            keychain.SSID.Value = ssid
                    except Exception as e:
                        pass
                # wifi bssid
                elif line.find("bssid") != -1:
                    try:
                        bssid_pattern = re.compile('.*bssid="(.*)".*')
                        results = re.search(bssid_pattern, line)
                        if results:
                            bssid = results.groups()[0]
                            print(bssid)
                            keychain.BSSID.Value = bssid
                    except Exception as e:
                        pass
                # wifi password
                elif line.find("psk") != -1:
                    try:
                        pwd = Password()
                        psk_pattern = re.compile('.*psk="(.*)".*')
                        results = re.search(psk_pattern, line)
                        if results:
                            psk = results.groups()[0]
                            print(psk)
                            pwd.Data.Value = psk
                            keychain.Password.Value = pwd
                    except Exception as e:
                        pass

                elif line.find("password") != -1:
                    try:
                        pwd = Password()
                        pwd_pattern = re.compile('.*password="(.*)".*')
                        results = re.search(pwd_pattern, line)
                        if results:
                            password = results.groups()[0]
                            print(password)
                            pwd.Data.Value = password
                            keychain.Password.Value = pwd
                    except Exception as e:
                        pass
                
                elif line.find("}") != -1:
                    models.append(keychain)

        return models


def analyze_wifipwd(root, extract_deleted, extract_source):
    pr = ParserResults()

    models = WIFIPWD(root, extract_deleted, extract_source).parse()
    mlm = ModelListMerger()
    if models:
        pr.Models.AddRange(list(mlm.GetUnique(models)))
        pr.Build('钥匙串')
    return pr