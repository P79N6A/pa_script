#coding=utf-8

import PA_runtime
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions
from PA_runtime import *
from PA.InfraLib.Extensions import PlistHelper

import gc
import base64
import time, datetime

WiFiAcc      = 1
AppleID      = 2
MailAcc      = 3
BrowserPsd   = 4
Others       = 5
BackupPsd    = 6

class Param():
    def __init__(self, t, name, createDate, modiyDate, id, password, url = None, ptcl = None):
        self.type = t
        self.name = name
        self.createDate = createDate
        self.modiyDate = modiyDate
        self.id = id
        self.password = password
        self.url = url
        self.ptcl = ptcl

class KeyChainParser():
    def __init__(self, node, extract_deleted, extract_source):
        self.root = node
        self.extract_deleted = False
        self.extract_source = extract_source
        self.params = []
        self.wifiMap = {}

    def parse(self):
        self.analyze_wifi_plist()
        self.analyze_keychain_plist()

        return self.get_models()

    def analyze_wifi_plist(self):
        nodes = list(self.root.FileSystem.Search("com.apple.wifi.plist"))
        if nodes is None:
            return
        for node in nodes:
            if 'SystemConfiguration/com.apple.wifi.plist' not in node.AbsolutePath:
                continue
            file = node.AbsolutePath
            file = 'E:\com.apple.wifi.plist'
            memoryRange = MemoryRange.CreateFromFile(file)
            try:
                root = BPReader.GetTree(memoryRange)
                wifi_list = root.Children['List of known networks'].Value
                for wifi_obj in wifi_list:
                    param = {}
                    if 'SSID_STR' in wifi_obj.Children:
                        param['name'] = wifi_obj.Children['SSID_STR'].Value
                    if 'SSID' in wifi_obj.Children:
                        param['SSID'] = str(bytes(wifi_obj.Children['SSID'].Value))
                    if 'BSSID' in wifi_obj.Children:
                        param['BSSID'] = wifi_obj.Children['BSSID'].Value
                    if 'lastJoined' in wifi_obj.Children:
                        param['createTime'] = TimeStamp(wifi_obj.Children['lastJoined'].Value)
                    if 'lastAutoJoined' in wifi_obj.Children:
                        param['modifyTime'] = TimeStamp(wifi_obj.Children['lastAutoJoined'].Value)
                    self.wifiMap[param['name']] = param
            except:
                traceback.print_exc()

    def analyze_keychain_plist(self):
        file = self.root.AbsolutePath()
        map_0 = PlistHelper.ReadPlist(file)
        if map_0 is not None and str(type(map_0)) == "<type 'NSDictionary'>":
            for iter_0 in map_0:
                key_0 = iter_0.Key
                value_0 = iter_0.Value
                if value_0.Count == 0:
                    continue
                array = value_0
                for val in array:
                    map_1 = val
                    tempData = {}
                    keyname = key_0
                    if map_1 is not None and str(type(map_1)) == "<type 'NSDictionary'>":
                        for iter_1 in map_1:
                            key_1 = iter_1.Key
                            value_1 = iter_1.Value
                            value = str(value_1)
                            if str(type(value_1)) == "<type 'NSString'>":
                                if key_1 == 'Keychain Data' or key_1 == 'v_Data':
                                    val = str(value_1)
                                    map_2 = PlistHelper.ReadPlist(Encoding.ASCII.GetBytes(val))
                                    if map_2 is not None and str(type(map_2)) == "<type 'NSDictionary'>":
                                        for iter_2 in map_2:
                                            key_2 = iter_2.Key
                                            value_2 = iter_2.Value
                                            if str(type(value_2)) == "<type 'NSData'>":
                                                value = str(bytes(value_2.Bytes))
                                            else:
                                                value = str(value_2)
                            elif str(type(value_1)) == "<type 'NSData'>":
                                value = str(bytes(value_1.Bytes))
                            tempData[key_1] = value

                        srvKey = ['Service', 'svce', 'agrp']
                        srvValue = { \
                            'AirPort' : WiFiAcc, \
                            'com.apple.account.idms.token' : AppleID, \
                            'com.apple.account.AppleIDAuthentication.token' : AppleID, \
                            'com.apple.account.POP.password' : MailAcc, \
                            'com.apple.account.IMAP.password' : MailAcc, \
                            'com.apple.account.SMTP.password' : MailAcc, \
                            'com.apple.cfnetwork' : BrowserPsd, \
                            'EN_KeyChain_ServiceName_*' : Others, \
                            'sina_cookie' : Others, \
                            'com.apple.gs.appleid.auth.com.apple.account.AppleIDAuthentication.token' : Others, \
                            'com.apple.gs.icloud.auth.com.apple.account.AppleIDAuthentication.token' : Others, \
                            'com.apple.account.idms.heartbeat-token' : Others, \
                            'com.apple.account.IdentityServices.token' : Others, \
                            'com.apple.gs.idms.pet.com.apple.account.AppleIDAuthentication.token' : Others, \
                            'com.apple.gs.supportapp.auth.com.apple.account.AppleIDAuthentication.token' : Others, \
                            'com.apple.gs.idms.hb.com.apple.account.AppleIDAuthentication.token' : Others, \
                            'com.apple.gs.pb.auth.com.apple.account.AppleIDAuthentication.token' : Others, \
                            'com.apple.gs.news.auth.com.apple.account.AppleIDAuthentication.token' : Others, \
                            'com.apple.account.GameCenter.token' : Others, \
                            'com.apple.account.AppleAccount.token' : Others, \
                            'com.apple.account.AppleAccount.maps-token' : Others, \
                            'com.apple.account.DeviceLocator.token' : Others, \
                            'com.apple.account.FindMyFriends.find-my-friends-app-token' : Others, \
                            'com.apple.account.FindMyFriends.find-my-friends-token' : Others, \
                            'com.apple.account.CloudKit.token' : Others, \
                            'com.apple.account.IdentityServices.token' : Others, \
                            'com.apple.twitter.oauth-token-secret' : Others, \
                            'com.apple.twitter.oauth-token' : Others, \
                            'AllEncryptedLoginAccountKeyChainService' : Others, \
                            'com.apple.ProtectedCloudStorage' : Others, \
                            'BackupAgent' : BackupPsd \
                            }
                        for key in srvKey:
                            if key in tempData.keys():
                                for name in srvValue.keys():
                                    if '*' in name:
                                        if name.replace('*', '') in tempData.keys():
                                            self.getData(tempData, srvValue[name], tempData[key])
                                    else:
                                        if tempData[key] == name:
                                            self.getData(tempData, srvValue[name], tempData[key])
                        for name in srvValue.keys():
                            if key_0 in srvValue.keys():
                                self.getData(tempData, '', tempData[key])

    def get_models(self):
        models = []

        for param in self.params:
            keychain = Generic.Keychain()
            if param.type == WiFiAcc:
                id = param.id
                for name in self.wifiMap.keys():
                    if name == id:
                        wifi = self.wifiMap[name]
                        if 'name' in wifi.keys():
                            keychain.Name.Value = wifi['name']
                        if 'SSID' in wifi.keys():
                            keychain.SSID.Value = wifi['SSID']
                        if 'BSSID' in wifi.keys():
                            keychain.BSSID.Value = wifi['BSSID']
                        if 'createTime' in wifi.keys():
                            keychain.CreateTime.Value = wifi['createTime']
                        if 'modifyTime' in wifi.keys():
                            keychain.EditTime.Value = wifi['modifyTime']
                        keychain.Password.Value = param.password
            if param.type == AppleID:
                keychain.Name.Value = param.name + '(' + param.id + ')'
                keychain.CreateTime.Value = self._get_timestamp(param.createDate)
                keychain.EditTime.Value = self._get_timestamp(param.modiyDate)
                keychain.Account.Value = param.password
            if param.type == MailAcc:
                keychain.Name.Value = param.name
                keychain.CreateTime.Value = self._get_timestamp(param.createDate)
                keychain.EditTime.Value = self._get_timestamp(param.modiyDate)
                keychain.Account.Value = param.id
                keychain.Password.Value = param.password
            if param.type == BrowserPsd:
                keychain.Name.Value = param.name
                keychain.CreateTime.Value = self._get_timestamp(param.createDate)
                keychain.EditTime.Value = self._get_timestamp(param.modiyDate)
                keychain.Account.Value = param.id
                keychain.Password.Value = param.password
                keychain.Site.Value = param.url
                keychain.Protocol.Value = param.ptcl
            if param.type == Others:
                name = param.name
                id = param.id
                if '(' not in name:
                    keychain.Name.Value = param.name + '(' + param.id + ')'
                    keychain.Token.Value = param.password
                else:
                    keychain.Name.Value = param.name
                    keychain.DSID.Value = param.id
                keychain.CreateTime.Value = self._get_timestamp(param.createDate)
                keychain.EditTime.Value = self._get_timestamp(param.modiyDate)
            if param.type == BackupPsd:
                keychain.Name.Value = param.name
                keychain.CreateTime.Value = self._get_timestamp(param.createDate)
                keychain.EditTime.Value = self._get_timestamp(param.modiyDate)
                keychain.Account.Value = param.id
                keychain.Password.Value = param.password
            models.append(keychain)
        return models

    def getData(self, tempData, t, name):
        url = None
        account = None
        labl = None
        cdat = None
        mdat = None
        password = None
        ptcl_name = None
        name2 = name
        if name == 'com.apple.cfnetwork':
            if 'class' not in tempData.keys():
                return False
            cls_name = tempData['class']
            if cls_name != 'inet':
                return False
            if 'atyp' not in tempData.keys():
                return False
            atyp_name = tempData['atyp']
            if 'srvr' not in tempData.keys():
                return False
            srvr_name = tempData['srvr']
            if 'ptcl' not in tempData.keys():
                return False
            ptcl_name = tempData['ptcl']
            if not IsDBNull(ptcl_name):
                if ptcl_name == 'http':
                    url = 'http://' + srvr_name
                elif ptcl_name == 'https':
                    url = 'https://' + srvr_name
                else:
                    url = ptcl_name + '://' + srvr_name
            else:
                url = srvr_name

        if name == 'com.apple.cfnetwork':
            accKey = ['Account', 'acct']
            for key in accKey:
                if key in tempData.keys():
                    account = tempData[key]
            lablKey = ['Label', 'labl']
            for key in lablKey:
                if key in tempData.keys():
                    labl = tempData[key]
        elif name == 'com.apple.ProtectedCloudStorage':
            dsid = None
            genaKey = ['gena', 'srvr']
            for key in genaKey:
                if key in tempData.keys():
                    dsid = tempData[key]
            accKey = ['Account', 'acct']
            for key in accKey:
                if key in tempData.keys():
                    account = tempData[key]
            if dsid is not None:
                name2 = dsid + ' (' + account + ')'
                account = dsid
        else:
            accKey = ['Account', 'acct', 'Label', 'labl', 'srvr']
            for key in accKey:
                if key in tempData.keys():
                    account = tempData[key]

        if name != 'com.apple.ProtectedCloudStorage':
            dataKey = ['Keychain Data', 'v_Data']
            for key in dataKey:
                if key in tempData.keys():
                    password = tempData[key]
                    if '<plist' in password:
                        map = PlistHelper.ReadPlist(Encoding.ASCII.GetBytes(password))
                        if map is not None and str(type(map)) == "<type 'NSDictionary'>":
                            for iter in map:
                                key = iter.Key
                                value = iter.Value
                                if str(type(value)) == "<type 'NSData'>":
                                    password = str(bytes(value.Bytes))
                                else:
                                    password = str(value)

        cdatKey = ['Create Date', 'cdat']
        for key in cdatKey:
            if key in tempData.keys():
                cdat = tempData[key]
                if '/' in cdat:
                    cdat = tempData[key].replace('/', '-')
                else:
                    cdat = cdat[0:4] + '-' + cdat[4:6] + '-' + cdat[6:8] + ' ' + cdat[8:10] + ':' + cdat[10:12]+ ':' + cdat[12:14]

        mdatKey = ['Modify Date', 'mdat']
        for key in mdatKey:
            if key in tempData.keys():
                mdat = tempData[key]
                if '/' in mdat:
                    mdat = tempData[key].replace('/', '-')
                else:
                    mdat = mdat[0:4] + '-' + mdat[4:6] + '-' + mdat[6:8] + ' ' + mdat[8:10] + ':' + mdat[10:12] + ':' + mdat[12:14]

        if name == 'com.apple.cfnetwork':
            self.params.append(Param(t, account if labl is None else labl, cdat, mdat, account, password, url, ptcl_name))
        else:
            self.params.append(Param(t, account if name2 is None else name2, cdat, mdat, account, password))
        return True

    @staticmethod
    def _get_timestamp(str):
        timestamp = int(time.mktime(time.strptime(str, '%Y-%m-%d %H:%M:%S')))
        try:
            ts = TimeStamp.FromUnixTime(timestamp, False)
            if not ts.IsValidForSmartphone():
                ts = None
            return ts
        except Exception as e:
            return None

def analyze_keychain(root, extract_deleted, extract_source):
    pr = ParserResults()
    
    models = KeyChainParser(root, extract_deleted, extract_source).parse()

    mlm = ModelListMerger()

    pr.Models.AddRange(list(mlm.GetUnique(models)))

    pr.Build('KeyChain')

    gc.collect()
    return pr

def execute(node,extracteDeleted):
    return analyze_keychain(node, extracteDeleted, False)
