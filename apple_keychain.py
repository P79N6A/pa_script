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
import uuid

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
        self.extra_id = str(uuid.uuid1()).replace('-', '')

class KeyChainParser():
    def __init__(self, node, extract_deleted, extract_source):
        self.root = node
        self.extract_deleted = False
        self.extract_source = extract_source
        self.params = []
        self.wifiMap = {}
        self.models = []

    def parse(self): 
        self.analyze_wifi_plist()
        self.analyze_keychain_plist()
        self.get_models()
        return self.models

    def analyze_wifi_plist(self):
        nodes = list(self.root.FileSystem.Search("/SystemConfiguration/com\.apple\.wifi\.plist$"))
        if nodes is None:
            return
        for node in nodes:
            memoryRange = node.Data
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
        map_0 = PlistHelper.ReadPlist(self.root)
        if map_0 is not None and str(type(map_0)) == "<type 'NSDictionary'>":
            for iter_0 in map_0:
                key_0 = iter_0.Key
                value_0 = iter_0.Value
                if str(type(value_0)) == "<type 'NSArray'>":
                    if value_0.Count == 0:
                        continue

                    array = value_0
                    for val in array:
                        map_1 = val
                        keyname = key_0
                        if map_1 is not None and str(type(map_1)) == "<type 'NSDictionary'>":
                            if len(map_1.Keys) == 2:
                                continue
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

                            extra_id = None
                            for key in srvKey:
                                if key in map_1.Keys:
                                    for name in srvValue.keys():
                                        value = self._get_map_value(map_1, key, 'str')
                                        if '*' in name:
                                            if name.replace('*', '') in value:
                                                extra_id = self.getData(map_1, srvValue[name], value)
                                        else:
                                            if value == name:
                                                extra_id = self.getData(map_1, srvValue[name], value)

                            for name in srvValue.keys():
                                if name.find(key_0) != -1:
                                    extra_id = self.getData(map_1, srvValue[name], '')

                            if key_0 == 'cert' or key_0 == 'certificates':
                                cert = Generic.KeychainProfile.Certificate()
                                cert.Deleted = DeletedState.Intact
                                cert.SourceFile.Value = self.root.AbsolutePath
                                cert.KeychainID.Value = extra_id
                                if key_0 == 'cert':
                                    cert.UUID.Value = self._get_map_value(map_1, 'UUID', 'str')
                                    cert.AccessGroup.Value = self._get_map_value(map_1, 'agrp', 'str')
                                    cert.CreationDate.Value = self._get_timestamp(self._get_map_value(map_1, 'cdat', 'str'))
                                    cert.ModificationDate.Value = self._get_timestamp(self._get_map_value(map_1, 'mdat', 'str'))
                                    cert.Label.Value = self._get_map_value(map_1, 'labl', 'str')
                                    cert.Data.Value = self._get_map_value(map_1, 'v_Data', 'data')
                                else:
                                    cert.AccessGroup = self._get_map_value(map_1, 'Entitlement Group', 'str')
                                    cert.CreationDate.Value = self._get_timestamp(self._get_map_value(map_1, 'Create Date', 'str'))
                                    cert.ModificationDate.Value = self._get_timestamp(self._get_map_value(map_1, 'Modify Date', 'str'))
                                    cert.Label.Value = self._get_map_value(map_1, 'Label', 'str')
                                    cert.Data.Value = self._get_map_value(map_1, 'Keychain Data', 'data')
                                cert.ProtectionDomain.Value = self._get_map_value(map_1, 'pdmn', 'str')
                                cert.Issuer.Value = self._get_map_value(map_1, 'issr', 'data')
                                cert.CertificateEncoding.Value = self._get_map_value(map_1, 'cenc', 'int')
                                cert.CertificateType.Value = self._get_map_value(map_1, 'ctyp', 'int')
                                cert.PublicKeyHash.Value = self._get_map_value(map_1, 'pkhh', 'data')
                                cert.SubjectKeyID.Value = self._get_map_value(map_1, 'skid', 'data')
                                cert.SerialNumber.Value = self._get_map_value(map_1, 'slnr', 'data')
                                cert.Subject.Value = self._get_map_value(map_1, 'subj')
                                cert.Synchronizable.Value = self._get_map_value(map_1, 'sync', 'bool')
                                self.models.append(cert)
                            elif key_0 == 'genp' or key_0 == 'GenericPassword':
                                genp = Generic.KeychainProfile.GenericPassword()
                                genp.Deleted = DeletedState.Intact
                                genp.SourceFile.Value = self.root.AbsolutePath
                                genp.KeychainID.Value = extra_id
                                if key_0 == 'genp':
                                    genp.UUID.Value = self._get_map_value(map_1, 'UUID', 'str')
                                    genp.AccessGroup.Value = self._get_map_value(map_1, 'agrp', 'str')
                                    genp.CreationDate.Value = self._get_timestamp(self._get_map_value(map_1, 'cdat', 'str'))
                                    genp.ModificationDate.Value = self._get_timestamp(self._get_map_value(map_1, 'mdat', 'str'))
                                    genp.Account.Value = self._get_map_value(map_1, 'acct', 'str')
                                    genp.Service.Value = self._get_map_value(map_1, 'srvr', 'str')
                                    genp.Data.Value = self._get_map_value(map_1, 'v_Data', 'data')
                                    genp.Label.Value = self._get_map_value(map_1, 'Label', 'str')
                                else:
                                    genp.AccessGroup.Value = self._get_map_value(map_1, 'Entitlement Group', 'str')
                                    genp.CreationDate.Value = self._get_timestamp(self._get_map_value(map_1, 'Create Date', 'str'))
                                    genp.ModificationDate.Value = self._get_timestamp(self._get_map_value(map_1, 'Modify Date', 'str'))
                                    genp.Account.Value = self._get_map_value(map_1, 'Account', 'str')
                                    genp.Service.Value = self._get_map_value(map_1, 'Service', 'str')
                                    genp.Data.Value = self._get_map_value(map_1, 'Keychain Data', 'data')
                                    genp.Label.Value = self._get_map_value(map_1, 'labl', 'str')
                                genp.ProtectionDomain.Value = self._get_map_value(map_1, 'pdmn', 'str')
                                genp.Synchronizable.Value = self._get_map_value(map_1, 'sync', 'bool')
                                genp.Description.Value = self._get_map_value(map_1, 'desc', 'str')
                                self.models.append(genp)
                            elif key_0 == 'inet' or key_0 == 'InternetPassword':
                                inet = Generic.KeychainProfile.InternetPassword()
                                inet.Deleted = DeletedState.Intact
                                inet.SourceFile.Value = self.root.AbsolutePath
                                inet.KeychainID.Value = extra_id
                                if key_0 == 'inet':
                                    inet.UUID.Value = self._get_map_value(map_1, 'UUID', 'str')
                                    inet.AccessGroup.Value = self._get_map_value(map_1, 'agrp', 'str')
                                    inet.CreationDate.Value = self._get_timestamp(self._get_map_value(map_1, 'cdat', 'str'))
                                    inet.ModificationDate.Value = self._get_timestamp(self._get_map_value(map_1, 'mdat', 'str'))
                                    inet.Account.Value = self._get_map_value(map_1, 'acct', 'str')
                                    inet.Server.Value = self._get_map_value(map_1, 'srvr', 'str')
                                    inet.Data.Value = self._get_map_value(map_1, 'v_Data', 'data')
                                else:
                                    inet.AccessGroup.Value = self._get_map_value(map_1, 'Entitlement Group', 'str')
                                    inet.CreationDate.Value = self._get_timestamp(self._get_map_value(map_1, 'Create Date', 'str'))
                                    inet.ModificationDate.Value = self._get_timestamp(self._get_map_value(map_1, 'Modify Date', 'str'))
                                    inet.Account.Value = self._get_map_value(map_1, 'Account', 'str')
                                    inet.Server.Value = self._get_map_value(map_1, 'Service', 'str')
                                    inet.Data.Value = self._get_map_value(map_1, 'Keychain Data')
                                inet.ProtectionDomain.Value = self._get_map_value(map_1, 'pdmn', 'str')
                                inet.Path.Value = self._get_map_value(map_1, 'path', 'str')
                                inet.Port.Value = self._get_map_value(map_1, 'port', 'str')
                                inet.Protocol.Value = self._get_map_value(map_1, 'ptcl', 'str')
                                inet.SecurityDomain.Value = self._get_map_value(map_1, 'sdmn', 'str')
                                inet.Synchronizable.Value = self._get_map_value(map_1, 'sync', 'bool')
                                inet.Description.Value = self._get_map_value(map_1, 'desc', 'str')
                                self.models.append(inet)
                            elif key_0 == 'keys' or key_0 == 'Keys':
                                keys = Generic.KeychainProfile.Key()
                                keys.KeychainID.Value = extra_id
                                keys.Deleted = DeletedState.Intact
                                keys.SourceFile.Value = self.root.AbsolutePath
                                if key_0 == 'keys':
                                    keys.UUID.Value = self._get_map_value(map_1, 'UUID', 'str')
                                    keys.AccessGroup.Value = self._get_map_value(map_1, 'agrp', 'str')
                                    keys.CreationDate.Value = self._get_timestamp(self._get_map_value(map_1, 'cdat', 'str'))
                                    keys.ModificationDate.Value = self._get_timestamp(self._get_map_value(map_1, 'mdat', 'str'))
                                    keys.ApplicationTag.Value = self._get_map_value(map_1, 'atag', 'data')
                                    keys.CanEncrypt.Value = self._get_map_value(map_1, 'encr', 'bool')
                                    keys.CanDecrypt.Value = self._get_map_value(map_1, 'decr', 'bool')
                                    keys.Data.Value = self._get_map_value(map_1, 'v_Data', 'data')
                                    keys.Label.Value = self._get_map_value(map_1, 'labl', 'str')
                                else:
                                    keys.AccessGroup.Value = self._get_map_value(map_1, 'Entitlement Group', 'str')
                                    keys.CreationDate.Value = self._get_timestamp(self._get_map_value(map_1, 'Create Date', 'str'))
                                    keys.ModificationDate.Value = self._get_timestamp(self._get_map_value(map_1, 'Modify Date', 'str'))
                                    keys.Data.Value =  self._get_map_value(map_1, 'keychain Data', 'data')
                                    keys.Label.Value = self._get_map_value(map_1, 'Label', 'str')
                                keys.ProtectionDomain.Value = self._get_map_value(map_1, 'pdmn', 'str')
                                keys.KeySizeInBits.Value = self._get_map_value(map_1, 'bsiz', 'str')
                                keys.EffectiveKeySize.Value = self._get_map_value(map_1, 'esiz', 'str')
                                keys.KeyClass.Value = self._get_map_value(map_1, 'kcls', 'int')
                                keys.ApplicationLabel.Value = self._get_map_value(map_1, 'klbl', 'str')
                                keys.IsPermanent.Value = self._get_map_value(map_1, 'perm', 'bool')
                                keys.CanDerive.Value = self._get_map_value(map_1, 'drve', 'bool')
                                keys.CanSign.Value = self._get_map_value(map_1, 'sign', 'bool')
                                keys.CanWrap.Value = self._get_map_value(map_1, 'wrap', 'bool')
                                keys.CanVerify.Value = self._get_map_value(map_1, 'vrfy', 'bool')
                                keys.CanUnwrap.Value = self._get_map_value(map_1, 'unwp', 'bool')
                                keys.Synchronizable.Value = self._get_map_value(map_1, 'sync', 'bool')
                                if 'v_Data' in map_1.Keys or 'Keychain Data' in map_1.Keys:
                                    keys.KeyType.Value = self._get_map_value(map_1, 'type', 'int')
                                self.models.append(keys)
                            elif key_0 == 'idnt':
                                idnt = Generic.KeychainProfile.Identity()
                                idnt.Deleted = DeletedState.Intact
                                idnt.SourceFile.Value = self.root.AbsolutePath
                                idnt.KeychainID.Value = extra_id
                                idnt.UUID.Value = self._get_map_value(map_1, 'UUID', 'str')
                                idnt.AccessGroup.Value = self._get_map_value(map_1, 'agrp', 'str')
                                idnt.CreationDate.Value = self._get_timestamp(self._get_map_value(map_1, 'cdat', 'str'))
                                idnt.ModificationDate.Value = self._get_timestamp(self._get_map_value(map_1, 'mdat', 'str'))
                                idnt.Label.Value = self._get_map_value(map_1, 'labl', 'str')
                                idnt.Issuer.Value = self._get_map_value(map_1, 'issr', 'data')
                                idnt.CertificateEncoding.Value = self._get_map_value(map_1, 'cenc', 'int')
                                idnt.CertificateType.Value = self._get_map_value(map_1, 'ctyp', 'int')
                                idnt.ProtectionDomain.Value = self._get_map_value(map_1, 'pdmn', 'str')
                                idnt.PublicKeyHash.Value = self._get_map_value(map_1, 'pkhh', 'data')
                                idnt.SubjectKeyID.Value = self._get_map_value(map_1, 'skid', 'data')
                                idnt.SerialNumber.Value = self._get_map_value(map_1, 'slnr', 'data')
                                idnt.Subject.Value = self._get_map_value(map_1, 'subj', 'data')
                                idnt.Synchronizable.Value = self._get_map_value(map_1, 'sync', 'bool')
                                idnt.CertificateData.Value = self._get_map_value(map_1, 'certdata', 'data')
                                idnt.ApplicationTag.Value = self._get_map_value(map_1, 'atag', 'data')
                                idnt.KeySizeInBits.Value = self._get_map_value(map_1, 'bsiz', 'str')                                
                                idnt.CanEncrypt.Value = self._get_map_value(map_1, 'encr', 'bool')
                                idnt.CanDecrypt.Value = self._get_map_value(map_1, 'decr', 'bool')
                                idnt.EffectiveKeySize.Value = self._get_map_value(map_1, 'esiz', 'str')
                                idnt.KeyClass.Value = self._get_map_value(map_1, 'kcls', 'int')
                                idnt.ApplicationLabel.Value = self._get_map_value(map_1, 'klbl', 'str')
                                idnt.Data.Value = self._get_map_value(map_1, 'v_Data', 'data')
                                if 'v_Data' in map_1.Keys:
                                    idnt.KeyType.Value = self._get_map_value(map_1, 'type', 'int')
                                idnt.IsPermanent.Value = self._get_map_value(map_1, 'perm', 'bool')
                                idnt.CanDerive.Value = self._get_map_value(map_1, 'drve', 'bool')
                                idnt.CanSign.Value = self._get_map_value(map_1, 'sign', 'bool')
                                idnt.CanWrap.Value = self._get_map_value(map_1, 'wrap', 'bool')
                                idnt.CanVerify.Value = self._get_map_value(map_1, 'vrfy', 'bool')
                                idnt.CanUnwrap.Value = self._get_map_value(map_1, 'unwp', 'bool')
                                self.models.append(idnt)

    def get_models(self):
        for param in self.params:
            keychain = Generic.Keychain()
            keychain.Deleted = DeletedState.Intact
            keychain.Source.Value = self.root.AbsolutePath
            keychain.KeychainID.Value = param.extra_id
            if param.type == WiFiAcc:
                keychain.Type.Value = Generic.KeychainType.WIFI
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
                        password = Generic.Password()
                        password.Account.Value = keychain.Name.Value
                        password.Data.Value = param.password
                        keychain.Password.Value = password
            if param.type == AppleID:
                keychain.Type.Value = Generic.KeychainType.AppleID
                keychain.Name.Value = param.name + '(' + param.id + ')'
                keychain.CreateTime.Value = self._get_timestamp(param.createDate)
                keychain.EditTime.Value = self._get_timestamp(param.modiyDate)
                account = Contacts.UserAccount()
                account.Name.Value = param.password
                keychain.Account.Value = account
            if param.type == MailAcc:
                keychain.Type.Value = Generic.KeychainType.EmailAccount
                keychain.Name.Value = param.name
                keychain.CreateTime.Value = self._get_timestamp(param.createDate)
                keychain.EditTime.Value = self._get_timestamp(param.modiyDate)
                account = Contacts.UserAccount()
                account.Name.Value = param.id
                account.Password.Value = param.password
                keychain.Account.Value = account
                password = Generic.Password()
                password.Account.Value = param.id
                password.Data.Value = param.password
                keychain.Password.Value = password
            if param.type == BrowserPsd:
                keychain.Type.Value = Generic.KeychainType.BrowserPassword
                keychain.Name.Value = param.name
                keychain.CreateTime.Value = self._get_timestamp(param.createDate)
                keychain.EditTime.Value = self._get_timestamp(param.modiyDate)
                account = Contacts.UserAccount()
                account.Name.Value = param.id
                account.Password.Value = param.password
                keychain.Account.Value = account
                password = Generic.Password()
                password.Account.Value = param.id
                password.Data.Value = param.password
                keychain.Password.Value = password
                keychain.Site.Value = param.url
                keychain.Protocol.Value = param.ptcl
            if param.type == Others:
                name = param.name
                id = param.id
                if '(' not in name:
                    keychain.Type.Value = Generic.KeychainType.Token
                    keychain.Name.Value = param.name + '(' + param.id + ')'
                    keychain.Token.Value = param.password
                else:
                    keychain.Type.Value = Generic.KeychainType.DSID
                    keychain.Name.Value = param.name
                    keychain.DSID.Value = param.id
                keychain.CreateTime.Value = self._get_timestamp(param.createDate)
                keychain.EditTime.Value = self._get_timestamp(param.modiyDate)
            if param.type == BackupPsd:
                keychain.Type.Value = Generic.KeychainType.BackupPassword
                keychain.Name.Value = param.name
                keychain.CreateTime.Value = self._get_timestamp(param.createDate)
                keychain.EditTime.Value = self._get_timestamp(param.modiyDate)
                account = Contacts.UserAccount()
                account.Name.Value = param.id
                account.Password.Value = param.password
                keychain.Account.Value = account
                password = Generic.Password()
                password.Account.Value = param.id
                password.Data.Value = param.password
                keychain.Password.Value = password
            self.models.append(keychain)

    def getData(self, map, t, name):
        url = None
        account = None
        labl = None
        cdat = None
        mdat = None
        password = None
        ptcl_name = None
        name2 = name
        if name == 'com.apple.cfnetwork':
            cls_name = self._get_map_value(map, 'class', 'str')
            atyp_name = self._get_map_value(map, 'atyp', 'str')
            srvr_name = self._get_map_value(map, 'srvr', 'str')
            ptcl_name = self._get_map_value(map, 'ptcl', 'str')
            if ptcl_name is not None:
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
                if key in map.Keys:
                    account = self._get_map_value(map, key, 'str')
            lablKey = ['Label', 'labl']
            for key in lablKey:
                if key in map.Keys:
                    labl = self._get_map_value(map, key, 'str')
        elif name == 'com.apple.ProtectedCloudStorage':
            dsid = None
            genaKey = ['gena', 'srvr']
            for key in genaKey:
                if key in map.Keys:
                    dsid = self._get_map_value(map, key, 'str')
            accKey = ['Account', 'acct']
            for key in accKey:
                if key in map.Keys:
                    account = self._get_map_value(map, key, 'str')
            if dsid is not None:
                name2 = dsid + ' (' + account + ')'
                account = dsid
        else:
            accKey = ['Account', 'acct', 'Label', 'labl', 'srvr']
            for key in accKey:
                if key in map.Keys:
                    account = self._get_map_value(map, key, 'str')
            
        if name != 'com.apple.ProtectedCloudStorage':
            dataKey = ['Keychain Data', 'v_Data']
            for key in dataKey:
                if key in map.Keys:
                    password = self._get_map_value(map, key, 'str')
                    if '<plist' in password:
                        map_1 = PlistHelper.ReadPlist(Encoding.ASCII.GetBytes(password))
                        if map_1 is not None and str(type(map_1)) == "<type 'NSDictionary'>":
                            for iter in map_1:
                                password = self._get_map_value(map_1, key, 'str')

        cdatKey = ['Create Date', 'cdat']
        for key in cdatKey:
            if key in map.Keys:
                cdat = self._get_map_value(map, key, 'str')

        mdatKey = ['Modify Date', 'mdat']
        for key in mdatKey:
            if key in map.Keys:
                mdat = self._get_map_value(map, key, 'str')

        param = None
        if name == 'com.apple.cfnetwork':
            param = Param(t, account if labl is None else labl, cdat, mdat, account, password, url, ptcl_name)
            self.params.append(param)
        else:
            param = Param(t, account if name2 is None else name2, cdat, mdat, account, password)
            self.params.append(param)
        return param.extra_id

    @staticmethod
    def _get_map_value(map, key, format = None):
        if key not in map.Keys:
            return None
        if str(type(map[key])) == "<type 'NSData'>":
            try:
                if format == 'str':
                    return str(bytes(map[key].Bytes))
                if format == 'bool':
                    return bool(bytes(map[key].Bytes))
                if format == 'int':
                    return int(str(bytes(map[key].Bytes)))
            except:
                return None
            return map[key].Bytes
        try:
            if format == 'data':
                return UnicodeEncoding.UTF8.GetBytes(str(map[key]))
            if format == 'bool':
                return bool(map[key])
            if format == 'int':
                return int(str(map[key]))
        except:
            return None
        return str(map[key])
        
    @staticmethod
    def _get_timestamp(str):
        if str is None:
            return None
        try:
            str = str.replace('/', '-')
            if '-' not in str:
                str = str[0:4] + '-' + str[4:6] + '-' + str[6:8] + ' ' + str[8:10] + ':' + str[10:12] + ':' + str[12:14]
            timestamp = int(time.mktime(time.strptime(str, '%Y-%m-%d %H:%M:%S')))
            ts = TimeStamp.FromUnixTime(timestamp, False)
            if not ts.IsValidForSmartphone():
                ts = None
            return ts
        except Exception as e:
            traceback.print_exc()

def analyze_keychain(root, extract_deleted, extract_source):
    pr = ParserResults()
    models = KeyChainParser(root, extract_deleted, extract_source).parse()
    mlm = ModelListMerger()
    pr.Models.AddRange(list(mlm.GetUnique(models)))
    pr.Build('钥匙串')
    gc.collect()
    return pr
