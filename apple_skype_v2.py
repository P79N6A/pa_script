#coding=utf-8
import os
import PA_runtime
from PA_runtime import *

SKYPE_DBB_EXTENSION         = '.dbb'
SKYPE_RECORD_MAGIC          = 'l33l'

## 记录类型
SKYPE_RECTYPE_USERS         = 0x02
SKYPE_RECTYPE_CALL          = 0x04
SKYPE_RECTYPE_PROFILE       = 0x05
SKYPE_RECTYPE_VOICEMAIL     = 0x07
SKYPE_RECTYPE_CHAT          = 0x08
SKYPE_RECTYPE_CHATMSG       = 0x09
SKYPE_RECTYPE_CONTACTGROUP  = 0x0A
SKYPE_RECTYPE_SMS           = 0x0C 
SKYPE_RECTYPE_CALLMEMBER    = 0x0D
SKYPE_RECTYPE_CHATMEMBER    = 0x0F

## 字段类型 (numeric, string, blob)
SKYPE_REC_FIELD_NUMERIC     = 0x00
SKYPE_REC_FIELD_STRING      = 0x03
SKYPE_REC_FIELD_BLOB        = 0x04

## 字段健值
PROFILE_ACCOUNT_MODIFS      = '\x00\x0F'
PROFILE_USER_NAME           = '\x03\x10'
PROFILE_FULL_NAME           = '\x03\x14'
PROFILE_PSTN_NUMBER         = '\x03\x18'
PROFILE_BIRTHDAY            = '\x00\x1D'
PROFILE_GENDER              = '\x00\x21' 
PROFILE_LANGUAGE            = '\x03\x24'
PROFILE_COUNTRY             = '\x03\x28'
PROFILE_STATE               = '\x03\x2C'
PROFILE_CITY                = '\x03\x30'
PROFILE_HOME_NUM            = '\x03\x34'
PROFILE_OFFICE_NUM          = '\x03\x38'
PROFILE_BUDDYBLOB           = '\x04\x39'
PROFILE_MOBILE_NUM          = '\x03\x3C'
PROFILE_EMAIL_ADDR          = '\x03\x40'
PROFILE_HOMPAGE             = '\x03\x44'
PROFILE_ABOUT_ME            = '\x03\x48'
PROFILE_TIMESTAMP           = '\x00\x4D'
PROFILE_AVATAR              = '\x04\x5B'
PROFILE_GIVEN_AUTH_LEVEL    = '\x00\x5D'
PROFILE_MOODTEXT            = '\x03\x68' 
PROFILE_TIMEZONE            = '\x00\x6D'
PROFILE_AUTHED_BUDDIES      = '\x00\x71'
PROFILE_IP_COUNTRY          = '\x03\x74'
PROFILE_BUDDY_STATUS        = '\x00\x79'
PROFILE_ISBLOCKED           = '\x00\x81\x01'
PROFILE_HOME_NUM_NRMLZD     = '\x03\x83\x01'
PROFILE_GIVEN_DISPLAY_NAME  = '\x03\x84\x01'
PROFILE_MOBILE_NUM_NRMLZD   = '\x03\x8B\x01'
PROFILE_LASTONLINE_TIME     = '\x00\x8D\x01'
PROFILE_CAPABILITIES        = '\x04\x92\x01'
PROFILE_GIF_ATTACHED        = '\x04\x96\x01'
PROFILE_LASTUSED_TS         = '\x00\x9D\x01'
PROFILE_OFFLINE_CALLFORWARD = '\x03\xB4\x02'
PROFILE_RICH_MOOD_TEXT      = '\x03\xB4\x06'

CALL_CALL_ID                = '\x00\x07'
CALL_OTHERS_USER_NAME       = '\x03\x98\x07'
CALL_OTHERS_DISP_NAME       = '\x03\x9C\x07'
CALL_BEGIN_TIMESTAMP        = '\x00\xA1\x07'
CALLMEMBER_DURATION         = '\x00\xA5\x07'
CALL_PRICE_PER_MIN          = '\x00\xA9\x07'
CALL_PRICE_CURRENCY         = '\x03\xAC\x07'
CALLMEMBERS_TYPE            = '\x00\xB1\x07'
CALLMEMBERS_STATUS          = '\x00\xB5\x07'
CALLMEMBERS_NAME            = '\x03\xB8\x01'
CALL_FAILURE_REASON         = '\x00\xB9\x07'
CALL_PRICE_PRECISION        = '\x00\xBD\x01'
CALL_PSTN_STATUS_TEXT       = '\x03\xC4\x07'
CALL_MEMBER_USER_NAME       = '\x03\xC8\x06'
CALL_MEMBER_DURATION        = '\x00\xD1\x06'
CALL_NAME                   = '\x03\xE4\x06'

CHAT_LAST_CHANGE            = '\x00\x17'
CHAT_DBPATH                 = '\x03\x33'
CHAT_SPLIT_FIRENDLYNAME     = '\x03\x37'
CHAT_MYROLE                 = '\x00\x89\x18'
CHAT_ACTIVITY_TIMESTAMP     = '\x00\xB5\x04'
CHAT_CHATNAME               = '\x03\xB8\x03'
CHAT_ADDER                  = '\x03\xBC\x06'
CHAT_TIMESTAMP              = '\x00\xBD\x03'
CHAT_DIALOG_PARTNER         = '\x03\xC0\x03'
CHAT_POSTERS                = '\x03\xC8\x03'
CHAT_PARTICIPANTS           = '\x03\xCC\x03'
CHAT_ACTIVE_MEMBERS         = '\x03\xD4\x03'
CHAT_FRIENDLY_NAME          = '\x03\xD8\x03'

CHATMEMBER_NAME             = '\x03\xC8\x04'
CHATMEMBER_IDENTITY         = '\x03\xCC\x04'
CHATMEMBER_ROLE             = '\x00\xD1\x04'
CHATMEMBER_IS_ACTIVE        = '\x00\xD5\x04'

CHATMSG_PK_ID               = '\x00\x03'
CHATMSG_CRC                 = '\x00\x07'
CHATMSG_REMOTE_ID           = '\x00\x0B'
CHATMSG_STATUS              = '\x00\x81\x04'
CHATMSG_DIALOG_PARTNER      = '\x03\xD8\x18'
CHATMSG_CHATNAME            = '\x03\xE0\x03'
CHAGMSG_GUID                = '\x04\xE2\x18'
CHATMSG_TIMESTAMP           = '\x00\xE5\x03' 
CHATMSG_AUTHOR              = '\x03\xE8\x03'
CHATMSG_FROM_DISPNAME       = '\x03\xEC\x03'
CHATMSG_MESSAGE_TYPE        = '\x00\xF1\x03' 
CHATMSG_IDENTITIES          = '\x03\xF4\x03'
CHATMSG_BODY_XML            = '\x03\xFC\x03'

SMS_PRICE                   = '\x00\x85\x06'
SMS_PRICE_CURRENCY          = '\x03\x88\x06'
SMS_TARGET_NUMBERS          = '\x03\x8C\x06'
SMS_BODY                    = '\x03\x94\x06'
SMS_TIMESTAMP               = '\x00\x99\x06'
SMS_PRICE_PRECISION         = '\x00\xC5\x01'
SMS_TYPE                    = '\x00\xF9\x05'
SMS_STATUS                  = '\x00\xFD\x05'

VOICEMAIL_PATH              = '\x03\x0F'
VOICEMAIL_PARTNER_HANDLE    = '\x03\x94\x03'
VOICEMAIL_PARTNER_DISPNAME  = '\x03\x98\x03'
VOICEMAIL_TIMESTAMP         = '\x00\xA9\x03'

class ValueChunkTuple :
    def __init__ (self, inVal, inChunk) :
        self.cValue = inVal
        self.cChunk = inChunk

class SkpeUserData :
    def __init__ (self, sSkypeUserName) :
        self.sOriginUserName = ''
        self.sOriginUserName = sSkypeUserName
        
    def generateContact (self, isUserAccount = False, extractDeleted = True, extractSource = True) :
        if not isUserAccount :
            cPhysCont = Contact()
            cPhysCont.Source.Value = 'Skype: ' + self.sOriginUserName
        else :
            cPhysCont = UserAccount ()
            cPhysCont.ServiceType.Value = 'Skype'
        
        
        if (hasattr(self, 'sAuthLevel') and self.sAuthLevel.cValue == 'Blocked') or (hasattr(self, 'sBuddyStatus') and self.sBuddyStatus.cValue == 'Deleted'):
            if extractDeleted:
                cPhysCont.Deleted = DeletedState.Deleted
            else:
                return None
        else :
            cPhysCont.Deleted = DeletedState.Intact
        ## Contact Name
        if hasattr(self, 'sFullName') :
            cPhysCont.Name.Value  = unicode (self.sFullName.cValue, 'utf-8')
            if extractSource:
                cPhysCont.Name.Source = MemoryRange (self.sFullName.cChunk)
        elif hasattr(self, 'sGivenDispName') :
            cPhysCont.Name.Value  = unicode (self.sGivenDispName.cValue, 'utf-8')
            if extractSource:
                cPhysCont.Name.Source = MemoryRange (self.sGivenDispName.cChunk)
        else :
            cPhysCont.Name.Value  = self.sUserName.cValue
            if extractSource:
                cPhysCont.Name.Source = MemoryRange (self.sUserName.cChunk)
        if hasattr(self, 'sUserName') :
            if isUserAccount :
                cPhysCont.Username.Value  = self.sUserName.cValue
                if extractSource:
                    cPhysCont.Username.Source = MemoryRange (self.sUserName.cChunk)
            else:
                ce                = UserID ()
                ce.Deleted        = cPhysCont.Deleted
                ce.Category.Value = 'Skype'
                ce.Value.Value    = self.sUserName.cValue
                if extractSource:
                    ce.Value.Source   = MemoryRange (self.sUserName.cChunk)
                cPhysCont.Entries.Add (ce)

        for name, category in [
            ('sPSTNNum', 'General'),
            ('sHomeNum', 'Home'),
            ('sOfficeNum', 'Office'),
            ('sMobileNum', 'Mobile'),
            ('sCallForwardNum', 'Other')]:
            value = getattr(self, name, ValueChunkTuple('', []))
            if value.cValue != '':
                ce                = PhoneNumber ()
                ce.Deleted        = cPhysCont.Deleted
                ce.Category.Value = category
                ce.Value.Value    = value.cValue
                if extractSource:
                    ce.Value.Source = MemoryRange (value.cChunk)
                cPhysCont.Entries.Add (ce)

        ## Email Address
        if hasattr(self, 'sEMailAddr') and self.sEMailAddr.cValue!= '':
            ce                = EmailAddress ()
            ce.Deleted        = cPhysCont.Deleted
            ce.Category.Value = 'General'
            ce.Value.Value    = self.sEMailAddr.cValue
            if extractSource:
                ce.Value.Source   = MemoryRange (self.sEMailAddr.cChunk)
            cPhysCont.Entries.Add (ce)
        if hasattr(self, 'sGif'):
            phot = ContactPhoto ()
            phot.Deleted = cPhysCont.Deleted
            node = Node(NodeType.File)
            node.Data = MemoryRange (Chunk(self.sGif.cChunk.BaseStream, self.sGif.cChunk.Offset + 1, self.sGif.cChunk.Length - 1))
            phot.PhotoNode.Value = node
            cPhysCont.Photos.Add (phot)
        ## Address
        if hasattr(self, 'sCountry') or hasattr(self, 'sCity') or hasattr(self, 'sState') :
            addr                = StreetAddress ()
            addr.Deleted        = DeletedState.Intact
            addr.Category.Value = 'General'
            if hasattr(self, 'sCity') :
                addr.City.Value  = unicode (self.sCity.cValue, 'utf-8')
                if extractSource:
                    addr.City.Source = MemoryRange (self.sCity.cChunk)
            if hasattr(self, 'sState') :
                addr.State.Value  = unicode (self.sState.cValue, 'utf-8')
                if extractSource:
                    addr.State.Source = MemoryRange (self.sState.cChunk)
            if hasattr(self, 'sCountry') :
                addr.Country.Value  = unicode (self.sCountry.cValue, 'utf-8')
                if extractSource:
                    addr.Country.Source = MemoryRange (self.sCountry.cChunk)
            cPhysCont.Addresses.Add (addr)
        ## Homepage
        if hasattr(self, 'sHomepage') and self.sHomepage.cValue!= '':
            hompg                = WebAddress ()
            hompg.Deleted        = cPhysCont.Deleted
            hompg.Category.Value = 'General'
            hompg.Value.Value    = self.sHomepage.cValue
            if extractSource:
                hompg.Value.Source   = MemoryRange (self.sHomepage.cChunk)
            cPhysCont.Entries.Add (hompg)
        ## About me and status
        if hasattr(self, 'sAboutMe') and self.sAboutMe.cValue!= '':
            cPhysCont.Notes.Add (u'About: ' + unicode (self.sAboutMe.cValue, 'utf-8'))
        if hasattr(self, 'sMoodText') and self.sMoodText.cValue!= '':
            cPhysCont.Notes.Add (u'Status: ' + unicode (self.sMoodText.cValue, 'utf-8'))

        return cPhysCont

    def updateKey (self, inKey, inStr) :
        if inKey == PROFILE_USER_NAME :
            self.sUserName = inStr
        elif inKey == PROFILE_FULL_NAME :
            self.sFullName = inStr
        elif inKey == PROFILE_PSTN_NUMBER :
            self.sPSTNNum = inStr
        elif inKey == PROFILE_LANGUAGE :
            self.sLanguage = inStr
        elif inKey == PROFILE_COUNTRY :
            self.sCountry = inStr
        elif inKey == PROFILE_STATE :
            self.sState = inStr
        elif inKey == PROFILE_CITY :
            self.sCity = inStr
        elif inKey == PROFILE_HOME_NUM :
            self.sHomeNum = inStr
        elif inKey == PROFILE_OFFICE_NUM :
            self.sOfficeNum = inStr
        elif inKey == PROFILE_MOBILE_NUM :
            self.sMobileNum = inStr
        elif inKey == PROFILE_EMAIL_ADDR :
            self.sEMailAddr = inStr
        elif inKey == PROFILE_HOMPAGE :
            self.sHomepage = inStr
        elif inKey == PROFILE_ABOUT_ME :
            self.sAboutMe = inStr
        elif inKey == PROFILE_MOODTEXT :
            self.sMoodText = inStr
        elif inKey == PROFILE_IP_COUNTRY :
            self.sIPCountry = inStr
        elif inKey == PROFILE_GIVEN_DISPLAY_NAME :
            self.sGivenDispName = inStr
        elif inKey == PROFILE_OFFLINE_CALLFORWARD :
            self.sCallForwardNum = inStr
        elif inKey == PROFILE_GIF_ATTACHED :
            self.sGif = inStr
        elif inKey == PROFILE_LASTONLINE_TIME :
            self.sLastOnlineTS = time.asctime (time.localtime (inStr.cValue))
        elif inKey == PROFILE_GIVEN_AUTH_LEVEL :
            if inStr.cValue != 1 :
                self.sAuthLevel = ValueChunkTuple ('Blocked', inStr.cChunk)
        elif inKey == PROFILE_GENDER :
            if inStr == 1 :
                self.sGender = ValueChunkTuple ('M', inStr.cChunk)
            elif inStr == 2 :
                self.sGender = ValueChunkTuple ('F', inStr.cChunk)
        elif inKey == PROFILE_BIRTHDAY :
            if inStr.cValue == 0 :
                return
            ## Numeric format digits are : YYYYMMDD
            day   = inStr.cValue % 100
            month = (inStr.cValue / 100) % 100
            year  = inStr.cValue / 10000
            self.sBirthday = ValueChunkTuple (str(day) + '/' + str (month) + '/' + str (year), inStr.cChunk)
        elif inKey == PROFILE_TIMEZONE :
            ## turn inStr format: (24 + X) * 3600 into: GMT + X
            timezone = (inStr.cValue / 3600) - 24
            if timezone >= 0:
                self.sTimeZone = ValueChunkTuple('GMT+' + str (timezone), inStr.cChunk)
            else :
                self.sTimeZone = ValueChunkTuple('GMT'  + str (timezone), inStr.cChunk)
        elif inKey == PROFILE_BUDDY_STATUS :
            ## 0- never been, 1- deleted. 2- pending auth, 3-added
            if inStr == 0 :
                stat = 'NeverWas'
            elif inStr == 1 :
                stat = 'Deleted'
            elif inStr == 2 :
                stat = 'PendingAuth'
            elif inStr == 3 :
                stat = 'Added'
            else :
                return
            self.sBuddyStatus = ValueChunkTuple(stat, inStr.cChunk)

## Skype call record
class SkypeCallRecord :

    def generateCall (self, extractDeleted, extractSource) :
        cPhysCall = Call ()

        cPhysCall.Source.Value = 'Skype'
        ## Me 
        if hasattr(self, 'sMemberUserName'):
            cPhysCall.Source.Value += ' : ' + self.sMemberUserName.cValue
        cPhysCall.Deleted      = DeletedState.Intact
        ## Other side 
        if hasattr(self, 'sOthersDispNam') :
            party = Party()
            party.Identifier.Value  = unicode (self.sOthersDispName.cValue, 'utf-8')
            if extractSource:
                party.Identifier.Source = self.sOthersDispName.cChunk
            cPhysCall.Parties.Add (party)            
        elif hasattr(self, 'sOthersUserName') :
            party = Party()
            party.Identifier.Value  = self.sOthersUserName.cValue
            if extractSource:
                party.Identifier.Source = MemoryRange (self.sOthersUserName.cChunk)
            cPhysCall.Parties.Add(party)            
        ## Type
        if hasattr(self, 'sCallType') :
            if self.sCallType.cValue == 'INCOMING_P2P' or self.sCallType.cValue == 'INCOMING_PSTN' :
                cPhysCall.Type.Value = CallType.Incoming
            elif self.sCallType.cValue == 'OUTGOING_P2P' or self.sCallType.cValue == 'OUTGOING_PSTN' :
                cPhysCall.Type.Value = CallType.Outgoing

        if hasattr(self, 'nPOSIXTimeStamp') and self.nPOSIXTimeStamp.cValue > 0 :
            cPhysCall.TimeStamp.Value  = TimeStamp.FromUnixTime(self.nPOSIXTimeStamp.cValue)
            if extractSource:
                cPhysCall.TimeStamp.Source = MemoryRange (self.nPOSIXTimeStamp.cChunk)
        if hasattr(self, 'nCallDuration') and self.nCallDuration.cValue > 0 :
            cPhysCall.Duration.Value  = TimeSpan (self.nCallDuration.cValue * 10000000)
            if extractSource:
                cPhysCall.Duration.Source = MemoryRange (self.nCallDuration.cChunk)

        return cPhysCall

    def updateKey (self, inKey, inStr) :
        if inKey == CALL_MEMBER_USER_NAME :
            self.sMemberUserName = inStr
        elif inKey == CALL_OTHERS_USER_NAME :
            self.sOthersUserName = inStr
        elif inKey == CALL_OTHERS_DISP_NAME :
            self.sOthersDispName = inStr
        elif inKey == CALL_PSTN_STATUS_TEXT :
            self.sPSTNStatus = inStr
        elif inKey == CALLMEMBER_DURATION :
            self.nCallDuration = inStr
        elif inKey == CALLMEMBERS_TYPE :
            if inStr.cValue == 1 :
                self.sCallType = ValueChunkTuple('INCOMING_P2P', inStr.cChunk)
            elif inStr.cValue == 2:
                self.sCallType = ValueChunkTuple('OUTGOING_P2P', inStr.cChunk)
            elif inStr.cValue == 3 :
                self.sCallType = ValueChunkTuple('INCOMING_PSTN', inStr.cChunk)
            elif inStr.cValue == 4 :
                self.sCallType = ValueChunkTuple('OUTGOING_PSTN', inStr.cChunk)

class SkypeChatMSG :

    def updateKey (self, inKey, inStr) :
        if inKey == CHATMSG_AUTHOR :
            self.sAuthor = inStr
        elif inKey == CHATMSG_FROM_DISPNAME :
            self.sFromDispName = inStr
        elif inKey == CHATMSG_TIMESTAMP :
            self.nTimeStamp = inStr
        elif inKey == CHATMSG_DIALOG_PARTNER :
            self.sDialogPartner = inStr
        elif inKey == CHATMSG_BODY_XML :
            self.sBodyXML = inStr
        elif inKey == CHATMSG_MESSAGE_TYPE :
            self.nType = inStr

    def generateChatMSG (self, extractDeleted, extractSource) :
        if self.nType.cValue != 3 :
            return None
        cPhysIM = InstantMessage () 
        cPhysIM.Deleted  = DeletedState.Intact
        cPhysIM.SourceApplication.Value = 'Skype'     
        if hasattr(self, 'sAuthor') :
            if extractSource:
                cPhysIM.From.Value  = Party.MakeFrom(self.sAuthor.cValue, MemoryRange (self.sAuthor.cChunk))
            else:
                cPhysIM.From.Value  = Party.MakeFrom(self.sAuthor.cValue, None)
            if hasattr(self, 'sFromDispName') :                
                cPhysIM.From.Value.Name.Value = unicode (self.sFromDispName.cValue, 'utf-8')
                if extractSource:
                    cPhysIM.From.Value.Name.Source = MemoryRange (self.sFromDispName.cChunk)
        ## Add content
        if hasattr(self, 'sBodyXML') :
            cPhysIM.Body.Value  = unicode (self.sBodyXML.cValue, 'utf-8')
            if extractSource:
                cPhysIM.Body.Source = MemoryRange (self.sBodyXML.cChunk)
        ## Add Timestamp
        if self.nTimeStamp.cValue > 0 :
            cPhysIM.TimeStamp.Value  = TimeStamp.FromUnixTime (self.nTimeStamp.cValue)
            if extractSource:
                cPhysIM.TimeStamp.Source = MemoryRange (self.nTimeStamp.cChunk)

        return cPhysIM

class SkypeChat :
    def __init__ (self) :
        self.lChatMSGs        = {}

    def updateKey (self, inKey, inStr) :
        if inKey == CHAT_CHATNAME :
            self.sName = inStr
        elif inKey == CHAT_ADDER :
            self.sAdder = inStr
        elif inKey == CHAT_POSTERS :
            self.sPosters = inStr
        elif inKey == CHAT_PARTICIPANTS :
            self.sMembers = inStr
        elif inKey == CHAT_ACTIVE_MEMBERS :
            self.sActiveMembers = inStr
        elif inKey == CHAT_TIMESTAMP :
            self.nCreateTimeStamp = inStr
        elif inKey == CHAT_LAST_CHANGE :
            self.nLastActivityTS = inStr

    def updateChatMessage (self, cSkypeChatMSG, nTimeStamp) :
        if not nTimeStamp in self.lChatMSGs :
            self.lChatMSGs [nTimeStamp] = []
        self.lChatMSGs [nTimeStamp].append (cSkypeChatMSG)

    def generateChat (self, extractDeleted, extractSource) :
        cPhysChat              = Chat ()
        cPhysChat.Source.Value = 'Skype'
        cPhysChat.Deleted      = DeletedState.Intact
        
        ## Name
        if hasattr(self, 'sName') :
            cPhysChat.Id.Value  = self.sName.cValue
            if extractSource:
                cPhysChat.Id.Source = MemoryRange (self.sName.cChunk)
        chunkOffset = 0
        if hasattr(self, 'sMembers') :
            members = self.sMembers.cValue.split (' ')
            for mem in members :
                if extractSource:
                    cPhysChat.Participants.Add (Party.MakeGeneral(mem, MemoryRange (Chunk (self.sMembers.cChunk.BaseStream, self.sMembers.cChunk.Offset + chunkOffset, len (mem)))))
                else:
                    cPhysChat.Participants.Add (Party.MakeGeneral(mem, None))
                chunkOffset += len(mem) + 1
        if hasattr(self, 'nCreateTimeStamp') and self.nCreateTimeStamp.cValue > 0 :
            cPhysChat.StartTime.Value  = TimeStamp.FromUnixTime (self.nCreateTimeStamp.cValue)
            if extractSource:
                cPhysChat.StartTime.Source = MemoryRange (self.nCreateTimeStamp.cChunk)
        if hasattr(self, 'nLastActivityTS') and self.nLastActivityTS.cValue > 0 :
            cPhysChat.LastActivity.Value  = TimeStamp.FromUnixTime (self.nLastActivityTS.cValue)
            if extractSource:
                cPhysChat.LastActivity.Source = MemoryRange (self.nLastActivityTS.cChunk)
        tskeys = self.lChatMSGs.keys()
        tskeys.sort()
        msgind = 0
        for tsk in tskeys :
            for cMSG in self.lChatMSGs [tsk] :
                cPhysIM_MSG = cMSG.generateChatMSG (extractDeleted, extractSource)
                if cPhysIM_MSG == None :
                    continue
                if cPhysIM_MSG.TimeStamp.Value != None and cPhysChat.StartTime.Value != None and cPhysIM_MSG.TimeStamp.Value.Value.Ticks < cPhysChat.StartTime.Value.Value.Ticks:
                    cPhysChat.StartTime.Value = cPhysIM_MSG.TimeStamp.Value
                    if extractSource:
                        cPhysChat.StartTime.Source = cPhysIM_MSG.TimeStamp.Source
                elif cPhysIM_MSG.TimeStamp.Value != None and cPhysChat.LastActivity.Value != None and cPhysIM_MSG.TimeStamp.Value.Value.Ticks > cPhysChat.LastActivity.Value.Value.Ticks:
                    cPhysChat.LastActivity.Value = cPhysIM_MSG.TimeStamp.Value
                    if extractSource:
                        cPhysChat.LastActivity.Source = cPhysIM_MSG.TimeStamp.Source
                cPhysChat.Messages.Add (cPhysIM_MSG)
                msgind += 1
        if msgind == 0 :
            return None 
        return cPhysChat

class SkypeSMS :
    def __init__ (self, sUserID) :
        self.sUserID        = sUserID

    def updateKey (self, inKey, inStr) :
        if inKey == SMS_TARGET_NUMBERS :
            self.sTargetNumbers = inStr
        elif inKey == SMS_BODY :
            self.sBody = inStr
        elif inKey == SMS_TIMESTAMP :
            self.nTimeStamp = inStr
        elif inKey == SMS_STATUS :
            self.nStatus = inStr

    def generateSMS (self, extractDeleted, extractSource) :
        cPhysSMS = SMS ()
        ## 是否是一个message ?
        if not hasattr(self, 'sBody') or self.sBody.cValue == '' :
            return None
        cPhysSMS.Source.Value     = 'Skype : ' + self.sUserID
        cPhysSMS.Folder.Value     = 'Skype'
        cPhysSMS.Deleted          = DeletedState.Intact
        ## Body
        cPhysSMS.Body.Value       = unicode (self.sBody.cValue, "utf-8")
        if extractSource:
            cPhysSMS.Body.Source      = MemoryRange (self.sBody.cChunk)
        ## TimeStamp
        cPhysSMS.TimeStamp.Value  = TimeStamp.FromUnixTime (self.nTimeStamp.cValue)
        if extractSource:
            cPhysSMS.TimeStamp.Source = MemoryRange (self.nTimeStamp.cChunk)
        ## 参与者 (空格分割)
        chunkOffset = 0
        for partic in self.sTargetNumbers.cValue.split (' ') :
            if len (partic) > 0 :
                if extractSource:
                    cPhysSMS.Parties.Add (Party.MakeTo (partic, MemoryRange (Chunk (self.sTargetNumbers.cChunk.BaseStream, self.sTargetNumbers.cChunk.Offset + chunkOffset, len (partic)))))
                else:
                    cPhysSMS.Parties.Add (Party.MakeTo (partic, None))
                chunkOffset += len(partic)
            chunkOffset += 1

        if self.nStatus.cValue == 6 :
            cPhysSMS.Status.Value = MessageStatus.Sent

        return cPhysSMS

class SkypeUserDatabase :
    def __init__ (self, sDBUserName) :
        self.SkypeDBUserName = sDBUserName
        
        self.lCallRecords = {}
        self.lProfiles    = {}
        self.lUserProfs   = {}
        self.lChats       = {}
        self.lSMSs        = []

    def parseCallAttemptTS (self, inStr) :
        res = inStr.split ('-')
        return (int (res [0]), int (res [1]))

    def extractNumricValue (self, pBuf, nBufLen, nOffset) :
        tOffset      = nOffset
        sNumerValue  = pBuf [tOffset : tOffset + 1]
        sLastChar    = sNumerValue 
        tOffset     += 1
        nNumerLen    = 1
        while ((unpack("B", sLastChar)[0]) & 0x80) != 0x00 and tOffset < nBufLen :
            sLastChar    = pBuf [tOffset : tOffset + 1]
            tOffset     += 1
            sNumerValue += sLastChar
            nNumerLen   += 1
        return (sNumerValue, nNumerLen)

    def parseNumericValue (self, pBuf, nBufLen) :
        result  = 0
        for ind in xrange(nBufLen) :
            asbyte  = unpack ("B", pBuf [ind : ind + 1] ) [0] & 0x7F
            result += asbyte << (7 * ind)
        return result

    ## 解析Skype记录
    def parseSkypeRecord (self, pRecBuf, nRecLen, pMemNode, nStartRecOffset) :
        recHeader    = unpack ("<IIB", pRecBuf [0 : 9])
        nRecUID      = recHeader [0] 
        nRecType     = recHeader [2]
        lDataEntries = {}
        nOffset      = 9
        while nOffset < nRecLen :
            (nFieldFormat, nFieldType) = unpack ("2B", pRecBuf [nOffset : nOffset + 2])

            ## 解析字段类型. 如果第二个字节的值大于 0x80 ,则需要包括第三个字节
            if nFieldType < 0x80 :
                sFieldType  = pRecBuf [nOffset : nOffset + 2]
                nOffset    += 2
            else :
                sFieldType  = pRecBuf [nOffset : nOffset + 3]
                nOffset    += 3

            if nFieldFormat == SKYPE_REC_FIELD_NUMERIC :
                (sNumerValue, nNumerLen)  = self.extractNumricValue (pRecBuf, nRecLen, nOffset)
                lDataEntries [sFieldType] = ValueChunkTuple (self.parseNumericValue (sNumerValue, nNumerLen), Chunk (pMemNode, nStartRecOffset + nOffset, nNumerLen))
                nOffset                  += nNumerLen
            elif nFieldFormat == SKYPE_REC_FIELD_STRING :
                nNullInd = pRecBuf.find ('\x00', nOffset)
                sDataStr = pRecBuf [nOffset : nNullInd]
                lDataEntries [sFieldType] = ValueChunkTuple (sDataStr, Chunk (pMemNode, nStartRecOffset + nOffset, nNullInd - nOffset))
                nOffset                   = nNullInd + 1
            elif nFieldFormat == SKYPE_REC_FIELD_BLOB :
                (sNumerValue, nNumerLen)   = self.extractNumricValue (pRecBuf, nRecLen, nOffset)
                nBLOBLen                   = self.parseNumericValue  (sNumerValue, nNumerLen)
                nOffset                   += nNumerLen
                lDataEntries [sFieldType]  = ValueChunkTuple (pRecBuf [nOffset : nOffset + nBLOBLen], Chunk (pMemNode, nStartRecOffset + nOffset, nBLOBLen))
                nOffset                   += nBLOBLen

        if nRecType == SKYPE_RECTYPE_USERS or nRecType == SKYPE_RECTYPE_PROFILE :
            if PROFILE_USER_NAME in lDataEntries :
                sKey = lDataEntries [PROFILE_USER_NAME].cValue
            elif PROFILE_GIVEN_DISPLAY_NAME in lDataEntries :
                sKey = lDataEntries [PROFILE_GIVEN_DISPLAY_NAME].cValue
            else :
                return
            
            if nRecType == SKYPE_RECTYPE_PROFILE :
                dict = self.lUserProfs
            else :
                dict = self.lProfiles
            if not sKey in dict :
                dict [sKey] = SkpeUserData (self.SkypeDBUserName)
            for key in lDataEntries :
                dict [sKey].updateKey (key, lDataEntries [key])
                
        elif nRecType == SKYPE_RECTYPE_CALL or nRecType == SKYPE_RECTYPE_CALLMEMBER :
            if CALL_NAME in lDataEntries :
                (nCallAttempt, nTS) = self.parseCallAttemptTS (lDataEntries [CALL_NAME].cValue)
                callAttTSChunk      = lDataEntries [CALL_NAME].cChunk
            elif CALLMEMBERS_NAME in lDataEntries :
                (nCallAttempt, nTS) = self.parseCallAttemptTS (lDataEntries [CALLMEMBERS_NAME].cValue)
                callAttTSChunk      = lDataEntries [CALLMEMBERS_NAME].cChunk
            else :
                return
            
            if not nTS in self.lCallRecords :
                self.lCallRecords [nTS] = SkypeCallRecord ()

            if nRecType == SKYPE_RECTYPE_CALL :
                self.lCallRecords [nTS].nMemberRecordUID = nRecUID
            else :
                self.lCallRecords [nTS].nOthersRecordUID = nRecUID
            for key in lDataEntries :
                if key == CALL_NAME or key == CALLMEMBERS_NAME:
                    self.lCallRecords [nTS].nPOSIXTimeStamp  = ValueChunkTuple(nTS, callAttTSChunk)
                    self.lCallRecords [nTS].nCallAttempt     = ValueChunkTuple(nCallAttempt, callAttTSChunk)
                else :
                     self.lCallRecords [nTS].updateKey (key, lDataEntries [key])

        elif nRecType == SKYPE_RECTYPE_CHAT :
            if CHAT_CHATNAME in lDataEntries :
                sKey = lDataEntries [CHAT_CHATNAME].cValue
            else :
                return
            if not sKey in self.lChats :
                self.lChats [sKey] = SkypeChat ()
            for key in lDataEntries :
                self.lChats [sKey].updateKey (key, lDataEntries [key])

        elif nRecType == SKYPE_RECTYPE_CHATMSG :
            if CHATMSG_TIMESTAMP in lDataEntries :
                sKey = lDataEntries [CHATMSG_TIMESTAMP].cValue
            else :
                return
            if not CHATMSG_CHATNAME in lDataEntries:
                return
            sChatName     = lDataEntries [CHATMSG_CHATNAME].cValue
            if not sChatName in self.lChats :
                self.lChats [sChatName] = SkypeChat ()
            cSkypeChatMSG = SkypeChatMSG ()
            for key in lDataEntries :
                cSkypeChatMSG.updateKey (key, lDataEntries [key])
            self.lChats [sChatName].updateChatMessage (cSkypeChatMSG, sKey)

        elif nRecType == SKYPE_RECTYPE_SMS :
            cSkypeSMS = SkypeSMS (self.SkypeDBUserName)
            for key in lDataEntries :
                cSkypeSMS.updateKey (key, lDataEntries [key])
            self.lSMSs.append (cSkypeSMS)

    def parseDBBFile (self, cFile) :
        pMemNode = cFile.Data
        if not pMemNode or pMemNode.Length == 0 :
            return
        nLength = pMemNode.Length
        pMemNode.seek (0,0)
        nOffset = 0
        while nOffset < nLength :
            pMemNode.seek (nOffset)
            sRecMagic  = pMemNode.read (4)
            nOffset   += 1
            ## magic
            if not sRecMagic == SKYPE_RECORD_MAGIC :
                continue
            nOffset += 3
            ##  length
            pMemNode.seek (nOffset)
            nRecLen = unpack ("<I", pMemNode.read (4)) [0]
            nOffset += 4
            if nRecLen == 0 :
                continue
            pMemNode.seek (nOffset)
            pRecBuf = pMemNode.read (nRecLen)
            self.parseSkypeRecord (pRecBuf, nRecLen, pMemNode, nOffset)
            nOffset += nRecLen

    def getDSEntries (self, extractDeleted, extractSource) :
        results = []
        for cont in self.lProfiles:
            cContact = self.lProfiles [cont]
            results.append (cContact.generateContact (False, extractDeleted, extractSource))

        for usprof in self.lUserProfs:
            cProf = self.lUserProfs [usprof]
            results.append (cProf.generateContact (True, extractDeleted, extractSource))


        ## Call logs
        for call in self.lCallRecords :
            cCall = self.lCallRecords [call]
            results.append (cCall.generateCall (extractDeleted, extractSource))

        ## Chats
        for chat in self.lChats :
            cChat     = self.lChats [chat]
            cPhysChat = cChat.generateChat (extractDeleted, extractSource)
            if cPhysChat != None :
                results.append (cPhysChat)

        ## SMSs
        for cSMS in self.lSMSs :
            cPhysSMS = cSMS.generateSMS (extractDeleted, extractSource)
            if cPhysSMS != None :
                results.append (cPhysSMS)

        return results

def analyze_skype_v2(skype_dir, extractDeleted, extractSource):
    lSkypeUserDBs = {}

    for user_dir in skype_dir.Directories:
        sUser = user_dir.Name
        if not sUser in lSkypeUserDBs :
            lSkypeUserDBs [sUser] = SkypeUserDatabase (sUser)

        for f in user_dir.Glob('*.dbb'):
            lSkypeUserDBs [sUser].parseDBBFile (f)

    results = []
    for sUser in lSkypeUserDBs :
        results.extend(lSkypeUserDBs[sUser].getDSEntries(extractDeleted, extractSource))
    return results

