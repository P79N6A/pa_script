#coding=utf-8
import os
import PA_runtime
from PA_runtime import *

SKYPE_SYNC_FILE_MAGIC = 'sCdB\x07'

def analyze_skype_v1(node, extractDeleted, extractSource, unallocated = None): 
    pr = ParserResults()
    if node == None:
        pr += analyze_skype_maindb(node, extractDeleted, extractSource, unallocated)
    else: 
        for f in node.Search('/main\.db$'):
            pr += analyze_skype_maindb(f, extractDeleted, extractSource, unallocated)
        pr += analyze_skype_chatsync(node, extractDeleted, extractSource)
        pr += analyze_skype_plists(node,extractSource)
    return pr

def analyze_skype_plists(node,extractSource):
    pr = ParserResults()
    get_user_account(pr, node, extractSource)
    get_chats_from_plist(pr, node, extractSource)
    get_contacts_from_plist(pr, node, extractSource)
    return pr

def get_contacts_from_plist(pr, node, extractSource):
    if node is None:
        return pr

    for contactsfile in node.Search("/Documents/skype-cache-.*ContactsListAdapter-Contacts-SingleSelect\.plist$"):
        bp = BPReader.GetTree(contactsfile)
        curr_username = get_username_from_filename(contactsfile.Name)
        if bp == None:
            continue
        for con in bp['sections']:
            c = Contact()
            c.Deleted = contactsfile.Deleted
            c.Source.Value = "Skype: " + curr_username
            if con['objects'].Count != 0:
                if con['objects'][0]['title'] is not None:
                    c.Name.Init(con['objects'][0]['title'].Value, MemoryRange(con['objects'][0]['title'].Source) if extractSource else None)
                elif con['objects'][0]['titleAttributedString'] is not None and con['objects'][0]['titleAttributedString']['NSString'] is not None:
                    c.Name.Init(con['objects'][0]['titleAttributedString']['NSString'].Value, MemoryRange(con['objects'][0]['titleAttributedString']['NSString'].Source) \
                        if extractSource else None)
                uid = UserID()
                uid.Deleted = c.Deleted
                if con['objects'][0]['accessibilityIdentifier'] is not None:
                    uid.Value.Init(con['objects'][0]['accessibilityIdentifier'].Value, MemoryRange(con['objects'][0]['accessibilityIdentifier'].Source) if extractSource else None)
                elif con['objects'][0]['conversationIdentity'] is not None:
                    uid.Value.Init(con['objects'][0]['conversationIdentity'].Value, MemoryRange(con['objects'][0]['conversationIdentity'].Source) if extractSource else None)
                uid.Category.Value = "Skype Username"
                if uid.Value.HasContent:
                    c.Entries.Add(uid)
            if c.HasLogicalContent:
                pr.Models.Add(c)

    for contactsfile in node.Search("/Documents/skype-cache-.*PeoplePicker-Contacts\.plist$"):
        bp = BPReader.GetTree(contactsfile)
        curr_username = get_username_from_filename(contactsfile.Name)
        if bp == None:
            continue
        for con in bp['sections']:
            for ent in con['objects']:
                c = Contact()
                c.Deleted = contactsfile.Deleted
                c.Source.Value = "Skype: " + curr_username

                if ent['title'] is not None:
                    c.Name.Init(ent['title'].Value, MemoryRange(ent['title'].Source) if extractSource else None)
                uid = UserID()
                uid.Deleted = c.Deleted
                if ent['accessibilityIdentifier'] is not None:
                    uid.Value.Init(ent['accessibilityIdentifier'].Value, MemoryRange(ent['accessibilityIdentifier'].Source) if extractSource else None)
                uid.Category.Value = "Skype Username"
                c.Entries.Add(uid)
                if c.HasLogicalContent:
                    pr.Models.Add(c)

def get_username_from_filename(txt):
    index = txt.find('.') + 1
    return txt[index: txt.find('.',index)]

def get_chats_from_plist(pr, node, extractSource):
    if node is None:
        return pr
    time_re = re.compile("((?P<h>\d+)h)?\s*((?P<min>\d+)min)?\s*((?P<sec>\d+)s)?")
    for chatfile in node.Search("/Documents/skype-cache-.*chathistory-.*\.plist$"):        
        parties = {}
        calls = []
        c = Chat()
        c.Deleted = chatfile.Deleted
        c.Id.Value = get_chat_id_from_filename(chatfile.Name)
        c.Source.Value = "Skype: " + c.Id.Value.split(" ~")[0]
        bp = BPReader.GetTree(chatfile)
        if not bp or bp['sections'].Count == 0:
            continue
        for message in bp['sections'][0]['objects'].Value:
            im = InstantMessage()
            im.Deleted = chatfile.Deleted            
            im.From.Value = get_party_from_bp(message, parties, extractSource)
            im.TimeStamp.Value = TimeStamp(message['timestamp'].Value.Value, True)  
            if extractSource:
                im.TimeStamp.Source = MemoryRange(message['timestamp'].Source)
            if message['isDeleted'].Value == True:
                im.Deleted = DeletedState.Deleted
            if message.Class.Name == 'SKPTextChatViewModel': 
                try:
                    im.Body.Init(message['text']['NSString'].Value, MemoryRange(message['text']['NSString'].Source) if extractSource else None)
                except:
                    im.Body.Init(message['text']['rawXML'].Value, MemoryRange(message['text']['rawXML'].Source) if extractSource else None)
            elif message.Class.Name == 'SKPContactRequestChatViewModel': 
                try:
                    im.Body.Init(message['text']['NSString'].Value, MemoryRange(message['text']['NSString'].Source) if extractSource else None)
                except:
                    im.Body.Init(message['text']['rawXML'].Value, MemoryRange(message['text']['rawXML'].Source) if extractSource else None)
            elif message.Class.Name == 'SKPInlineChatViewModel':
                try:
                    im.Body.Init(message['attributedText']['NSString'].Value, MemoryRange(message['attributedText']['NSString'].Source) if extractSource else None)
                except:
                    im.Body.Init(message['attributedText']['rawXML'].Value, MemoryRange(message['attributedText']['rawXML'].Source) if extractSource else None)
            elif message.Class.Name == 'SKPMediaDocumentChatViewModel':
                att = Attachment()
                att.Deleted = c.Deleted
                att.URL.Init(message['fullScreenURL']['NS.relative'].Value, MemoryRange(message['fullScreenURL']['NS.relative'].Source) if extractSource else None)
                att.Filename.Init(att.URL)
                im.Attachments.Add(att)
            elif message.Class.Name == 'SKPCallEventChatViewModel':
                call = Call()
                call.Deleted = c.Deleted
                call.Source.Value = "Skype"
                if message['duration'].Value:                                      
                    time_result = time_re.search(message['duration'].Value)
                    if time_result:
                        hours = int(time_result.group('h')) if time_result.group('h') else 0
                        minutes = int(time_result.group('min')) if time_result.group('min') else 0
                        seconds = int(time_result.group('sec')) if time_result.group('sec') else 0                  
                        call.Duration.Init(TimeSpan(hours, minutes, seconds), MemoryRange(message['duration'].Source) if extractSource else None)
                    im.Body.Value = message['messageText'].Value + " duration: " + message['duration'].Value                    
                else:
                    call.Duration.Value = TimeSpan(0)
                    im.Body.Value = message['messageText'].Value
                
                if extractSource:
                    im.Body.Source = MemoryRange(message['messageText'].Source)

                if message["isIncoming"].Value == False:
                    call.Type.Value = CallType.Outgoing
                elif call.Duration.Value == TimeSpan(0):
                    call.Type.Value = CallType.Missed
                else:
                    call.Type.Value = CallType.Incoming

                call.TimeStamp.Init(im.TimeStamp)
                call.Parties.Add(im.From.Value)
                calls.append(call)
                pr.Models.Add(call)
                im.LinkModels(call)
            elif message.Class.Name == 'SKPLocationChatViewModel':
                if message['address'] is not None and message['address'].Value is not None:
                    im.Body.Init(message['address'].Value, MemoryRange(message['address'].Source))
                if message['location'] is not None and message['location'].Value is not None:
                    cor  = Coordinate(message['location']['kCLLocationCodingKeyCoordinateLatitude'].Value, message['location']['kCLLocationCodingKeyCoordinateLongitude'].Value)
                    if extractSource:
                        cor.Latitude.Source = MemoryRange(message['location']['kCLLocationCodingKeyCoordinateLatitude'].Source)
                        cor.Longitude.Source = MemoryRange(message['location']['kCLLocationCodingKeyCoordinateLongitude'].Source)
                    im.Position.Value = cor
                    l = Location()
                    l.Category.Value = "Skype"
                    l.Position.Value = cor
                    l.Description.Init(im.Body)
                    l.TimeStamp.Init(im.TimeStamp)
                    l.Deleted = im.Deleted
                    pr.Models.Add(l)
                    l.LinkModels(im)
            else:
                continue
            c.Messages.Add(im)
        if c.Messages.Count > 0:
            c.SetTimesByMessages()
            c.SetParticipantsByMessages()
            get_second_party(c, calls)
            pr.Models.Add(c)

def get_second_party(c, calls):
    for call in calls:
        p1 = call.Parties[0].Identifier.Value
        for p in c.Participants:
            if p.Identifier.Value != p1:
                p2 = Party()
                p2.Name.Init(p.Name)
                p2.Identifier.Init(p.Identifier)
                p2.Deleted = p.Deleted
                p2.Role.Value = PartyRole.To
                call.Parties.Add(p2)

def get_chat_id_from_filename(filename):
    try:
        i = filename.find('.')
        p1 = filename[i + 1: filename.find('.', i+1)]
        j = filename.find('chathistory-viewmodels-')
        p2 = filename[j + len('chathistory-viewmodels-'): filename.find('.',j+1)]
        return p1 + " ~ " + p2
    except:
        return ""

def get_party_from_bp(message, parties, extractSource):
    if parties.has_key(message['unescapedAuthorDisplayName'].Value):
        return parties[message['unescapedAuthorDisplayName'].Value]
    p = Party()
    parties[message['unescapedAuthorDisplayName'].Value] = p
    p.Deleted = DeletedState.Intact
    p.Role.Value = PartyRole.From
    p.Identifier.Init(message['unescapedAuthorDisplayName'].Value, MemoryRange(message['unescapedAuthorDisplayName'].Source) if extractSource else None)
    try:
        p.Name.Init(message['authorSkypeName'].Value, MemoryRange(message['authorSkypeName'].Source) if extractSource else None)
    except:
        p.Name.Init(message['author'].Value, MemoryRange(message['author'].Source) if extractSource else None)
    return p

def get_user_account(pr,node,extractSource):
    if node is None:
        return pr
    bpnode = node.GetByPath('Library/Preferences/com.skype.skype.plist')
    if bpnode is None:
        return pr
    bp = BPReader.GetTree(bpnode)
    if not bp:
        return pr
    if bp.ContainsKey('lastLoggedInSkypeName'):
        ua = UserAccount()        
        ua.Deleted = node.Deleted
        ua.ServiceType.Value = 'Skype'
        ua.Name.Init(bp['lastLoggedInSkypeName'].Value,MemoryRange(bp['lastLoggedInSkypeName'].Source) if extractSource else None)

        if bp.ContainsKey('LocationManagerCountryCode'):
            c = UserID()
            c.Category.Value = 'Country'
            c.Deleted = bp.Deleted
            c.Value.Init(bp['LocationManagerCountryCode'].Value,MemoryRange(bp['LocationManagerCountryCode'].Source) if extractSource else None)
            ua.Entries.Add(c)

        pr.Models.Add(ua)
    return pr

def get_user_party(ua):
    party = Party()
    party.Deleted = ua.Deleted
    party.Name.Init(ua.Name.Value, ua.Name.Source)
    party.Identifier.Init(ua.Username.Value, ua.Username.Source)
    return party

def get_skype_favorites_contact(node):
    result = {}
    if node == None or node.Parent == None:
        return result
    db_node = node.Parent.GetByPath('eascache.db')
    if not db_node: return result
    db = SQLiteParser.Database.FromNode(db_node)
    if not db or 'fullobjects' not in db.Tables: return result
    for rec in db['fullobjects']:
        if 'Contacts' not in rec or 'FavoriteOrder' not in rec:
            return result
        if type(rec['FavoriteOrder'].Value) != str or rec['FavoriteOrder'].Value != '2592':
            continue
        if type(rec['Contacts'].Value) != str or rec['Contacts'].Value.find('<OID>') == -1:
            continue
        contacts = rec['Contacts'].Value
        contact_id = contacts[contacts.find('<OID>') + 5:contacts.find('</OID>')]
        result[contact_id] = rec['FavoriteOrder'].Source
    return result

def analyze_skype_maindb(node, extractDeleted, extractSource, unallocated):
    favorite_contacts = get_skype_favorites_contact(node)
    emoticon_fixer = re.compile('<ss type=".*?">(.*?)</ss>')

    NUMBERS = [
        ('pstnnumber', ContactCategory.General),
        ('phone_home', ContactCategory.Home),
        ('phone_office', ContactCategory.Work),
        ('phone_mobile', ContactCategory.Mobile),
        ]
    db = None
    if node is not None:        
        db = Database.FromNode(node)
        if db is None and unallocated is None:
            return
    fs = node
    results = []
    users = []
    curr_user = None
    if db is not None and 'Accounts' in db.Tables:
        ts = TableSignature('Accounts')        
        if extractDeleted:
            ts['fullname'] = ts['skypename'] = Signatures.SignatureFactory.GetFieldSignature(FieldType.Text, FieldConstraints.NotNull)
            ts['about'] = ts['mood_text'] = ts['city'] = ts['province'] = ts['country'] = ts['emails'] = ts['homepage'] = Signatures.SignatureFactory.GetFieldSignature(FieldType.Text)
        for rec in db.ReadTableRecords(ts, extractDeleted):
            if rec.Deleted == DeletedState.Deleted and curr_user != None:
                continue
            ua = UserAccount()
            ua.Deleted = rec.Deleted
            if 'skypename' in rec and not IsDBNull (rec['skypename'].Value):
                if rec['skypename'].Value in users:
                    continue
                ua.Username.Value = rec['skypename'].Value
                if extractSource:
                    ua.Username.Source = MemoryRange(rec['skypename'].Source)
                users.append(rec['skypename'].Value)
            if 'fullname' in rec and not IsDBNull (rec['fullname'].Value):
                ua.Name.Value = rec['fullname'].Value
                if extractSource:
                    ua.Name.Source = MemoryRange(rec['fullname'].Source)
            ua.ServiceType.Value = 'Skype'

            for name, cat in NUMBERS:
                if not IsDBNull(rec[name].Value) and rec[name].Value != '':
                    phone = PhoneNumber(rec[name].Value, cat)
                    if extractSource:
                        phone.Value.Source = MemoryRange(rec[name].Source)
                    phone.Deleted = rec.Deleted
                    ua.Entries.Add(phone)

            if not IsDBNull(rec['emails'].Value) and rec['emails'].Value != '':
                email = EmailAddress(rec['emails'].Value, ContactCategory.General)
                if extractSource:
                    email.Value.Source = MemoryRange(rec['emails'].Source)
                email.Deleted = rec.Deleted                                     
                ua.Entries.Add(email)

            if not IsDBNull(rec['homepage'].Value) and rec['homepage'].Value != '':
                web = WebAddress(rec['homepage'].Value, ContactCategory.General)
                if extractSource:
                    web.Value.Source = MemoryRange(rec['homepage'].Source)
                web.Deleted = rec.Deleted
                ua.Entries.Add(web)

            if not IsDBNull(rec['about'].Value) and rec['about'].Value != '':
                if extractSource:
                    ua.Notes.Add("about: {0}".format(rec['about'].Value), MemoryRange(rec['about'].Source))
                else:
                    ua.Notes.Add("about: {0}".format(rec['about'].Value))

            if not IsDBNull(rec['mood_text'].Value) and rec['mood_text'].Value != '':
                if extractSource:
                    ua.Notes.Add("mood text: {0}".format(rec['mood_text'].Value), MemoryRange(rec['mood_text'].Source))
                else:
                    ua.Notes.Add("mood text: {0}".format(rec['mood_text'].Value))

            is_add = False
            address = StreetAddress()
            address.Deleted = rec.Deleted
            address.Category.Value = ContactCategory.General
            if not IsDBNull(rec['city'].Value) and rec['city'].Value != '':
                address.City.Value = rec['city'].Value
                if extractSource:
                    address.City.Source = MemoryRange(rec['city'].Source)
                is_add = True
            if not IsDBNull(rec['country'].Value) and rec['country'].Value != '':
                address.Country.Value = rec['country'].Value
                if extractSource:
                    address.Country.Source = MemoryRange(rec['country'].Source)
                is_add = True
            if not IsDBNull(rec['province'].Value) and rec['province'].Value != '':
                address.State.Value = rec['province'].Value
                if extractSource:
                    address.State.Source = MemoryRange(rec['province'].Source)
                is_add = True
            if is_add:
                ua.Addresses.Add(address)

            if not IsDBNull(rec['avatar_image'].Value):
                m = MemoryRange(rec['avatar_image'].Source)
                if m.Length > 0:
                    pic = ContactPhoto()
                    pic.Deleted = rec.Deleted
                    node = Node(NodeType.File)
                    node.Data = m.GetSubRange(1, m.Length - 1)
                    pic.PhotoNode.Value = node
                    ua.Photos.Add(pic)

                    f = Node('{0}.jpg'.format(ua.Name.Value), NodeType.File | NodeType.Embedded)
                    f.Data = m.GetSubRange(1, m.Length - 1)
                    f.Deleted = rec.Deleted
                    fs.Children.Add(f)
            curr_user = ua            
            results.append(ua)
        
    ts = TableSignature('SMSes')
    if extractDeleted:
        ts['body'] = ts['target_numbers'] = Signatures.SignatureFactory.GetFieldSignature(FieldType.Text, FieldConstraints.NotNull)
        ts['status'] = Signatures.NumericSet(1)
        ts['timestamp'] = Signatures.NumericSet(1, 4, 7)
    records = []
    if db is not None and 'SMSes' in db.Tables:
        records.extend(db.ReadTableRecords(ts, extractDeleted, True))
    if unallocated is not None:
        records.extend(unallocated.carvedRecords[SQLiteParser.UnallocatedCarving.Content.Skype_SMSes])
    for rec in records:
        if IsDBNull(rec['body'].Value) or len(rec['body'].Value) == 0:
            continue

        sms = SMS()
        sms.Deleted = rec.Deleted
        sms.Source.Value = 'Skype'
        sms.CarveSource = rec.CarveSource
        if not IsDBNull(rec['status'].Value):
            if rec['status'].Value == 3:
                sms.Status.Value = MessageStatus.Unsent
                sms.Folder.Value = Folders.Sent
                if extractSource:
                    sms.Status.Source = MemoryRange(rec['status'].Source)
            elif rec['status'].Value == 6:
                sms.Status.Value = MessageStatus.Sent
                sms.Folder.Value = Folders.Sent
                if extractSource:
                    sms.Status.Source = MemoryRange(rec['status'].Source)
            else:
                sms.Folder.Value = Folders.Inbox

        if not IsDBNull (rec['timestamp'].Value) and int(rec['timestamp'].Value) > 0:
            time = TimeStamp.FromUnixTime(rec['timestamp'].Value)
            #if time.Value.Year < 2017 and time.Value.Year > 2005:
            if time.IsValidForSmartphone():
                sms.TimeStamp.Value = time
                if extractSource:
                    sms.TimeStamp.Source = MemoryRange(rec['timestamp'].Source)
            
        if not IsDBNull(rec['target_numbers'].Value):
            if extractSource:
                sms.Parties.Add(Party.MakeTo(rec['target_numbers'].Value, MemoryRange(rec['target_numbers'].Source)))
            else:
                sms.Parties.Add(Party.MakeTo(rec['target_numbers'].Value, None))
            
        sms.Body.Value = rec['body'].Value
        if extractSource:
            sms.Body.Source = MemoryRange(rec['body'].Source)

        results.append(sms)

    contacts = {}
    tempResults = set()
    if db is not None and 'Contacts' in db.Tables:
        ts = TableSignature('Contacts')
        if extractDeleted:
            ts['about'] = ts['mood_text'] = ts['city'] = ts['province'] = ts['country'] = ts['fullname'] = ts['skypename'] = ts['displayname'] = ts['emails'] = ts['homepage'] = Signatures.SignatureFactory.GetFieldSignature(FieldType.Text)
        for rec in db.ReadTableRecords(ts, extractDeleted):
            if ('given_authlevel' in rec and (IsDBNull(rec['given_authlevel'].Value) or rec['given_authlevel'].Value==0)) and ('group_membership' not in rec or IsDBNull(rec['group_membership'].Value) or rec['group_membership'].Value==0):
                continue
            c = Contact()
            c.Deleted = rec.Deleted
            c.Source.Value = 'Skype: {0}'.format(fs.Parent.Name)
            if not IsDBNull(rec['fullname'].Value) and rec['fullname'].Value != '':
                c.Name.Value = rec['fullname'].Value
                if extractSource:
                    c.Name.Source = MemoryRange(rec['fullname'].Source)
            elif not IsDBNull(rec['skypename'].Value) and rec['skypename'].Value != '':
                c.Name.Value = rec['skypename'].Value
                if extractSource:
                    c.Name.Source = MemoryRange(rec['skypename'].Source)
            elif not IsDBNull(rec['displayname'].Value) and rec['displayname'].Value != '':
                c.Name.Value = rec['displayname'].Value
                if extractSource:
                    c.Name.Source = MemoryRange(rec['displayname'].Source)
            else:
                continue

            if not IsDBNull(rec['skypename'].Value):
                uid = UserID()
                uid.Value.Value = rec['skypename'].Value
                if uid.Value.Value in favorite_contacts:
                    c.Type.Init(ContactType.Favorite, MemoryRange(favorite_contacts[uid.Value.Value]) if extractSource else None)
                if extractSource:
                    uid.Value.Source = MemoryRange(rec['skypename'].Source)
                uid.Deleted = rec.Deleted
                uid.Category.Value = 'Skype'
                c.Entries.Add(uid)
                contacts[rec['skypename'].Value] = rec['fullname']

            for name, cat in NUMBERS:
                if not IsDBNull(rec[name].Value) and rec[name].Value != '':
                    phone = PhoneNumber(rec[name].Value, cat)
                    if extractSource:
                        MemoryRange(rec[name].Source)
                    phone.Deleted = rec.Deleted
                    c.Entries.Add(phone)

            if not IsDBNull(rec['emails'].Value) and rec['emails'].Value != '':
                email = EmailAddress(rec['emails'].Value, ContactCategory.General)
                if extractSource:
                    email.Value.Source = MemoryRange(rec['emails'].Source)
                email.Deleted = rec.Deleted                                     
                c.Entries.Add(email)

            if not IsDBNull(rec['homepage'].Value) and rec['homepage'].Value != '':
                web = WebAddress(rec['homepage'].Value, ContactCategory.General)
                if extractSource:  
                    web.Value.Source = MemoryRange(rec['homepage'].Source)
                web.Deleted = rec.Deleted
                c.Entries.Add(web)

            if not IsDBNull(rec['about'].Value) and rec['about'].Value != '':
                if extractSource:
                    c.Notes.Add("about: {0}".format(rec['about'].Value), MemoryRange(rec['about'].Source))
                else:
                    c.Notes.Add("about: {0}".format(rec['about'].Value))

            if not IsDBNull(rec['mood_text'].Value) and rec['mood_text'].Value != '':
                if extractSource:
                    c.Notes.Add("mood text: {0}".format(rec['mood_text'].Value), MemoryRange(rec['mood_text'].Source))
                else:
                    c.Notes.Add("mood text: {0}".format(rec['mood_text'].Value))

            is_add = False
            address = StreetAddress()
            address.Deleted = rec.Deleted
            address.Category.Value = ContactCategory.General
            if not IsDBNull(rec['city'].Value) and rec['city'].Value != '':
                address.City.Value = rec['city'].Value
                if extractSource: 
                    address.City.Source = MemoryRange(rec['city'].Source)
                is_add = True
            if not IsDBNull(rec['country'].Value) and rec['country'].Value != '':
                address.Country.Value = rec['country'].Value
                if extractSource:
                    address.Country.Source = MemoryRange(rec['country'].Source)
                is_add = True
            if not IsDBNull(rec['province'].Value) and rec['province'].Value != '':
                address.State.Value = rec['province'].Value
                if extractSource:
                    address.State.Source = MemoryRange(rec['province'].Source)
                is_add = True
            if is_add:
                c.Addresses.Add(address)

            if not IsDBNull(rec['avatar_image'].Value):
                m = MemoryRange(rec['avatar_image'].Source)
                if m.Length > 0:
                    pic = ContactPhoto()
                    pic.Deleted = rec.Deleted 
                    node = Node(NodeType.File)
                    node.Data = m.GetSubRange(1, m.Length - 1)
                    pic.PhotoNode.Value = node
                    c.Photos.Add(pic)

                    f = Node('{0}'.format(c.Name.Value), NodeType.File | NodeType.Embedded)
                    f.Data = m.GetSubRange(1, m.Length - 1)
                    f.Deleted = rec.Deleted
                    fs.Children.Add(f)

            if c not in tempResults:
                tempResults.add(c)
        results.extend(tempResults)

    calls = {}
    calls_hosts = {}
    deletedCalls = []
    ts = TableSignature('Calls')
    if extractDeleted:
        ts['begin_timestamp'] = Signatures.NumericSet(4)
        ts['duration'] = Signatures.NumericSet(0, 1, 2)
        ts['is_incoming'] = Signatures.NumericSet(1)
    records = []
    if db is not None and 'Calls' in db.Tables:
        records.extend(db.ReadTableRecords(ts, extractDeleted, True))
    if unallocated is not None:
        records.extend(unallocated.carvedRecords[SQLiteParser.UnallocatedCarving.Content.Skype_Calls])
    for rec in records:
        if rec['id'].Value != 0 and rec['id'].Value not in calls:
            calls[rec['id'].Value] = Call()
            c = calls[rec['id'].Value]
        else:
            c = Call()
            deletedCalls.append(c)        
        c.Deleted = rec.Deleted
        if fs is not None:
            c.Source.Value = 'Skype: {0}'.format(fs.Parent.Name)
        else:
            c.Source.Value = "Skype"
        c.CarveSource = rec.CarveSource
        if not IsDBNull(rec['begin_timestamp'].Value):
            if rec['begin_timestamp'].Value > 0 or rec.Deleted == DeletedState.Intact:
                time = TimeStamp.FromUnixTime(rec['begin_timestamp'].Value)
            if time.IsValidForSmartphone():
                c.TimeStamp.Value = time
                if extractSource:
                    c.TimeStamp.Source = MemoryRange(rec['begin_timestamp'].Source)

        if not IsDBNull(rec['duration'].Value):
            c.Duration.Value = TimeSpan.FromSeconds(rec['duration'].Value)
            if extractSource:
                c.Duration.Source = MemoryRange(rec['duration'].Source)

        if not IsDBNull(rec['is_incoming']):
            if rec['is_incoming'].Value:
                c.Type.Value = CallType.Incoming               
                if rec.ContainsKey('host_identity') and not IsDBNull(rec["host_identity"].Value) and curr_user is not None and rec["host_identity"].Value != curr_user.Username.Value:
                    calls_hosts[rec['id'].Value] = rec['host_identity'].Value
            else:
                c.Type.Value = CallType.Outgoing
            if extractSource:
                c.Type.Source = MemoryRange(rec['is_incoming'].Source)
        if 'is_unseen_missed' in rec and not IsDBNull(rec['is_unseen_missed']):
            if rec['is_unseen_missed'].Value == 1:
                c.Type.Value = CallType.Missed
                if extractSource:
                    c.Type.Source = MemoryRange(rec['is_unseen_missed'].Source)

    if db is not None and 'CallMembers' in db.Tables:        
        ts = TableSignature('CallMembers')
        if extractDeleted:
            ts['identity'] = ts['dispname'] = Signatures.SignatureFactory.GetFieldSignature(FieldType.Text, FieldConstraints.NotNull)
        
        for rec in db.ReadTableRecords(ts, extractDeleted):
            if curr_user is not None:
                user_party = get_user_party(curr_user)
            else:
                user_party = None
            if rec['call_db_id'].Value not in calls:                
                continue
            c = calls[rec['call_db_id'].Value]            
            
            p = Party()
            p.Identifier.Value = rec['identity'].Value
            if extractSource:
                p.Identifier.Source = MemoryRange(rec['identity'].Source)
            p.Name.Value = rec['dispname'].Value
            if extractSource:
                p.Name.Source = MemoryRange(rec['dispname'].Source)
            if c.Type.Value == CallType.Incoming or c.Type.Value == CallType.Missed:
                if not calls_hosts.has_key(rec['call_db_id'].Value)  or (calls_hosts.has_key(rec['call_db_id'].Value) and calls_hosts[rec['call_db_id'].Value] == p.Identifier.Value):
                    p.Role.Value = PartyRole.From
                else:
                    p.Role.Value = PartyRole.To
                if user_party is not None:
                    user_party.Role.Value = PartyRole.To
            elif c.Type.Value == CallType.Outgoing:
                p.Role.Value = PartyRole.To
                if user_party is not None:
                    user_party.Role.Value = PartyRole.From        
            # Overwrite call type as missed if start_timestamp is 0
            if 'start_timestamp' in rec and not IsDBNull(rec['start_timestamp'].Value) and rec['start_timestamp'].Value == 0:
                c.Type.Value = CallType.Missed
                if extractSource:
                    c.Type.Source = MemoryRange(rec['start_timestamp'].Source)
                        
            if 'ip_address' in rec and not IsDBNull(rec['ip_address'].Value):
                KTree.KNodeTools.TryReadToMultiField(rec,'ip_address',p.IPAddresses,extractSource)                
            if p.Identifier.Value not in [party.Identifier.Value for party in c.Parties.Items]:
                c.Parties.Add(p)
            if 'debuginfo' in rec and not IsDBNull(rec['debuginfo'].Value):
                c.VideoCall.Init(rec['debuginfo'].Value.find('Video capture:') != -1 or rec['debuginfo'].Value.find('video send') != -1 or rec['debuginfo'].Value.find('video recv') != -1,
                                 MemoryRange(rec['debuginfo'].Source) if extractSource else None)
            if user_party is not None and not c.Parties.Contains(user_party):
                c.Parties.Add(user_party)   
               
    
    results.extend(calls.values())
    results.extend(deletedCalls)

    chats = {}
    chats_names = {}
    messages = {}
    participants = {}
    if db is not None and 'Participants' in db.Tables:
        ts = TableSignature('Participants')
        if extractDeleted:
            ts['identity'] = Signatures.SignatureFactory.GetFieldSignature(FieldType.Text, FieldConstraints.NotNull)
        for rec in db.ReadTableRecords(ts, extractDeleted):   
            if IsDBNull(rec['identity'].Value):
                continue
            if IsDBNull(rec['convo_id'].Value):
                continue
            if rec['convo_id'].Value not in participants:
                participants[rec['convo_id'].Value] = []
            party = Party.MakeGeneral(rec['identity'].Value, None)
            if extractSource:
                party.Identifier.Source = MemoryRange(rec['identity'].Source)
            if rec['identity'].Value in contacts and not IsDBNull(contacts[rec['identity'].Value].Value):                    
                party.Name.Value = contacts[rec['identity'].Value].Value
                if extractSource:
                    party.Name.Source = MemoryRange(contacts[rec['identity'].Value].Source)
            if party.Identifier.Value not in [p.Identifier.Value for p in participants[rec['convo_id'].Value]]:
                participants[rec['convo_id'].Value].append(party)

    if db is not None and 'Chats' in db.Tables:
        ts = TableSignature('Chats')
        if extractDeleted:
            ts['name'] = ts['friendlyname'] = ts['dialog_partner'] = ts['posters'] = ts['participants'] = ts['activemembers'] = ts['split_friendlyname'] = Signatures.SignatureFactory.GetFieldSignature(FieldType.Text)
            ts['activity_timestamp'] = ts['last_change'] = ts['timestamp'] = Signatures.NumericSet(4)
            ts['conv_dbid'] = Signatures.SignatureFactory.GetFieldSignature(FieldType.Int)
        for rec in db.ReadTableRecords(ts, extractDeleted, True):
            if IsDBNull(rec['conv_dbid'].Value): 
                continue
            conv_id = rec['conv_dbid'].Value
            if conv_id in chats:
                continue
            chats[conv_id] = Chat()
            if 'name' in rec and not IsDBNull(rec['name'].Value):
                chats_names[conv_id] = rec['name'].Value
            chats[conv_id].Deleted = rec.Deleted
            chats[conv_id].Source.Value = 'Skype: {0}'.format(fs.Parent.Name)
            messages[conv_id] = set() 
            if conv_id in participants:
                chats[conv_id].Participants.AddRange(participants[conv_id])  
    
    attachments = {}
    if db is not None and 'Transfers' in db.Tables:
        ts = TableSignature('Transfers')
        if extractDeleted:
            ts['partner_handle'] = ts['paretner_dispname'] = ts['filename'] = ts['filepath'] = Signatures.SignatureFactory.GetFieldSignature(FieldType.Text, FieldConstraints.NotNull)
            ts['starttime'] = ts['finishtime'] = ts['accepttime'] = Signatures.NumericSet(0, 4, 8)
            ts['convo_id'] = Signatures.SignatureFactory.GetFieldSignature(FieldType.Int, FieldConstraints.NotNull)
        for rec in db.ReadTableRecords(ts, extractDeleted, True):
            if IsDBNull(rec['convo_id'].Value):
                continue            
            if IsDBNull(rec['filename'].Value) or IsDBNull(rec['filepath'].Value):
                continue
            if rec['convo_id'].Value not in attachments:
                attachments[rec['convo_id'].Value] = {}
            atts = attachments[rec['convo_id'].Value]
            if rec['filename'].Value in atts:
                continue            
            att = Attachment()
            att.Deleted = rec.Deleted            
            att.Filename.Value = rec['filename'].Value
            f = None
            if extractSource:
                att.Filename.Source = MemoryRange(rec['filename'].Source)
            for f in fs.Parent.Parent.Parent.SearchNodesExactPath(rec['filename'].Value):
                if f != None and f.Data != None and att.Data.Source == None:
                    att.Data.Source = f.Data
                    break
            if att.Data.Source is None:
                for f in fs.Parent.Parent.Parent.Parent.Parent.SearchNodesExactPath(rec['filename'].Value):
                    if f != None and f.Data != None and att.Data.Source == None:
                        att.Data.Source = f.Data
                        break
            if att.Data.Source is None:
                for filesystem in ds.FileSystems:
                    for f in list(filesystem.SearchNodesExactPath(rec['filename'].Value)) + list(filesystem.SearchNodesExactPath(rec['filepath'].Value.rsplit('/', 1)[1])):
                        if f != None and f.Data != None and att.Data.Source == None and f.Data.Length == int(rec['filesize'].Value):
                            att.Data.Source = f.Data
                            break
            if att.Data.Source is None:
                path = rec['filename'].Value.rsplit('.')
                if len(path) == 2:
                    for f in fs.Parent.Parent.Parent.Parent.Parent.SearchNodesExactPath("{0}-{1}.{2}".format(path[0], rec['id'].Value, path[1])):
                        if f != None and f.Data != None and att.Data.Source == None:
                            att.Data.Source = f.Data
                            break
            if att.Filename.Value != None or att.Data.Source != None:                
                atts[rec['filename'].Value] = (att , f)          
    videos = {}
    if db is not None and 'VideoMessages' in db.Tables:
        ts = TableSignature('VideoMessages')
        if extractDeleted:
            ts['sharing_id'] = ts['author'] = ts['local_path'] = ts['public_link'] = Signatures.SignatureFactory.GetFieldSignature(FieldType.Text, FieldConstraints.NotNull)
            ts['title'] = Signatures.SignatureFactory.GetFieldSignature(FieldType.Text)
            ts['creation_timestamp'] = Signatures.NumericSet(4)
        for rec in db.ReadTableRecords(ts, extractDeleted):
            if IsDBNull(rec['sharing_id'].Value):
                continue
            if IsDBNull(rec['local_path'].Value):
                continue
            if rec['sharing_id'].Value in videos:
                continue
            videos[rec['sharing_id'].Value] = rec

    ts = TableSignature('Messages')
    if extractDeleted:
        ts['chatname'] = ts['body_xml'] = ts['from_dispname'] = Signatures.SignatureFactory.GetFieldSignature(FieldType.Text, FieldConstraints.NotNull)
        ts['timestamp'] = Signatures.NumericSet(4)            
    records = []
    if db is not None and 'Messages' in db.Tables:
        records.extend(db.ReadTableRecords(ts, extractDeleted))
    if unallocated is not None:
        records.extend(unallocated.carvedRecords[SQLiteParser.UnallocatedCarving.Content.Skype_Messages])
    for rec in records:
        if IsDBNull(rec['chatname'].Value):
            continue
        if IsDBNull(rec['type'].Value):
            continue
        conv_id = rec['convo_id'].Value
        if conv_id not in chats:
            chats[conv_id] = Chat()
            chats[conv_id].Deleted = DeletedState.Intact
            if conv_id in participants:
                chats[conv_id].Participants.AddRange(participants[conv_id])  
            messages[conv_id] = set()
            if fs is not None:
                chats[conv_id].Source.Value = 'Skype: {0}'.format(fs.Parent.Name)
            else:
                chats[conv_id].Source.Value = "Skype"

        if rec['type'].Value not in [50, 61, 68, 70, 201, 202, 255, 63, 254]:
            continue           
        im = InstantMessage()
        im.Deleted = rec.Deleted
        if 'chatname' in rec and not IsDBNull(rec['chatname'].Value) and conv_id in chats_names:
            if rec['chatname'].Value == chats_names[conv_id]:
                im.Platform.Value = Enums.Platform.Mobile
            else:
                im.Platform.Value = Enums.Platform.PC
        im.SourceApplication.Value = chats[conv_id].Source.Value
        im.CarveSource = rec.CarveSource        

        if not IsDBNull(rec['timestamp'].Value):
            try:
                time = TimeStamp.FromUnixTime(rec['timestamp'].Value)
                if time.IsValidForSmartphone():
                    im.TimeStamp.Value = time
                    if extractSource:
                        im.TimeStamp.Source = MemoryRange(rec['timestamp'].Source)
            except:
                pass

        # Text Message
        if rec['type'].Value in [50, 61] and not IsDBNull(rec['body_xml'].Value):
            if rec['body_xml'].Value != "":
                im.Body.Value = emoticon_fixer.sub('\\1', HTMLParser.HTMLParser().unescape(rec['body_xml'].Value))
                if extractSource:
                    im.Body.Source = MemoryRange(rec['body_xml'].Source)
            else:
                im.Deleted = DeletedState.Deleted
        elif rec['type'].Value == 68 and not IsDBNull(rec['body_xml'].Value):
            try:
                if not rec['body_xml'].Value.startswith('<files'):
                    xml_start = rec['body_xml'].Value.index('<files')
                    im.Body.Value = rec['body_xml'].Value[:xml_start]
                    if extractSource:
                        im.Body.Source = MemoryRange(rec['body_xml'].Source)
                    doc = XDocument.Parse(rec['body_xml'].Value[xml_start:])
                else:
                    doc = XDocument.Parse(rec['body_xml'].Value)                                
                elem = doc.Element('files')
                if elem != None:
                    elem = elem.Element('file')
                    if elem != None:
                        idx = elem.Value
                        if rec['convo_id'].Value in attachments and idx in attachments[rec['convo_id'].Value]:
                            attachment_att, attachment_node = attachments[rec['convo_id'].Value][idx]
                            CreateSourceEvent(attachment_node, im)
                            im.Attachments.Add(attachment_att)
            except:
                pass
        # Video Message
        elif rec['type'].Value == 70 and not IsDBNull(rec['body_xml'].Value):
            try:
                doc = XDocument.Parse(rec['body_xml'].Value)                
                elem = doc.Element('videomessage')                
                if elem != None:
                    if elem.Value != None and elem.Value != '':
                        im.Body.Value = elem.Value
                        if extractSource:
                            im.Body.Source = MemoryRange(rec['body_xml'].Source)
                    if elem.Attribute('sid') != None and elem.Attribute('sid').Value in videos:
                        vid_rec = videos[elem.Attribute('sid').Value]                    
                        for filesystem in ds.FileSystems:
                            for f in filesystem.SearchNodesExactPath(vid_rec['local_path'].Value):
                                if f != None and f.Data != None:
                                    att = Attachment()
                                    att.Deleted = vid_rec.Deleted
                                    att.Filename.Value = f.Name
                                    att.Data.Source = f.Data
                                    CreateSourceEvent(f, im)
                                    im.Attachments.Add(att)
                                    break
                            if im.Attachments.Count > 0:
                                break
            except:
                pass
        # Image Message / Video Message (of type 255)
        elif rec['type'].Value in [201,255] and not IsDBNull(rec['body_xml'].Value):
            imgContentType = None            
            try:
                doc = XDocument.Parse(rec['body_xml'].Value)  
                elems = doc.Descendants('Text')                               
                if len(list(elems)) == 0:   
                    elems = doc.Descendants('URIObject')                                                        
                for elem in elems:
                    if elem.Value is not None and elem.Value != '':
                        im.Body.Value = elem.Value
                        if extractSource:
                            im.Body.Source = MemoryRange(rec['body_xml'].Source)
                uriObjElem = doc.Element('URIObject')
                if uriObjElem is not None and uriObjElem.Attribute('content-type') is not None:
                    imgContentType = uriObjElem.Attribute('content-type').Value               
            except:
                if rec['body_xml'].Value == "":
                    ## as seen in a BlackBerry10 Backup - see ticket #1157680
                    im.Deleted = DeletedState.Deleted

            if not IsDBNull(rec['author'].Value) and not IsDBNull(rec['call_guid'].Value):
                # Genereate image filename
                m = md5()
                m.update('mediadocument://{0}@original_image/{1}'.format(rec['author'].Value, rec['call_guid'].Value))
                m_thumb = md5()
                m_thumb.update('mediadocument://{0}@thumbnail/{1}?size=%257B348,%2520340%257D'.format(rec['author'].Value, rec['call_guid'].Value))
                found_att = False

                if fs is not None:
                    f_name = m.hexdigest()
                    files = fs.FileSystem.SearchNodesExactPath(f_name)
                    for file in files:
                        if '/Caches/' in file.AbsolutePath:
                            att = Attachment()
                            att.Deleted = rec.Deleted
                            att.Data.Source = file.Data
                            CreateSourceEvent(file, im)
                            att.Filename.Value = f_name
                            if imgContentType is not None:
                                att.ContentType.Value = imgContentType
                            im.Attachments.Add(att)
                            found_att = True
                            break
                    
                    if not found_att:
                        f_name_thumb = m_thumb.hexdigest()
                        files_thumbs = fs.FileSystem.SearchNodesExactPath(f_name_thumb)
                        for file in files_thumbs:
                            if '/Caches/' in file.AbsolutePath:
                                att = Attachment()
                                att.Deleted = rec.Deleted
                                att.Data.Source = file.Data
                                CreateSourceEvent(file, im)
                                att.Filename.Value = f_name_thumb
                                if imgContentType is not None:
                                    att.ContentType.Value = imgContentType
                                im.Attachments.Add(att)
                                found_att = True
                                break
        
        elif rec['type'].Value == 202 and not IsDBNull(rec['body_xml'].Value):
            if rec['body_xml'].Value == "":
                im.Deleted = DeletedState.Deleted
            else:
                try:
                    doc = XDocument.Parse(rec['body_xml'].Value)
                except:
                    doc = None
                if doc is not None:
                    elem = doc.Element('location')                
                    if elem != None:
                        if elem.Value != None and elem.Value != '':
                            im.Body.Value = elem.Value
                            if elem.FirstNode is not None and elem.FirstNode.Attribute('href') is not None and elem.FirstNode.Attribute('href').Value is not None:
                                im.Body.Value += "\n" + elem.FirstNode.Attribute('href').Value
                            if extractSource:
                                im.Body.Source = MemoryRange(rec['body_xml'].Source)
                        if elem.Attribute('latitude') != None and elem.Attribute('latitude').Value.isnumeric() and elem.Attribute('longitude') != None and elem.Attribute('longitude').Value.isnumeric():
                            longitude = float(elem.Attribute('longitude').Value)/1000000
                            latitude = float(elem.Attribute('latitude').Value)/1000000

                            c = Coordinate()
                            c.Longitude.Init(longitude, MemoryRange(rec['body_xml'].Source) if extractSource else None)
                            c.Latitude.Init(latitude, MemoryRange(rec['body_xml'].Source) if extractSource else None)
                            im.Position.Value = c

                            l = Location()
                            l.Deleted = rec.Deleted
                            l.CarveSource = rec.CarveSource
                            l.Category.Value = im.SourceApplication.Value
                            l.Position.Value = c
                            l.TimeStamp.Init(im.TimeStamp)

                            if fs is not None:
                                if rec['author'].Value != fs.Parent.Name:
                                    l.Origin.Value = LocationOrigin.External

                            if l.TimeStamp.Value is None and elem.Attribute('timeStamp') != None and elem.Attribute('timeStamp').Value.isnumeric():
                                l.TimeStamp.Init(TimeStamp.FromUnixTime(int(elem.Attribute('timeStamp').Value)), MemoryRange(rec['body_xml'].Source) if extractSource else None)

                            if elem.Attribute('address') != None and elem.Attribute('address').Value != "":
                                l.Description.Init(elem.Attribute('address').Value, MemoryRange(rec['body_xml'].Source) if extractSource else None)

                            results.append(l)
                            LinkModels(im, l)

        # Failed Image Message / Video Message (of type 254)
        elif rec['type'].Value in [254] and not IsDBNull(rec['body_xml'].Value):            
            imgContentType = None             
            try:
                doc = XDocument.Parse(rec['body_xml'].Value)
                elems = doc.Descendants('URIObject')                                                        
                for elem in elems:
                    #print elem
                    if elem.Value is not None and elem.Value != '':
                        im.Body.Value = elem.Value
                        if extractSource:
                            im.Body.Source = MemoryRange(rec['body_xml'].Source)                
            except:                
                pass


        # Contact sharing        
        elif rec['type'].Value == 63 and not IsDBNull(rec['body_xml'].Value):            
            imgContentType = None            
            try:
                doc = XDocument.Parse(rec['body_xml'].Value)  
                contacts = doc.Element('contacts')
                c = contacts.Element('c')                
                user_name = c.Attribute('s').Value
                real_name = c.Attribute('f').Value
                body =  "Shared contact : {0} ({1})".format(real_name, user_name)                
                im.Body.Value = body
                if extractSource:
                    im.Body.Source = MemoryRange(rec['body_xml'].Source)                
            except:                
                if rec['body_xml'].Value == "":
                    ## as seen in a BlackBerry10 Backup - see ticket #1157680
                    im.Deleted = DeletedState.Deleted

        if not IsDBNull(rec['author'].Value):
            im.From.Value = Party.MakeFrom(rec['author'].Value, None)
            if extractSource:
                im.From.Value.Identifier.Source = MemoryRange(rec['author'].Source)
        if not IsDBNull(rec['from_dispname'].Value):
            if im.From.Value is None:
                im.From.Value = Party()
                im.From.Value.Role.Value = PartyRole.From
            im.From.Value.Name.Value = rec['from_dispname'].Value
            if extractSource:
                im.From.Value.Name.Source = MemoryRange(rec['from_dispname'].Source)
        elif im.From.Value is not None and rec['author'].Value in contacts and not IsDBNull(contacts[rec['author'].Value].Value):
            im.From.Value.Name.Value = contacts[rec['author'].Value].Value
            if extractSource:
                im.From.Value.Name.Source = MemoryRange(contacts[rec['author'].Value].Source)
        if im.From.Value != None and im.From.Value.Identifier.Value not in [p.Identifier.Value for p in chats[conv_id].Participants.Items]: #for p in chats[name].Participants.Items]:
            party = Party.MakeGeneral(im.From.Value.Identifier.Value, None)
            party.Name.Value = im.From.Value.Name.Value
            if extractSource:
                party.Identifier.Source = im.From.Value.Identifier.Source
                party.Name.Source = im.From.Value.Name.Source
            #chats[name].Participants.Add(party)
            chats[conv_id].Participants.Add(party)

        #if im not in messages[name]:
        if im not in messages[conv_id]:
            #messages[name].add(im)
            messages[conv_id].add(im)
            #chats[name].Messages.Add(im)
            chats[conv_id].Messages.Add(im)      
    for id in chats:
        c = chats[id]
        c.SetTimesByMessages()
        if len(c.Messages) > 0:
            results.append(c)
    pr = ParserResults()
    pr.Models.AddRange(results)
    return pr

def analyze_skype_chatsync(node, extractDeleted, extractSource):
    syncresults = {}
    pr = ParserResults()
    for file in node.Search("/chatsync/.*/.*\\.dat$"):        
        syncresults = {}
        if file.Type != NodeType.File or file.Data == None or file.Data.Length == 0:
            continue
        sUser = file.Parent.Parent.Parent.Name
        if (not extractDeleted) and file.Deleted:
            continue
        if not sUser in syncresults:
            syncresults[sUser] = []
        parser = SkypeChatsyncFileParser(file.Data, file.Deleted)
        try:
            syncresults[sUser].append(parser.parse_file())
            sync_analizer = SkypeChatsyncAnalyzer(syncresults, extractDeleted, extractSource)
            chat = sync_analizer.ParseMessageBlock()    
            if chat != None:
                pr.Models.Add(chat)
            lastIpInfo = sync_analizer.get_last_ip_info()                
            pr.DeviceInfoFields.AddRange(lastIpInfo)

        except BaseException, e:
            if file.Deleted == DeletedState.Intact:
                ServiceLog.Error (e.clsException)    
    return pr
    
def enum(**enums):
    return type('Enum', (), enums) 

SkypeFieldType = enum(INT = 0, CONST_LEN = 0x1, STRING = 0x3, BLOB = 0x4, END_OF_RECORD = 0x5, ARRAY = 0x6)
BlockType = enum(CHAT_NAME = 0x1, CONNECTION_DATA = 0x2, PARTICIPANT_INDECES = 0x3, OTHER_DATA = 0x4, INDECES = 0x5, MESSAGES = 0x6)

Conversation = namedtuple('Conversation', ['data_blocks', 'timestamp'])
Field = namedtuple('Field', 'field_type, code, value, chunks') 
BlockHeader = namedtuple('BlockHeader', 'length, block_id, block_type, padding')
Block = namedtuple('Block', 'header, data')
       
class Record:
    def __init__(self, fields):
        self.fields = fields
        
    def __str__(self):
        tokens = []
        for field in self.fields:
            tokens.append(str(field))
        return '; '.join(tokens)

class MessageBlock:
    def __init__(self, hendle1, hendle2, timestamp, flags, data_size):
        self.hendle1 = hendle1
        self.hendle2 = hendle2
        self.flags = flags
        self.data_size = data_size
        self.timestamp = timestamp
        self.records = []
 
def buf_2_str(buf):
    if buf == None:
        return ''
    return''.join('%02X' % ord(b) for b in buf)            

class SkypeChatsyncFileParser:
    file_signature = SKYPE_SYNC_FILE_MAGIC
    file_header_size = 0x20
    block_header_size = 0x10
    endianity_str_2_endianity = {'<': Endianity.LittleEndian, '>': Endianity.BigEndian}
    record_start_magic = 'A'

    def __init__(self, istream, is_deleted):
        self.endianity = None
        self.fi = istream
        self.is_deleted = is_deleted
    
    def pars_file_header(self):
        buf = self.fi.read(self.file_header_size)
        if(len(buf) < self.file_header_size):
            return None, None
        self.get_endianity(buf)
        signature, timestamp, length, padding = struct.unpack(self.endianity + '5sII19s', buf)
        if(signature != self.file_signature):
            raise Exception('Unknown format: {0}'.format(repr(signature)))
        return length, Field(SkypeFieldType.INT, None, timestamp, [Chunk(self.fi, 5, 4)])
    
    def get_endianity(self, buf):
        endianity = '<'
        size = struct.unpack(endianity + 'I', buf[9:0xd])[0]
        if size + self.file_header_size == self.fi.Length:
            self.endianity = endianity
            return
        endianity = '>'
        size = struct.unpack(endianity + 'I', buf[9:0xd])[0]
        if size + self.file_header_size == self.fi.Length:
            self.endianity = endianity
            return
        raise Exception('Unsupported file format')

    def parse_file(self):
        self.fi.seek(0)
        length, timestamp = self.pars_file_header() 
        if length == None:
            return None           
        blocks = []
        position = 0
        while position < length:
            size, block = self.parse_block()
            if block.header.block_type == BlockType.CONNECTION_DATA or block.header.block_type == BlockType.CHAT_NAME or block.header.block_type == BlockType.MESSAGES:
                blocks.append(block)
            position += size
        return Conversation(data_blocks = blocks, timestamp = timestamp)

    def parse_block(self):
        header_size, header = self.parse_block_header()
        parse_block_data = self.get_block_parser(header)
        data = parse_block_data(header)
        return header_size + header.length, Block(header, data)
    
    def parse_block_header(self):
        buf = self.fi.read(self.block_header_size)
        if(len(buf) < self.block_header_size):
            raise Exception('Out of range')
        length, block_id, block_type, padding = struct.unpack(self.endianity + "III4s", buf)
        return self.block_header_size, BlockHeader(length, block_id, block_type, padding) 
        
    def get_block_parser(self, header):
        if header.block_type == BlockType.CONNECTION_DATA or header.block_type == BlockType.CHAT_NAME:
            parser = self.parse_generic_block
        elif header.block_type == BlockType.MESSAGES:
            parser = self.parse_block_messages
        else:
            parser = self.parse_default
        return parser

    def parse_default(self, header):
        try:
            return self.fi.read(header.length)
        except:
            pass

    def parse_block_messages(self, header):
        position = 0
        records = []  
        while position < header.length:
            try:          
                size, record = self.read_record()                
                pos = 0                      
                while pos < size - 0x14:              
                    sz,rec = self.parse_record(size - 0x14 - pos)                                  
                    pos = pos + sz                         
                if rec:
                    record.records.append(rec)
                    #records.append(rec)
            except BaseException, e:                
                # we don't need data from block 6
                size = self.look_for_next_record(header.length - self.fi.tell())
                if size == 0:
                    break
            position += size  
            records.append(record)                     
        return records 
    
    def read_record(self):
        buf = self.fi.read(0x14)
        if len(buf) < 0x14:
            return None
        message_data = struct.unpack(self.endianity + 'IIIII', buf)
        block = MessageBlock(*message_data)
        if block.data_size == 0:
            block.data_size = 2        
        #block.records = self.fi.read(block.data_size)
        return block.data_size + 0x14, block

    def parse_block_indeces(self, header):        
        records = []
        position = 0
        for i in range(header.length/0x10):
            buf = self.fi.read(0x10)
            if len(buf) < 0x10:
                break
            values = struct.unpack(self.endianity + '4I', buf)
            records.append(values)            
        return records

    def parse_generic_block(self, header):
        position = 0
        length = header.length
        records = []
        while position < length:      
            try:
                size, record = self.parse_record(length - position)
                if record:
                    records.append(record)
            except BaseException, e:
                if self.is_deleted == DeletedState.Intact:
                    #we only care about data in 2 blocks that we use
                    if header.block_type == BlockType.CONNECTION_DATA or header.block_type == BlockType.CHAT_NAME:
                        raise
                if header.length > self.fi.tell():
                    size = self.look_for_next_record(header.length - self.fi.tell())
                else:
                    position = self.fi.tell()
                    break
            position += size             
        return records
    
    def parse_field(self, max_length):
        field_type = ord(self.fi.read(1))
        max_length -= 1
        if field_type == SkypeFieldType.INT:
            code, csize = self.read_VLQ(max_length)
            value, vsize, chunk =  self.read_VLQ_with_soutce(max_length - csize)
        elif field_type == SkypeFieldType.STRING:
            code, csize = self.read_VLQ(max_length)
            value, vsize, chunk = self.read_cstring(max_length-csize)
        elif field_type == SkypeFieldType.BLOB:
            code, csize = self.read_VLQ(max_length)
            value, vsize, chunk = self.read_blob(max_length - csize)
        elif field_type == SkypeFieldType.CONST_LEN:
            code, csize = self.read_VLQ(max_length)
            value, vsize, chunk = self.read_constant_length(max_length)
        elif field_type == SkypeFieldType.END_OF_RECORD:
            code, csize = self.read_VLQ(max_length)
            value, vsize, chunk = 0, 0, None
        elif field_type == SkypeFieldType.ARRAY:
            code, csize = self.read_VLQ(max_length)
            if code == 0:
                value, vsize, chunk = [], 0,  None
            else:
                arrsize, arrsizesize = self.read_VLQ(max_length)
                vsize = arrsizesize
                value = []
                chunks = []
                for i in range(arrsize):
                    v, _vsize, chunk = self.read_VLQ_with_soutce(max_length - csize - vsize)
                    vsize += _vsize
                    value.append(v)
                    chunks.append(chunk)
                return csize + vsize + 1, Field(field_type, code, value, chunks)
        else:
            raise Exception("Field of unexpected type {0:X} detected at {1:X}.".format(field_type, self.fi.tell()))
        return csize + vsize + 1, Field(field_type, code, value, [chunk])
    
    def read_VLQ(self, max_length):
        value, vsize = VariableLengthQuantityConverter.ConvertStream(self.fi, max_length, self.endianity_str_2_endianity[self.endianity])
        return int(str(value)), int(str(vsize))

    def read_VLQ_with_soutce(self, max_length):
        offset = self.fi.tell()
        value, vsize = self.read_VLQ(max_length)
        return value, vsize, Chunk(self.fi, offset, vsize)
    
    def read_constant_length(self, max_length):
        offset = self.fi.tell()
        return self.fi.read(8), 8, Chunk(self.fi, offset, 8)

    def read_cstring(self, max_length):
        offset = self.fi.tell()
        c = self.fi.read(1)
        result = c
        length = 1
        while c != '\x00' and c != '' and max_length > length:
            c = self.fi.read(1)
            result += c
            length += 1
        return result, length, Chunk(self.fi, offset, length)
    
    def read_blob(self, max_length):
        length, lengthsize = self.read_VLQ(max_length)
        offset = self.fi.tell()
        data = self.fi.read(length)
        return  data, lengthsize + length, Chunk(self.fi, offset, length)

    def parse_record(self, max_length):
        start = self.fi.tell()         
        signature = self.fi.read(1)
        if (signature != SkypeChatsyncFileParser.record_start_magic):
            raise Exception("Position: {0:X}. Record expected to start with {1} but was {2}.".format(self.fi.tell(), SkypeChatsyncFileParser.record_start_magic, repr(signature)))
        n, nsize = self.read_VLQ(max_length - 1)
        if n == 0:
            return nsize + 1, Record([])
        else:
            fields = []
            length = max_length
            position = nsize + 1
            index = 0
            while position < length:
                cur_pos = self.fi.tell()
                size, field = self.parse_field(length - position)
                position += size
                index += 1
                fields.append(field)                
                if(field.field_type == SkypeFieldType.END_OF_RECORD):                  
                    break
                if index == n+1:                     
                    size = self.look_for_next_record(length - position)
                    position += size
                    break

            return self.fi.tell() - start, Record(fields)
        
    def look_for_next_record(self, max_length):
        index = 0
        while index < max_length:
            c = self.fi.read(1)
            index += 1
            if c == '':
                break
            if c == SkypeChatsyncFileParser.record_start_magic:
                n, nsize = self.read_VLQ(max_length - index)
                if index + nsize < max_length:
                    c2 = self.fi.read(1)
                    if c2 == '':
                        self.fi.seek(-nsize-1, os.SEEK_CUR)
                        break
                    field_type = ord(c2)
                    if field_type != SkypeFieldType.INT and field_type != SkypeFieldType.STRING and field_type != SkypeFieldType.BLOB and field_type != SkypeFieldType.CONST_LEN and field_type != SkypeFieldType.ARRAY:
                        self.fi.seek(-nsize-1, os.SEEK_CUR)
                        continue
                self.fi.seek(-2 - nsize, os.SEEK_CUR)
                break
        return index

class SkypeChatsyncAnalyzer:
    local_ip_position = 0x9
    network_ip_position = 0x15
    ip_length = 4
    port_len = 2
    total_len = ip_length + port_len

    def __init__(self, chats, extractDeleted, extractSource):
        self.chats = chats
        self.extractDeleted = extractDeleted
        self.extractSource = extractSource

    def ParseMessageBlock(self):
        participants = self.get_chat_participants()             

        c = Chat()
        c.Source.Value = 'Skype:ChatSync'
        c.Deleted = DeletedState.Unknown

        for user,chats in self.chats.items():
            for chat in chats:                
                for data_block in chat.data_blocks:
                    if data_block.header.block_type == BlockType.MESSAGES:
                        for msgBlock in data_block.data:
                            for rec in msgBlock.records:                         
                                userId = None
                                userIdValue = None      
                                msg = None                                
                                for field in rec.fields:                                
                                    if field.code == 2:
                                        userId = field.value                                    
                                    if field.code == 3:
                                        userIdValue = field.value
                                    if field.code == 4:
                                        msg_text = None
                                        try:
                                            msg_start = field.value.index('\x03\x02')                                    
                                            msg_end = field.value.index('\x00', msg_start+1)
                                            msg_text = field.value[msg_start+2:msg_end]
                                            msg_text = unicode(msg_text, 'utf-8')                                               
                                        except:
                                            try:                                    
                                                msg_start = field.value.index('\x03"')                                    
                                                msg_end = field.value.index('\x00', msg_start+1)
                                                msg_text = field.value[msg_start+2:msg_end]
                                                msg_text = unicode(msg_text, 'utf-8')                                               
                                            except:
                                                pass                               
                                        msg = msg_text
                                if userId != None and msg != None:
                                    im = InstantMessage()
                                    im.Deleted = DeletedState.Unknown
                                    im.Body.Value = msg
                                    im.SourceApplication.Value = 'Skype:ChatSync'
                                    im.TimeStamp.Value = TimeStamp.FromUnixTime(msgBlock.timestamp)
                                    if len(participants) >= userId - 1:                                    
                                        im.From.Value = Party(participants[userId - 1])
                                    c.Messages.Add(im)                                                                
        
        if c.Messages.Count > 0:
            c.SetParticipantsByMessages()
            c.SetTimesByMessages()
            l = self.find_ips(chat,user)            
            for name,ip_data in l:
                for p in c.Participants:                    
                    if p.Identifier.Value == name.value[:-1]:     
                        for net in ip_data:                                         
                            tmp = net.ToString().replace('DeviceInfoLocalNetworkIP (Network Interfaces)','Local')
                            tmp = tmp.replace('DeviceInfoInternetNetworkIP (Network Interfaces)','Public')                            
                            p.IPAddresses.Add(tmp)                          
            return c
        return None

    def get_chat_participants(self):        
        for user,chats in self.chats.items():
            for chat in chats:
                for block in chat.data_blocks:
                    if block.header.block_type == BlockType.CHAT_NAME:                          
                        participants = block.data[0].fields[0].value.split(';')[0]
                        participant1, participant2 = [name[1:] for name in participants.split('/')]                        
                        return participant1,participant2   

    def get_last_ip_info(self):        
        chat, user = self.find_last_chat()                
        if chat:
            l = self.find_ips(chat,user)
            for name,ip_data in l:               
                if name and name.value[:-1] == user:
                    return ip_data            
        return []

    def find_last_chat(self):
        lats_time = 0
        last_chat = None
        last_user = None
        for user, chats in self.chats.items():
            for chat in chats:
                if chat and chat.timestamp.value > lats_time:
                    lats_time = chat.timestamp.value
                    last_chat = chat
                    last_user = user
        return last_chat, last_user

    def get_participants_data(self, records):
        result = []
        for record in records:
            name = next((field for field in record.fields if field.field_type == SkypeFieldType.STRING and field.code == 0 and field.value != 0), None)
            ip_data_buf = next((field for field in record.fields if field.field_type == SkypeFieldType.BLOB and field.code == 1), None)            
            if name and ip_data_buf:
                result.append((name, ip_data_buf))                    
        return result 
    
    def convert_ip(self, ip_buf):
        tokens = []
        for i in ip_buf[:SkypeChatsyncAnalyzer.ip_length]:            
            tokens.append(str(ord(i)))        
        return '{ip}:{port}'.format(ip = '.'.join(tokens), port = struct.unpack('>H', ip_buf[SkypeChatsyncAnalyzer.ip_length:SkypeChatsyncAnalyzer.total_len])[0])
    
    def get_connection_data(self, ip_data, time, is_local): 
        group_name = DeviceInfoGroups.NetworkInterfaces
        if is_local:
            ip_name = DeviceInfoNames.LocalIPAddress
            offset = SkypeChatsyncAnalyzer.local_ip_position
        else:            
            ip_name = DeviceInfoNames.InternetIPAddress
            offset = SkypeChatsyncAnalyzer.network_ip_position
        ip = self.convert_ip(ip_data.value[offset:offset + SkypeChatsyncAnalyzer.total_len])                
        time_repr = datetime.datetime.fromtimestamp(time.value)        
        value = '{0} at {1}'.format(ip, time_repr)
        source = None
        if self.extractSource:
            source_chunks = []
            source_chunks.append(Chunk(ip_data.chunks[0].BaseStream, ip_data.chunks[0].Offset + offset, SkypeChatsyncAnalyzer.total_len))
            source_chunks.extend(time.chunks)
            source = MemoryRange(source_chunks)        
        return MetaDataField(ip_name, value, source, group_name)

    def get_connection(self, ip_data, time):
        results = []        
        if len(ip_data.value) > SkypeChatsyncAnalyzer.local_ip_position + SkypeChatsyncAnalyzer.total_len:            
            results.append(self.get_connection_data(ip_data, time, is_local = True))
            if len(ip_data.value) > SkypeChatsyncAnalyzer.network_ip_position + SkypeChatsyncAnalyzer.total_len:
                results.append(self.get_connection_data(ip_data, time, is_local = False))
        return results
    
    def find_ips(self, chat, user):
        ips = []
        for block in chat.data_blocks:
            if block and block.header.block_type == 0x2:                
                records = self.get_participants_data(block.data)                     
                for name, ip_data in records:                            
                    ips.append((name,self.get_connection(ip_data, chat.timestamp)))           
        return ips