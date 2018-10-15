#coding=utf-8
import os
import clr
import PA_runtime
from PA_runtime import *
import hashlib
import clr
try:
    clr.AddReference('System.Data.SQLite')
    clr.AddReference('bcp_basic')
except:
    pass
del clr
import System.Data.SQLite as SQLite
import bcp_basic

SQL_CREATE_TABLE_CONTACT = '''
    CREATE TABLE IF NOT EXISTS contacts(
    row_contact_id TEXT,
    mimetype_id INTEGER,
    mail TEXT,
    company TEXT,
    title TEXT,
    last_time_contacted INTEGER,
    last_time_modified INTEGER,
    times_contacted INTEGER,
    phone_number TEXT,
    name TEXT,
    address TEXT,
    notes TEXT,
    telegram TEXT,
    head_pic TEXT,
    source TEXT,
    deleted INTEGER,
    repeated INTEGER
    )'''

SQL_INSERT_TABLE_CONTACT = '''
    INSERT INTO contacts(row_contact_id, mimetype_id, mail, company, title, last_time_contacted, last_time_modified, times_contacted,
        phone_number, name, address, notes, telegram, head_pic, source, deleted, repeated)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

ToPhoneCategory = {
    '_$!<MOBILE>!$_': ContactCategory.Mobile,
    '_$!<HOME>!$_': ContactCategory.Home,
    '_$!<WORK>!$_': ContactCategory.Work,
    '_$!<WORKFAX>!$_': ContactCategory.Fax,
    '_$!<HOMEFAX>!$_': ContactCategory.Fax,
    '_$!<OTHERFAX>!$_': ContactCategory.Fax,
    '_$!<PAGER>!$_': ContactCategory.Pager,
    '_$!<OTHER>!$_': ContactCategory.Other,
    '_$!<HOMEPAGE>!$_': 'Homepage',
    '_$!<MAIN>!$_': ContactCategory.General,
    }

AddressParts = {
    'STREET': 'Street1',
    'STATE': 'State',
    'ZIP': 'PostalCode',
    'CITY': 'City',
    'COUNTRY': 'Country',
    }

PROPERTIES = {
    3: PhoneNumber,
    4: EmailAddress,
    22: WebAddress
    }

epoch = DateTime(2001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc)

def to_category(value, categories):
    if 0 < value < len(categories):
        return categories[value - 1]
    
    return ContactCategory.General

def read_entries(records, recordId, phoneTypes, address_dict, extractDeleted, extractSource):
    entries = []    
    addresses = []    
    if recordId == 0:
        return [], []

    for record in records:
        if record['record_id'].Value == recordId:
            entry = None
            label = record['label']

            property = record['property'].Value
            if property == 5: # address
                entry = address_dict.get(record['UID'].Value)
                if entry != None:
                    if not IsDBNull(label.Value):
                        entry.Category.Value = to_category(label.Value, phoneTypes)
                        if extractSource:
                            entry.Category.Source = MemoryRange(label.Source)
                    addresses.append(entry)

            else:
                value = record['value'].Value.ToString()
                if extractSource:
                    source = MemoryRange(record['value'].Source)
                else:
                    source = None

                if property in PROPERTIES:
                    if extractSource:
                        entry = PROPERTIES[property](value, source)
                    else:
                        entry = PROPERTIES[property](value)

                if entry != None:
                    if not IsDBNull(label.Value):
                        entry.Category.Value = to_category(label.Value, phoneTypes)
                        if extractSource:
                            entry.Category.Source = MemoryRange(label.Source)

                    entry.Deleted = record.Deleted
                    if entry not in entries:
                        entries.append(entry)                            

    return entries, addresses

def build_addresses(db, extractDeleted, extractSource):
    # 读取ABMultiValueEntryKey表
    multiValueEntryKeys = list(db['ABMultiValueEntryKey'])
    multiValueEntryKeys.sort()
    
    addressTypes = [record['value'].Value for record in multiValueEntryKeys]

    addresses = defaultdict(StreetAddress)

    # 读取ABMultiValueEntry表
    for record in db['ABMultiValueEntry']:
        key = record['key'].Value
        if key > 0 and key <= len(addressTypes):
            part = AddressParts.get(addressTypes[key - 1].upper())

            if part != None:
                parentId = record['parent_id'].Value
                address = addresses[parentId]
                address.Deleted = DeletedState.Intact
                field = getattr(address, part)

                value = record['value']
                field.Value = value.Value
                if extractSource:
                    field.Source = MemoryRange(value.Source)

    return addresses

def build_contacts(db, images, image_file, extractDeleted, extractSource):
    addresses = build_addresses(db, extractDeleted, extractSource)

    # 获取电话标签(手机,iPhone,住宅,etc)
    labelRecords = list(db['ABMultiValueLabel'])
    labelRecords.sort()
    
    phoneCategories = [ToPhoneCategory.get(record['value'].Value.upper(), record['value'].Value) for record in labelRecords if not record['value'].IsDBNull]

    multiValue = SQLiteParser.TableSignature('ABMultiValue')
    if extractDeleted:
        multiValue['record_id'] = SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Int, SQLiteParser.FieldConstraints.NotNull)
        multiValue['property'] = multiValue['identifier'] = multiValue['label'] = SQLiteParser.Signatures.NumericSet(1)

    valueRows = list(db.ReadTableRecords(multiValue, extractDeleted))

    contacts = defaultdict(Contact)

    # 主表
    person = SQLiteParser.TableSignature('ABPerson')
    if extractDeleted:
        person['CreationDate'] = person['ModificationDate'] = SQLiteParser.Signatures.NumericSet(4)
        person['Kind'] = person['IsPreferredName'] = SQLiteParser.Signatures.NumericSet(1, 8, 9)    
    personsTable = db.ReadTableRecords(person, extractDeleted, True) 
    for record in personsTable:
        if record['ROWID'].Value in contacts:
            continue
        contact = contacts[record['ROWID'].Value]
        contact.Deleted = record.Deleted
        
        first = record['First']
        middle = record['Middle']
        last = record['Last']

        fullName = []
        if extractSource:
            nameChunks = []
        cnstyle = True
        datas = [last, middle, first]

        for field in datas:
            if IsDBNull(field.Value):
                continue
            if type(field.Value) != str:
                continue
            part = field.Value            
            fullName.append(part)
            if extractSource:
                nameChunks.extend(field.Source)
        name = ' '.join(fullName)   
        if name.find('\x00') != -1:
            continue
        
        contact.Name.Value = name
        if extractSource:
            contact.Name.Source = MemoryRange(nameChunks)
        
        entries, address_entries = read_entries(valueRows, record['ROWID'].Value, phoneCategories, addresses, extractDeleted, extractSource)        
            
        for entrie in entries:
            if entrie.Value.Value not in [e.Value.Value for e in contact.Entries]:
                contact.Entries.AddRange(entries)
        for address in address_entries:
            if address not in contact.Addresses:
                contact.Addresses.AddRange(address_entries)        

        if not IsDBNull(record['Organization'].Value):
            company = record['Organization']
            companyString = company.Value.ToString()
            if companyString:
                organization = Organization()
                organization.Deleted = record.Deleted
                organization.Name.Value = companyString
                if extractSource:
                    organization.Name.Source = MemoryRange(company.Source)
                if organization not in contact.Organizations:
                    contact.Organizations.Add(organization)

        if 'Note' in record and not IsDBNull(record['Note'].Value) and record['Note'].Value not in [note.Value for note in contact.Notes.Items]:
            if extractSource:
                contact.Notes.Add(record['Note'].Value, MemoryRange(record['Note'].Source))
            else:
                contact.Notes.Add(record['Note'].Value, None)

        if 'CreationDate' in record and not IsDBNull(record['CreationDate']):
            try:
                contact.TimeCreated.Value = TimeStamp(TimeStampFormats.GetTimeStampEpoch1Jan2001(record['CreationDate'].Value), True)
                if extractSource:
                    contact.TimeCreated.Source = MemoryRange(record['CreationDate'].Source)
            except:
                pass

        if 'ModificationDate' in record and not IsDBNull(record['ModificationDate']):
            try:
                contact.TimeModified.Value = TimeStamp(TimeStampFormats.GetTimeStampEpoch1Jan2001(record['ModificationDate'].Value), True)
                if extractSource:
                    contact.TimeModified.Source = MemoryRange(record['ModificationDate'].Source)
            except:
                pass
            
        if record['ROWID'].Value in images:
            pic = images.pop(record['ROWID'].Value)
            
            cp = ContactPhoto()
            node = Node(NodeType.File)
            cp.Deleted = pic.Deleted
            node.Data = MemoryRange(pic['data'].Source)
            cp.PhotoNode.Value = node

            contact.Photos.Add(cp)
            if image_file != None:
                f = Node("{0}".format(contact.Name.Value), NodeType.File | NodeType.Embedded)
                f.Deleted = record.Deleted
                f.Data = MemoryRange(pic['data'].Source)
                image_file.Children.Add(f)

    for record in images.values():
        f = Node("{0}".format(record['record_id'].Value), NodeType.File | NodeType.Embedded)
        f.Deleted = record.Deleted
        f.Data = MemoryRange(record['data'].Source)
        image_file.Children.Add(f)
    results = []
    for contact in contacts.values():
        if contact.Name.Value not in [None, ''] or contact.Entries or contact.Addresses:
            results.append(contact)
    return results

def read_images(f):
    images = {}

    db = SQLiteParser.Database.FromNode(f)
    if db != None:
        imageTable = []
        if 'ABFullSizeImage' in db.Tables:
            imageTable = db['ABFullSizeImage']
        elif 'ABImage' in db.Tables:
            imageTable = db['ABImage']

        for record in imageTable:
            if record['data'].Source is None:
                continue
            if record['data'].Source.Count > 0 and record['record_id'].Value not in images:
                images[record['record_id'].Value] = record                                        
        
        if 'ABThumbnailImage' in db.Tables:
            for rec in db['ABThumbnailImage']:
                if rec['data'].Source is None:
                    continue
                emb_f = Node("{0}".format(rec['record_id'].Value), NodeType.File | NodeType.Embedded)
                emb_f.Deleted = rec.Deleted
                emb_f.Data = MemoryRange(rec['data'].Source)
                f.Children.Add(emb_f)

    return images

def read_recent(db, extractDeleted, extractSource):
    results = {}
    if 'ABRecent' not in db.Tables:
        return results
    ts = SQLiteParser.TableSignature('ABRecent')
    if extractDeleted:
        ts['date'] = SQLiteParser.Signatures.NumericSet(4)
        ts['name'] = SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Text) # can be null, not declared in create expression
        ts['property'] = SQLiteParser.Signatures.NumericSet(1)
        ts['value'] = SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)

    for record in db.ReadTableRecords(ts, extractDeleted):
        c = Contact()
        c.Deleted = record.Deleted
        c.Source.Value = 'Recently Contacted'
        try:
            c.TimeContacted.Value = TimeStamp(TimeStampFormats.GetTimeStampEpoch1Jan2001(long(record['date'].Value)), True)
            if not c.TimeContacted.Value.IsValidForSmartphone():
                if c.Deleted == DeletedState.Intact:
                    print("wrong time: " + record['value'].Value + c.TimeContacted.Value.ToString())
                else:
                    continue
            if extractSource:
                c.TimeContacted.Source = MemoryRange(record['date'].Source)
        except:
            if record.Deleted == DeletedState.Intact:
                pass
            else:
                continue
        if IsDBNull(record['value'].Value) or (not '@' in record['value'].Value):
            continue
        if not IsDBNull(record['name'].Value):
            c.Name.Value = record['name'].Value
            if extractSource:
                c.Name.Source = MemoryRange (record['name'].Source)
            combined_name = record['name'].Value + record['value'].Value
        else:
            combined_name = record['value'].Value
        if extractSource:
            email = EmailAddress(record['value'].Value, MemoryRange(record['value'].Source))
        else:
            email = EmailAddress(record['value'].Value)
        email.Deleted = record.Deleted
        email.Category.Value = ContactCategory.General
        c.Entries.Add(email) 

        if combined_name not in results:
            results[combined_name] = c
        else:
            set_newer_record(results, combined_name, c)
    
    return results.values()

def set_newer_record(results, combined_name, contact):
    if contact.Deleted == DeletedState.Intact:
        second = results[combined_name]
        results[combined_name] = contact
        first = contact
    else:
        second = contact
        first = results[combined_name]
    if second.TimeContacted.Value != None:
        if first.TimeContacted.Value == None or (second.TimeContacted.Value != None and first.TimeContacted.Value.CompareTo(second.TimeContacted.Value)<0):
            first.TimeContacted.Value = second.TimeContacted.Value
            first.TimeContacted.Source = second.TimeContacted.Source

def analyze_addressbook(node, extractDeleted, extractSource):
    pr = ParserResults()
    message='解析通讯录完毕'

    pr = ParserResults()
    ab_file = node.GetByPath('AddressBook.sqlitedb')
    image_file = node.GetByPath('AddressBookImages.sqlitedb')
    
    if ab_file is None or ab_file.Data is None:
        return pr
    try:
        images = {}
        if image_file is not None and image_file.Data is not None: 
            try:
                images = read_images(image_file)
            except:
                pass

        db = SQLiteParser.Database.FromNode(ab_file)
        if db is None:
            raise Exception('解析通讯录出错:无法读取通讯录数据库')

        entries = list(build_contacts(db, images, image_file, extractDeleted, extractSource))
        recent = list(read_recent(db, extractDeleted, extractSource))  
        results = entries + recent
        generate_mid_db(node, results)
        pr.Models.AddRange(results)
    except Exception,ex:
        traceback.print_exc()
        TraceService.TraceException(ex)
    pr.Build('通讯录')
    return pr


def generate_mid_db(node, results):
    '''创建中间数据库'''
    #创建数据库
    cachepath = ds.OpenCachePath("Contacts")
    md5_db = hashlib.md5()
    db_name = 'contacts'
    md5_db.update(db_name.encode(encoding = 'utf-8'))
    db_path = cachepath + '\\' + md5_db.hexdigest().upper() + '.db'
    if os.path.exists(db_path):
        os.remove(db_path)
    db_cache = SQLite.SQLiteConnection('Data Source = {}'.format(db_path))
    db_cache.Open()
    db_cmd = SQLite.SQLiteCommand(db_cache)
    #创建表
    if db_cmd is not None:
        db_cmd.CommandText = SQL_CREATE_TABLE_CONTACT
        db_cmd.ExecuteNonQuery()
    db_cmd.Dispose()
    #提取插入数据
    i = 0
    for result in results:
        i = i + 1
        id = '0000000000000000000000000000000' + str(i)
        id = id[-32::1]
        name = result.Name.Value
        addr = ''
        for address in result.Addresses:
            addr = addr + ',' + address.FullName.Value
        addr = addr[1::]
        note = ''
        for n in result.Notes:
            note = note + ',' + str(n)
        note = note[1::]
        time_contacted = result.TimeContacted.Value
        time_modified = result.TimeModified.Value
        times_contacted = result.TimesContacted.Value
        phone_number = ''
        for number in result.Entries:
            phone_number = phone_number + ',' + number.Value.Value
        phone_number = phone_number[1::]
        source = node.AbsolutePath
        deleted = 0 if result.Deleted == DeletedState.Intact else 1 if result.Deleted == DeletedState.Deleted else None
        repeated = 0
        param = (id, None, None, None, None, time_contacted, time_modified, times_contacted, phone_number, name, addr, note, None, None, source, deleted, repeated)
        db_insert_table(db_cache, SQL_INSERT_TABLE_CONTACT, param)
    db_cmd.Dispose()
    db_cache.Close()

def db_insert_table(db, sql, values):
    db_cmd = SQLite.SQLiteCommand(db)
    if db_cmd is not None:
        db_cmd.CommandText = sql
        db_cmd.Parameters.Clear()
        for value in values:
            param = db_cmd.CreateParameter()
            param.Value = value
            db_cmd.Parameters.Add(param)
        db_cmd.ExecuteNonQuery()