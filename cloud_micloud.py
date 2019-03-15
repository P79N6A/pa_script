# coding=utf-8
import hashlib

__author__ = 'TaoJianping'

import clr

try:
    clr.AddReference('model_notes')
    clr.AddReference('model_contact')
    clr.AddReference('model_callrecord')
    clr.AddReference('model_sms')
    clr.AddReference('model_media')
    clr.AddReference('ScriptUtils')
except:
    pass

clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')

del clr
import PA_runtime
import model_sms
import model_callrecord
import model_notes
import model_contact
import model_media
import json
import re
import System
from PA_runtime import *
from System.Data.SQLite import *
from System.Xml.Linq import *
from System.Xml.XPath import Extensions as XPathExtensions
from ScriptUtils import TimeHelper

# const
DEBUG = False
MICOULDVERSION = 1


def print_error():
    if DEBUG:
        TraceService.Trace(TraceLevel.Error, "android_wechat.py Error: LINE {}".format(traceback.format_exc()))


class YunkanMiCloudParser(object):
    """
    云勘数据 备份解析
        1. 联系人
        2. 短信
        3. 照片
        4. 视频
        5. notes
    """

    def __init__(self, node, extract_deleted, extract_source):
        self.root = node
        self.app_name = 'miCloud'
        self.app_version = MICOULDVERSION
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.cache_db = self._get_cache_db()
        self.owner_phone = self._get_owner_phone(node)
        self.nm = model_notes.MN()
        self.cm = model_contact.MC()
        self.sms = model_sms.ModelSMS()
        self.crm = model_callrecord.MC()
        self.mm = model_media.MM()
        self.debug = DEBUG

    def _get_cache_db(self):
        """获取中间数据库的db路径"""
        self.cache_path = ds.OpenCachePath(self.app_name)
        m = hashlib.md5()
        m.update(self.root.AbsolutePath.encode('utf-8'))
        return os.path.join(self.cache_path, m.hexdigest().upper())

    @staticmethod
    def _get_owner_phone(node):
        return 1550805191684

    @staticmethod
    def _open_json(node):
        if not node:
            return
        try:
            path = node.PathWithMountPoint
            with open(path, 'r') as f:
                data = json.load(f)
                return data
        except Exception as e:
            print_error()
            return {}

    def _open_file(self, node):

        data = self._open_json(node)

        if not data:
            return {}

        post_success = data.get('result', None)
        if post_success != 'ok':
            return {}

        return data.get('data', {})

    def _parse_contacts(self, node):
        contacts_files = node.Search(r'Contact_\d+.txt')
        for file_ in contacts_files:

            data = self._open_json(file_)
            if not data:
                continue

            post_success = data.get('result', None)
            if post_success != 'ok':
                continue

            contacts = data.get('data', {}).get('content', None)
            if not contacts:
                continue

            for contact_id, contact_info in contacts.items():
                try:
                    contact = model_contact.Contact()
                    contact.raw_contact_id = contact_id
                    contact.source = file_.AbsolutePath
                    detail = contact_info.get('content', {})
                    contact.name = detail.get('displayName', None)
                    contact.phone_number = ",".join([p['value'] for p in detail.get('phoneNumbers', [])])
                    contact.company = ",".join([o.get('company', '') for o in detail.get('organizations', [])])
                    contact.mail = ",".join([p['value'] for p in detail.get('emails', [])])
                    contact.notes = detail.get('note', None)
                    self.cm.db_insert_table_call_contacts(contact)
                except Exception as e:
                    print(e)

        self.cm.db_commit()

    def _parse_sms(self, node):
        sms_files = node.Search(r'Sms_\d+.txt')
        for file_ in sms_files:
            data = self._open_file(file_)
            phone_calls = data.get('phonecall_view', {}).get('entries', [])
            sms = data.get('entries', [])

            for pc in phone_calls:
                try:
                    record = model_callrecord.Records()
                    record.source = file_.AbsolutePath
                    record.duration = pc.get('duration', None)
                    record.phone_number = pc.get('number', None)
                    record.id = pc.get('id', None)
                    record.date = TimeHelper.convert_timestamp(pc.get('date', None))
                    self.crm.db_insert_table_call_records(record)
                except Exception as e:
                    print(e)

            for s in sms:
                try:
                    s = s['entry']
                    message = model_sms.SMS()
                    message.source = file_.AbsolutePath
                    message.body = s.get('snippet', None)
                    message.send_time = TimeHelper.convert_timestamp(s.get('localTime', None))
                    message.read_status = 1 if s['unread'] == 0 else 0
                    message.is_sender = 1 if s['folder'] == 1 else 0
                    message._id = s.get('id', None)
                    recipients = s.get('recipients', None)
                    message.sender_phonenumber, message.recv_phonenumber = (self.owner_phone, recipients) \
                        if message.is_sender else (recipients, self.owner_phone)
                    self.sms.db_insert_table_sms(message)
                except Exception as e:
                    print(e)

        self.crm.db_commit()
        self.sms.db_commit()

    def _parse_photos(self, node):
        gallery_files = node.Search(r'Gallery_user_galleries_\d+.txt')
        for f in gallery_files:
            data = self._open_file(f)
            for g in data.get('galleries', []):
                try:
                    file_name = g.get('fileName', None)
                    if not file_name:
                        continue
                    pic = next(iter(self.root.Search(file_name)), None)
                    if not pic:
                        continue
                    m = model_media.Media()
                    m.source = f.AbsolutePath
                    m.url = pic.AbsolutePath
                    m.id = g.get('id', None)
                    m.title = g.get('title', None)
                    m.type = 'image'
                    m.modify_date = TimeHelper.convert_timestamp(g.get('dateModified', None))
                    m.add_date = TimeHelper.str_to_ts(g.get('exifinfo', {}).get('dateTime', None),
                                                      _format="%Y-%m-%d %H:%M:%S")
                    m.size = g.get('size', None)
                    m.height = g.get('exifinfo', {}).get('imageLength', None)
                    m.width = g.get('exifinfo', {}).get('imageWidth', None)
                    self.mm.db_insert_table_media(m)
                except Exception as e:
                    print(e)
        self.mm.db_commit()

    def _parse_videos(self, node):
        videos_files = node.Search(r'video_\d+')
        for f in videos_files:
            try:
                m = model_media.Media()
                file_name = os.path.basename(f.PathWithMountPoint)
                m.source = f.AbsolutePath
                m.url = f.AbsolutePath
                m.id = m.title = file_name
                m.type = 'video'
                m.size = os.path.getsize(f.PathWithMountPoint)
                self.mm.db_insert_table_media(m)
            except Exception as e:
                    print(e)
        self.mm.db_commit()

    def _parse_notes(self, node):
        note_files = node.Search(r'Note_\d+.txt')
        deleted_note_files = self.root.Search(r'NoteDeleted_\d+.txt')

        note_existing = 0
        note_deleted = 1

        files = [(f, note_existing) for f in note_files] + [(f, note_deleted) for f in deleted_note_files]

        for f, is_deleted in files:
            data = self._open_file(f)
            for n in data.get('entries', []):
                try:
                    note = model_notes.Notes()
                    note.html_content = n.get('snippet', None)
                    note.deleted = is_deleted
                    note.source = f.AbsolutePath
                    note.fold_id = n.get('folderId', None)
                    note.id = n['id']
                    note.modified = TimeHelper.convert_timestamp(n.get('modifyDate', None))
                    note.created = TimeHelper.convert_timestamp(n.get('createDate', None))
                    self.nm.db_insert_table_notes(note)
                except Exception as e:
                    print(e)
        self.nm.db_commit()

    def _need_parse(self, cache_db, app_version):
        return any((
            self.mm.need_parse(cache_db, app_version),
            self.sms.need_parse(cache_db, app_version),
            self.crm.need_parse(cache_db, app_version),
            self.cm.need_parse(cache_db, app_version),
        ))

    def _db_create(self):
        self.cm.db_create(self.cache_db + '.cm')
        self.crm.db_create(self.cache_db + '.crm')
        self.sms.db_create(self.cache_db + '.sms')
        self.mm.db_create(self.cache_db + '.mm')
        self.nm.db_create(self.cache_db + '.nm')

    def _update_db_version(self, app_version):
        self.sms.db_insert_table_version(model_sms.VERSION_KEY_DB, model_sms.VERSION_VALUE_DB)
        self.sms.db_insert_table_version(model_sms.VERSION_KEY_APP, app_version)
        self.sms.db_commit()

        self.crm.db_insert_table_version(model_callrecord.VERSION_KEY_DB, model_callrecord.VERSION_VALUE_DB)
        self.crm.db_insert_table_version(model_callrecord.VERSION_KEY_APP, app_version)
        self.crm.db_commit()

        self.mm.db_insert_table_version(model_media.VERSION_KEY_DB, model_media.VERSION_VALUE_DB)
        self.mm.db_insert_table_version(model_media.VERSION_KEY_APP, app_version)
        self.mm.db_commit()

        self.cm.db_insert_table_version(model_contact.VERSION_KEY_DB, model_contact.VERSION_VALUE_DB)
        self.cm.db_insert_table_version(model_contact.VERSION_KEY_APP, app_version)
        self.cm.db_commit()

    def _db_close(self):
        self.cm.db_close()
        self.nm.db_close()
        self.cm.db_close()
        self.crm.db_close()
        self.sms.db_close()

    def generate_models(self):

        pr1 = ParserResults()
        prog1 = progress.GetBackgroundProgress("云勘小米备份-联系人", DescripCategories.Contacts)
        prog1.Start()
        try:
            models = model_contact.Generate(self.cache_db + '.cm').get_models()
            pr1.Models.AddRange(models)
            pr1.Categories = DescripCategories.Contacts
            pr1.Build('云勘小米备份')
            ds.Add(pr1)
        except Exception as e:
            print(e)
        prog1.Finish(True)

        pr2 = ParserResults()
        prog2 = progress.GetBackgroundProgress("云勘小米备份-多媒体", DescripCategories.Photos)
        prog2.Start()
        try:
            models = model_media.Generate(self.cache_db + '.mm', model_media.COORDINATE_TYPE_GOOGLE).get_models()
            pr2.Models.AddRange(models)
            pr2.Categories = DescripCategories.Photos
            pr2.Build('云勘小米备份')
            ds.Add(pr2)
        except Exception as e:
            print(e)
        prog2.Finish(True)

        pr3 = ParserResults()
        prog3 = progress.GetBackgroundProgress("云勘小米备份-短信", DescripCategories.Messages)
        prog3.Start()
        try:
            models = model_sms.GenerateSMSModel(self.cache_db + '.sms').get_models()
            pr3.Models.AddRange(models)
            pr3.Categories = DescripCategories.Messages
            pr3.Build('云勘小米备份')
            ds.Add(pr3)
        except Exception as e:
            print(e)
        prog3.Finish(True)

        pr4 = ParserResults()
        prog4 = progress.GetBackgroundProgress("云勘小米备份-通话记录", DescripCategories.Calls)
        prog4.Start()
        try:
            models = model_callrecord.Generate(self.cache_db + '.crm').get_models()
            pr4.Models.AddRange(models)
            pr4.Categories = DescripCategories.Calls
            pr4.Build('云勘小米备份')
            ds.Add(pr4)
        except:
            pass
        prog4.Finish(True)

        pr5 = ParserResults()
        prog5 = progress.GetBackgroundProgress("云勘小米备份-备忘录", DescripCategories.Notes)
        prog5.Start()
        try:
            models = model_notes.Generate(self.cache_db + '.nm').get_models()
            pr5.Models.AddRange(models)
            pr5.Categories = DescripCategories.Notes
            pr5.Build('云勘小米备份')
            ds.Add(pr5)
        except:
            pass
        prog5.Finish(True)

    def _main(self):
        for node in self.root.Children:
            self._parse_contacts(node)
            self._parse_notes(node)
            self._parse_sms(node)
            self._parse_photos(node)
            self._parse_videos(node)

    def parse(self):
        if self.debug or self._need_parse(self.cache_db, MICOULDVERSION):
            self._db_create()
            self._main()
            self._update_db_version(MICOULDVERSION)
            self._db_close()

        return self.generate_models()


def parse_yunkan_micloud(root, extract_deleted, extract_source):
    YunkanMiCloudParser(root, extract_deleted, extract_source).parse()
    #pr = ParserResults()
    #pr.Categories = DescripCategories.Railway12306
    #results = YunkanMiCloudParser(root, extract_deleted, extract_source).parse()
    #if results:
        #pr.Models.AddRange(results)
        #pr.Build("miCloud")
    #return pr
