# -*- coding: utf-8 -*-

from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions

import os
import System
import sqlite3

SQL_CREATE_TABLE_NOTES = '''
    CREATE TABLE IF NOT EXISTS notes(
        id INTEGER,
        title TEXT,
        content TEXT,
        created INTEGER,
        modified INTEGER,
        remind_id TEXT,
        fold_id INTEGER,
        fold_name TEXT,
        has_attachment INTEGER,
        attach_name TEXT,
        html_content TEXT,
        delete_flag INTEGER,
        has_todo INTEGER,
        attach_url TEXT,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_INSERT_TABLE_NOTES = '''
    INSERT INTO notes(id, title, content, created, modified, remind_id, fold_id, fold_name, has_attachment, attach_name, html_content, delete_flag, has_todo, attach_url, source, deleted, repeated)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

SQL_FIND_TABLE_NOTES = '''
    select * from notes
    '''



class MN(object):
    def __init__(self):
        self.db = None
        self.cursor = None

    def db_create(self,db_path):
        if os.path.exists(db_path):
            os.remove(db_path)
        self.db = sqlite3.connect(db_path)
        self.cursor = self.db.cursor()
        self.db_create_tables()

    def db_commit(self):
        if self.db is not None:
            self.db.commit()

    def db_close(self):
        if self.cursor is not None:
            self.cursor.close()
            self.cursor = None
        if self.db is not None:
            self.db.close()
            self.db = None

    def db_create_tables(self):
        if self.cursor is not None:
            self.cursor.execute(SQL_CREATE_TABLE_NOTES)

    def db_insert_table_notes(self, Notes):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_NOTES, Notes.get_values())


class Column(object):
    def __init__(self):
        self.source = ''
        self.deleted = 0
        self.repeated = 0

    def __setattr__(self, name, value):
        if not IsDBNull(value):
            self.__dict__[name] = value
        else:
            self.__dict__[name] = None

    def get_values(self):
        return (self.source, self.deleted, self.repeated)


class Notes(Column):
    def __init__(self):
        super(Notes, self).__init__()
        self.id = None
        self.title = None
        self.content = None
        self.created = None
        self.modified = None
        self.remind_id = None
        self.fold_id = None
        self.fold_name = None
        self.has_attachment = None
        self.attach_name = None
        self.html_content = None
        self.delete_flag = None
        self.has_todo = None
        self.attach_url = None
        
    def get_values(self):
        return (self.id, self.title, self.content, self.created, self.modified, self.remind_id, 
        self.fold_id, self.fold_name, self.has_attachment, self.attach_name, self.html_content, self.delete_flag, self.has_todo, self.attach_url) + super(Notes, self).get_values()


class Generate(object):

    def __init__(self, db_cache):
        self.db_cache = db_cache
        self.db = None
        self.cursor = None

    def get_models(self):
        models = []
        self.db = sqlite3.connect(self.db_cache)
        models.extend(self._get_model_notes())
        self.db.close()
        self.db = None
        return models

    def _get_model_notes(self):
        model = []
        self.cursor = self.db.cursor()
        self.cursor.execute(SQL_FIND_TABLE_NOTES)
        for row in self.cursor:
            note = Generic.Note()
            if row[9] is not None:
                for a in range(len(row[9].split(','))):
                    attachment = Generic.Attachment()
                    attachment.Filename.Value = row[9].split(',')[a]
                    attachment.URL.Value = row[13]
                    note.Attachments.Add(attachment)
            if row[10] is not None:
                note.Body.Value = row[10]
            if row[3] is not None:
                note.Creation.Value = TimeStamp.FromUnixTime(int(str(row[3])[0:-3:1]), False) if len(str(row[3])) > 10 else TimeStamp.FromUnixTime(row[3], False) if len(str(row[3])) == 10 else TimeStamp.FromUnixTime(0, False)
            if row[7] is not None:
                note.Folder.Value = row[7]
            if row[4] is not None:
                note.Modification.Value = TimeStamp.FromUnixTime(int(str(row[4])[0:-3:1]), False) if len(str(row[4])) > 10 else TimeStamp.FromUnixTime(row[4], False) if len(str(row[4])) == 10 else TimeStamp.FromUnixTime(0, False)
            if row[1] is not None:
                note.Title.Value = row[1]
            if row[2] is not None:
                note.Summary.Value = row[2]
            if row[14] is not None:
                note.SourceFile.Value = self._get_source_file(str(row[14]))
            model.append(note)
        self.cursor.close()
        self.cursor = None
        return model

    def _get_source_file(self, source_file):
        if isinstance(source_file, str):
            return source_file.replace('/', '\\')
        return source_file