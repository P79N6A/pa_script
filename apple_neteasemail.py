# coding=utf-8
import os
import traceback

import PA_runtime
from PA_runtime import * 
from model_mails import *

# 邮件内容类型
CONTENT_TYPE_HTML = 1 # HTML 格式
CONTENT_TYPE_TEXT = 2 # 纯文本


def analyze_neteasemail(node, extract_deleted, extract_source):
    """
        baiduMap(/Library/Preferences/com.baidu.map.plist)
        result: Account, SearchHistory, myhistoryAddress, Recent Visit Count
    """
    pr = ParserResults()
    res = NeteaseMailParser(node, extract_deleted, extract_source).parse()
    pr.Models.AddRange(res)
    return pr


class NeteaseMailParser(object):
    """
        网易邮箱大师    
    """
    def __init__(self, node, extract_deleted, extract_source):

        # node: '/Documents/imail.db'
        self.root = node 
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source

        self.mm = MM()        
        self.cachepath = ds.OpenCachePath("NeteaseMasterMail")
        self.cachedb = self.cachepath + "\\NeteaseMasterMail.db"
        self.mm.db_create(self.cachedb) 
        self.mm.db_create_table()
        self.accounts = {}
        self.mail_folder = {}

    def parse(self):

        self.parse_email(node=self.root.GetByPath("Documents/imail.db"))
        self.parse_todo(node=self.root.GetByPath("Documents/todo.db"))
        self.parse_contacts(node=self.root.GetByPath("Documents/contacts.db"))
        self.mm.db_close()

        generate = Generate(self.cachedb)
        return generate.get_models()
        

    def parse_email(self, node):
        """ 
            邮件内容 
        """

        imail_db = SQLiteParser.Database.FromNode(node)
        if imail_db is None:
            return
        
        # 邮件类型
        for rec in self.my_read_table(db=imail_db, table_name='mailBox'):
            self.mail_folder[rec['mailBoxId'].Value] = rec['name'].Value

        try:
            for rec in self.my_read_table(db=imail_db, table_name='mailAbstract'):
                mail = Mails()
                if IsDBNull(rec['subject'].Value):
                    continue
                mail.mailId = rec['localId'].Value if not IsDBNull(rec['localId'].Value) else None
                mail.mail_folder = self.mail_folder.get(rec['mailBoxId'].Value, None)
                mail.subject = rec['subject'].Value
                mail.abstract = rec['summary'].Value if not IsDBNull(rec['summary'].Value) else None
                mail.accountId = rec['accountRawId'].Value


                mail.fromEmail = self.convert_email_form(rec['mailFrom'].Value) if not IsDBNull(rec['mailFrom'].Value) else None

                mail.tos = self.convert_email_form(rec['mailTos'].Value) if not IsDBNull(rec['mailTos'].Value) else None

                # [{"name":"pangu_x01","email":"pangu_x01@163.com"}]
                # "cici" <848565664@qq.com
                mail.cc = self.convert_email_form(rec['ccs'].Value) if not IsDBNull(rec['ccs'].Value) else None
                mail.bcc = rec['bccs'].Value if not IsDBNull(rec['bccs'].Value) else None
                mail.isForward = rec['forwarded'].Value if not IsDBNull(rec['forwarded'].Value) else None
                mail.isRead = rec['unread'].Value ^ 1 if not IsDBNull(rec['mailTos'].Value) else None
                if rec['mailBoxId'].Value == 3:   # 发件箱
                    mail.sendStatus = 1
                elif rec['mailBoxId'].Value == 2: # 草稿箱
                    mail.sendStatus = 0
                mail.receiveUtc = rec['sentDate'].Value if not IsDBNull(rec['sentDate'].Value) else None
                mail.downloadSize = rec['size'].Value if not IsDBNull(rec['size'].Value) else None
                try:
                    self.mm.db_insert_table_mails(mail) 
                except Exception as e:
                    pass 
            self.mm.db_commit()
        except Exception as e:
            traceback.print_exc()

        # mail - content
        self.parse_email_content(imail_db)
        # mail - attachName
        self.parse_email_attachName(imail_db)
        # account
        self.parse_email_account(imail_db)
        # account - accountConfig -- password
        self.parse_email_password(imail_db)
        # attachment 
        self.parse_email_attachment(imail_db)

    def parse_todo(self, node):
        """ 
            待办事项 
        """
        try:
            todo_db = SQLiteParser.Database.FromNode(node)
            if todo_db is None:
                return
            for rec in self.my_read_table(db=todo_db, table_name='todoList'):
                t = Todo()
                t.content = rec['content'].Value
                t.createdTime = rec['createdTime'].Value
                t.reminderTime = rec['reminderTime'].Value
                t.done = rec['done'].Value
                t.isdeleted = rec['deleted'].Value
                try:
                    self.mm.db_insert_table_todo(t)
                except:
                    traceback.print_exc()
                    pass
            self.mm.db_commit()
        except Exception as e:
            traceback.print_exc()
            
    def parse_contacts(self, node):
        """ 
            联系人 
        """
        try:
            todo_db = SQLiteParser.Database.FromNode(node)
            if todo_db is None:
                return
            for rec in self.my_read_table(db=todo_db, table_name='recentcontact'):
                contact = Contact()
                contact.contactName = rec['name'].Value
                contact.contactEmail = rec['email'].Value
                try:
                    self.mm.db_insert_table_contact(contact)
                except:
                    traceback.print_exc()
                    pass
            self.mm.db_commit()
        except Exception as e:
            traceback.print_exc()

    def parse_email_content(self, imail_db):
        """ 
            mail - content 
        """
        SQL_UPDATE_EMAIL_CONTENT = '''
            update mails set content=? where mailId=?
            '''
        content_id = []
        try:
            for rec in self.my_read_table(db=imail_db, table_name='mailContent'):
                if rec['type'].Value == CONTENT_TYPE_TEXT: # 只提取 HTML 格式, 跳过纯文本类型
                    continue
                elif rec['type'].Value == CONTENT_TYPE_HTML:
                    mailId = rec['mailId'].Value
                    if mailId == 40:
                        continue
                    mailContent = rec['value'].Value
                    try:
                        self.mm.cursor.execute(SQL_UPDATE_EMAIL_CONTENT, (mailContent, mailId))
                    except:
                        pass
            self.mm.db_commit()
        except Exception as e:
            traceback.print_exc()
            
    def parse_email_attachName(self, imail_db):
        """ 
            mail - attachName 
        """
        SQL_UPDATE_EMAIL_ATTACHNAME = '''
            update mails set attachName=? where mailId=?
            '''
        accatchName_id = []
        for rec in self.my_read_table(db=imail_db, table_name='mailAttachment'):
            if IsDBNull(rec['name'].Value) or IsDBNull(rec['mailId'].Value):
                continue
            attachName = rec['name'].Value
            mailId = rec['mailId'].Value
            accatchName_id.append((attachName, mailId))
        # 去重
        _accatchName_id = {}
        for i in accatchName_id:
            if i[1] in _accatchName_id:
                _accatchName_id[i[1]] = ','.join([_accatchName_id[i[1]], i[0]])
            _accatchName_id[i[1]] = i[0]
        accatchName_id = []
        for k, v in _accatchName_id.iteritems():
            accatchName_id.append((v, k))
        try:
            self.mm.cursor.executemany(SQL_UPDATE_EMAIL_ATTACHNAME, accatchName_id)
            self.mm.db_commit()
        except Exception as e:
            traceback.print_exc()

    def parse_email_account(self, imail_db):
        """ 
            mail - account 
        """
        for rec in self.my_read_table(db=imail_db, table_name='account'):
            account = Accounts()
            account.accountId = rec['accountId'].Value 
            account.accountEmail = rec['email'].Value
            if rec['type'].Value == 1 and rec['deleted'].Value == 0:
                self.accounts[account.accountId] = {'email': account.accountEmail}
            try:
                self.mm.db_insert_table_account(account)
                self.mm.db_commit()
            except:
                pass

    def parse_email_password(self, imail_db):
        """ 
            mail - password 
        """
        SQL_UPDATE_ACCOUNTS_PASSWORD = '''
               update accounts set password=? where accountEmail=?
               ''' 
        password_accountEmail = []
        for accountId in self.accounts:
            table_name = 'accountConfig_{}'.format(accountId)
            for rec in self.my_read_table(db=imail_db, table_name=table_name):
                if rec['DBkey'].Value == 'wmsrPass':
                    a = self.accounts[accountId]['email']
                    b = rec['DBvalue'].Value
                    password_accountEmail.append((rec['DBvalue'].Value, self.accounts[accountId]['email']))
                    break
        try:
            self.mm.cursor.executemany(SQL_UPDATE_ACCOUNTS_PASSWORD, password_accountEmail)
            self.mm.db_commit()
        except Exception as e:
            traceback.print_exc()

    def parse_email_attachment(self, imail_db):
        """ 
            mail - attachment 
        """
        SQL_ASSOCIATE_TABLE_ATTACHMENT = '''
                select a.accountRawId as accountId, 
                       b.name         as attachName, 
                       b.createdate   as downloadUtc, 
                       b.size         as downloadSize, 
                       a.mailFrom     as fromEmail, 
                       a.sentDate     as mailUtc, 
                       b.localPath    as attachDir 
                from mailAbstract as a INNER JOIN mailAttachment as b
                       ON a.localId = b.mailId;'''
        imail_db_node = self.root.GetByPath("/Documents/imail.db")
        if(imail_db_node is None):
            return 
        imail_db_path = imail_db_node.PathWithMountPoint
        db = sqlite3.connect(imail_db_path)
        mails = Mails()
        try:
            if db is None:
                return []
            cursor = db.cursor()         
            cursor.execute(SQL_ASSOCIATE_TABLE_ATTACHMENT)
            row = []
            row = cursor.fetchone()
            while row is not None:
                attach = Attach()
                attach.accountEmail = self.accounts[row[0]]['email']
                attach.attachName = row[1]
                attach.downloadUtc = row[2]
                attach.downloadSize = row[3]
                attach.fromEmail = row[4]
                attach.mailUtc = row[5]
                attach.attachDir = row[6]
                try:
                    self.mm.db_insert_table_attach(attach)
                except:
                    pass 
                row = cursor.fetchone()
            self.mm.db_commit()
        except Exception as e:
            traceback.print_exc()
        finally:
            db.close()

    def my_read_table(self, table_name, db_path=None, db=None):
        """ 
            读取手机数据库
        """
        if db is None:
            try:
                account_node = self.root.GetByPath(db_path)
                db = SQLiteParser.Database.FromNode(account_node)
                if db is None:
                    return 
            except Exception as e:
                traceback.print_exc()
        tb = SQLiteParser.TableSignature(table_name)  
        return db.ReadTableRecords(tb, self.extract_deleted, True)


    def convert_email_form(self, name_email):
        """
            转换邮件格式
        """
        # [{"name":"pangu_x01","email":"pangu_x01@163.com"}]
        # "cici" <848565664@qq.com
        res = ''
        name_email = eval(name_email)
        try:
            if type(name_email) == list:
                for i in name_email:
                    name = i['name']
                    email = i['email']
                    res += '<' + name + '> ' + email + ';'
            elif type(name_email) == dict:
                name = name_email['name']
                email = name_email['email']
                res += '<' + name + '> ' + email + ';'
        except:
            res = None

        return res



        