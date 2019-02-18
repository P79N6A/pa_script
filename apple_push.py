#coding=utf-8
import os
import PA_runtime
from PA_runtime import *

class iphone_aps_parser(object):
    def __init__(self, node, extract_deleted, extract_source):
        try:
            self.aps_node = node.GetByPath("/ApplePushService/aps.db")
        except:
            self.aps_node = None
        try:
            self.push_store_directory = node.GetByPath('SpringBoard/PushStore') 
        except:
            self.push_store_directory = None
        
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source

        self.aps_topics = {  
                    'com.tencent.mttlite':["QQ push notification",self.parse_QQ_aps_record],    
                    'com.google.Gmail':["Gmail push notification",self.parse_generic_aps_record],    
                    'com.tencent.xin':["WeChat push notification",self.parse_generic_aps_record],  # 
                    'com.atebits.Tweetie2':["Twitter push notification",self.parse_twitter_aps_record],  
                    'com.facebook.Messenger':["Facebook messenger push notification",self.parse_facebook_aps_record],    
                    'com.facebook.Facebook':["Facebook push notification",self.parse_facebook_aps_record],    
                    'net.whatsapp.WhatsApp':["Whatsapp push notification",self.parse_whatsapp_aps_record],    #
                    'com.apple.madrid':["iMessage push notification",self.parse_iMessages_aps_record],
                    'com.gogii.textplus':["TextPlus push notification",self.parse_generic_aps_record],
                    'com.skype.skype':["Skype push notification",self.parse_skype_aps_record],   #
                    'pinterest':["Pinterest push notification",self.parse_pinterest_aps_record]   #
                    }
        unknown_topics = ['com.pinger.textfreeWithVoice',] 

    def im_equal(self,im1,im2):             
        return im1.Body.Value == im2.Body.Value and im1.Source.Value == im2.Source.Value       

    def im_hash(self,notification): 
        return hash(notification.TimeStamp.Value)

    def im_better(self,im1,im2):
        
        if im1.Deleted == DeletedState.Intact and im2.Deleted == DeletedState.Deleted:
            return True
        for field in im1.GetFields():            
            if im2.GetFieldByName(field.Key).HasContent and not im1.GetFieldByName(field.Key).HasContent:
                return False
        return True
        
    
    def parse(self):        
        results = self.parse_aps_db() + self.parse_push_store()        
        mlm = ModelListMerger()     
        mlm.UpdateModelEqualityFunction(Notification, self.im_equal)        
        mlm.UpdateIsBetterFunction(Notification, self.im_better)
        unique_notifications = mlm.GetUniqueList(results)    
        pr = ParserResults()
        pr.Models.AddRange(unique_notifications)
        return pr
       

    def parse_push_store(self):                
        push_store_results = []
        if self.push_store_directory:
            for node in self.push_store_directory:
                try:
                    topic = node.Name.split('.pushstore')[0] 
                except:
                    continue
                tree = BPReader.GetTree(node)    
                if tree != None and tree.Value != "$null":                                    
                    push_store_results.extend(self.parse_push_store_entry(topic,tree))
                    
        return push_store_results

    def parse_push_store_entry(self,topic,tree):        
        parsed_results = []
        for notification_dict in tree.Value:                  
            notification = Notification()   
            if 'AppNotificationUserInfo' in notification_dict.Children:       
                try:
                    if topic in self.aps_topics:                       
                        aps_dict = notification_dict['AppNotificationUserInfo']                                    
                        notification = self.aps_topics[topic][1](aps_dict)                                         
                        if notification is None:                                                                                        
                            notification = Notification()                                                                
                except:
                    pass                
            if 'AppNotificationTitle' in notification_dict.Children and not notification.Source.HasContent:    
                source_application = [notification_dict['AppNotificationTitle'].Value,MemoryRange(notification_dict['AppNotificationTitle'].Source) \
                                        if self.extract_source else None]
                notification.Subject.Init(source_application[0] +" push notification",source_application[1])
            if 'AppNotificationCreationDate' in notification_dict.Children and not notification.TimeStamp.HasContent: 
                creation_date = [TimeStamp(notification_dict['AppNotificationCreationDate'].Value.Value.ToUniversalTime(),True), \
                                MemoryRange(notification_dict['AppNotificationCreationDate'].Source) if self.extract_source else None]
                notification.TimeStamp.Init(creation_date[0],creation_date[1])                
            if 'AppNotificationMessage' in notification_dict.Children and not notification.Body.HasContent:  
                body = [notification_dict['AppNotificationMessage'].Value,MemoryRange(notification_dict['AppNotificationMessage'].Source) \
                                        if self.extract_source else None]
                notification.Body.Init(body[0],body[1])
            notification.Deleted = tree.Deleted     
            if not notification.Source.HasContent:
                notification.Source.Value = topic
            parsed_results.append(notification)            
        return parsed_results


    def parse_aps_db(self):     
        results = []  
        if not self.aps_node:
            return results
        db=SQLiteParser.Database.FromNode(self.aps_node)          
        ts = SQLiteParser.TableSignature('incoming_message') 
          
        if db != None:    
            for rec in db.ReadTableRecords(ts, self.extract_deleted):   
                try:          
                    tree = BPReader.GetTree(MemoryRange(rec['payload'].Source))                                 
                    if tree != None:
                        pass
                    if rec['topic'].Value in self.aps_topics:               
                        source = self.aps_topics[rec['topic'].Value][0]               
                        aps_notification = self.aps_topics[rec['topic'].Value][1](tree)                               
                    else:
                        aps_notification = None                
                    if aps_notification != None:                              
                        aps_notification.Source.Value = source
                        aps_notification.Deleted = rec.Deleted
                        results.append(aps_notification)
                except:
                    continue
        return results

    def parse_generic_aps_record(self,tree):         
        if tree != None and 'aps' in tree.Children and 'alert' in tree.Children['aps'].Children:            
            aps = tree.Children['aps']        
            body = [aps.Children['alert'].Value,MemoryRange(aps.Children['alert'].Source) if self.extract_source else None]
            notification = self.create_aps_notification({'Body':body})
            return notification
        else:
            return None
    def parse_pinterest_aps_record(self,tree):                    
        if tree != None and 'aps' in tree.Children and 'alert' in tree.Children['aps'].Children:                                    
            aps = tree.Children['aps']             
            alert = aps['alert']
            web_addresses = []                  
            if 'body' in alert.Children:                                
                body = [alert['body'].Value, MemoryRange(alert['body'].Source) if self.extract_source else None]                                
            else:
                body = None
            if 'id' in alert.Children:                
                notification_id = [alert['id'].Value, MemoryRange(alert['id'].Source) if self.extract_source else None]   
            else:
                notification_id = None
            if 'img' in alert.Children:                
                image_url = [alert['img'].Value, MemoryRange(alert['img'].Source) if self.extract_source else None]
                web_address = WebAddress()
                web_address.Deleted = tree.Deleted
                web_address.Value.Init(image_url[0],image_url[1])
                web_address.Domain.Value = "image URL"
                web_addresses.append(web_address)
            if 'url' in alert.Children:                
                notification_url = [alert['url'].Value, MemoryRange(alert['url'].Source) if self.extract_source else None]
                web_address = WebAddress()
                web_address.Deleted = tree.Deleted
                web_address.Value.Init(notification_url[0],notification_url[1])
                web_address.Domain.Value = "notification URL"
                web_addresses.append(web_address)        
            

            notification = self.create_aps_notification({"Body":body,"Urls":web_addresses,"NotificationId":notification_id})
            
            return notification
        return None

    def parse_skype_aps_record(self,tree):        
        if tree != None and 'aps' in tree.Children:                    
            aps = tree['aps']   # aps dictionary
            if 'category' in aps:
                category = [aps['category'].Value,MemoryRange(aps['category'].Source)]  
            if 'alert' in aps.Children:   
                loc_args = aps['alert']['loc-args']
                sender_name = [loc_args[0].Value,MemoryRange(loc_args[0].Source) if self.extract_source else None]
                body = [loc_args[1].Value,MemoryRange(loc_args[1].Source) if self.extract_source else None]

                sender_party = Party()                                                    
                sender_party.Identifier.Init(sender_name[0],sender_name[1])
                sender_party.Role.Value = PartyRole.From

                timestamp = [TimeStamp.FromUnixTime(int(tree['i'].Value)/1E3), MemoryRange(tree['i'].Source) if self.extract_source else None]

                notification = self.create_aps_notification({'TimeStamp':timestamp,"Body":body,"Participants":[sender_party],"Subject":category})            
                
                return notification
        return None


    def parse_iMessages_aps_record(self,tree):             
        if tree != None and 'sP' in tree.Children and 'tP' in tree.Children:        
            dictionary = tree.Children                          

            sender_phone_number = [dictionary['sP'].Value,MemoryRange(dictionary['sP'].Source) if self.extract_source else None]

            sender_party = Party()                                                    
            sender_party.Identifier.Init(sender_phone_number[0],sender_phone_number[1])
            sender_party.Role.Value = PartyRole.From

            tP = [dictionary['tP'].Value,MemoryRange(dictionary['tP'].Source) if self.extract_source else None] 
        
            user_party = Party()                                             
            user_party.Identifier.Init(tP[0],tP[1])
            user_party.Role.Value = PartyRole.To

            timestamp = [TimeStamp.FromUnixTime(int(dictionary['e'].Value)/1E9), MemoryRange(dictionary['e'].Source) if self.extract_source else None]

            notification = self.create_aps_notification({'TimeStamp':timestamp,'Participants':[sender_party],'To':user_party})
            return notification
        else:
            return None


    def parse_QQ_aps_record(self,tree):                
        if tree != None and 'aps' in tree.Children:                    
            aps = tree['aps'].Children    
            if 'alert' in aps:            
                body = [aps['alert'].Value,MemoryRange(aps['alert'].Source) if self.extract_source else None]
                if 'extra' in tree.Children:                
                    extra = tree['extra'].Children                 
                    URL = [extra['ourl'].Value, MemoryRange(extra['ourl'].Source) if self.extract_source else None]
                    web_address = WebAddress()
                    web_address.Deleted = tree.Deleted
                    web_address.Value.Init(URL[0],URL[1])
                    web_address.Domain.Value = "URL"
                    web_address = [web_address]
                else:   
                    web_address = None             

                notification = self.create_aps_notification({"Body":body,"Urls":web_address})            
                return notification
        return None


    def parse_whatsapp_aps_record(self,tree):           
        if tree != None and 'aps' in tree.Children:                
            timestamp = [TimeStamp.FromUnixTime(int(int(tree['id'].Value[:10]))),MemoryRange(tree['id'].Source) if self.extract_source else None]
            aps = tree['aps'].Children   
            body = [aps['alert'].Children['body'].Value,MemoryRange(aps['alert'].Children['body'].Source) if self.extract_source else None]
            whatsapp_server_ip = [aps['i'].Value,aps['i'].Source]    
            sender_phone_number = [aps['u'].Value,MemoryRange(aps['u'].Source) if self.extract_source else None]

            sender_party = Party()                                                    
            sender_party.Identifier.Init(sender_phone_number[0],sender_phone_number[1])
            sender_party.Role.Value = PartyRole.From
        
        
            notification = self.create_aps_notification({"Body":body,"Participants":[sender_party],"TimeStamp":timestamp})
            return notification

        else:
            return None

    def create_aps_notification(self,field_dict):    
        notification = Notification()

        for field_name in field_dict:        
            if "Single" in str(type(notification.GetFieldByName(field_name))):   
                try:             
                    notification.GetFieldByName(field_name).Value = field_dict[field_name]   
                except:
                    pass
            elif "Multi" in str(type(notification.GetFieldByName(field_name))):                
                notification.GetFieldByName(field_name).AddRange(field_dict[field_name])   
            elif field_dict[field_name] != None:                              
                notification.GetFieldByName(field_name).Init(field_dict[field_name][0],field_dict[field_name][1])
                
        return notification

    def parse_facebook_aps_record(self,tree):     
        if tree != None and 'p' in tree.Children and 'aps' in tree.Children:    
            aps = tree.Children['aps']   
            p_dict = tree.Children['p']    
            if 'a' in p_dict.Children:    
                sender_facebook_id = [p_dict['a'].Value,MemoryRange(p_dict['a'].Source) if self.extract_source else None]
            elif 'o' in p_dict.Children:   
                if '_' in p_dict['o'].Value:
                    sender_facebook_id = [p_dict['o'].Value.split('_')[0],MemoryRange(p_dict['o'].Source) if self.extract_source else None]   
                    comment_id = [p_dict['o'].Value.split('_')[1],MemoryRange(p_dict['o'].Source) if self.extract_source else None] 
                else:                                                                                                          
                    sender_facebook_id = [p_dict['o'].Value,MemoryRange(p_dict['o'].Source) if self.extract_source else None]       
            else: 
                 sender_facebook_id = None
            sender_party = Party()                
            if sender_facebook_id:                                    
                sender_party.Identifier.Init(sender_facebook_id[0],sender_facebook_id[1])
            sender_party.Role.Value = PartyRole.From

            if 's' in p_dict.Children:    # Message
                timestamp_key = 's'    
                timestamp_factor = 1E3    
            elif 'i' in p_dict.Children:    
                timestamp_key = 'i'
                timestamp_factor = 1E6   
            timestamp = [TimeStamp.FromUnixTime(int(int(p_dict[timestamp_key].Value)/timestamp_factor)),    \
                         MemoryRange(p_dict[timestamp_key].Source) if self.extract_source else None]       
            if 'body' in aps['alert'].Children:
                body = [aps['alert'].Children['body'].Value,MemoryRange(aps['alert'].Children['body'].Source) if self.extract_source else None]
            else:
                body = [aps['alert'].Value,MemoryRange(aps['alert'].Source) if self.extract_source else None]

            notification = self.create_aps_notification({'Participants':[sender_party],'TimeStamp':timestamp,'Body':body})
            return notification
        else:
            return None

    def parse_twitter_aps_record(self,tree):            
        if tree != None and 'aps' in tree.Children:                
            aps = tree['aps'].Children   
            if 'alert' in aps:
                sender = [tree['D'].Value,MemoryRange(tree['D'].Source) if self.extract_source else None] 
                chat_id = [tree['cid'].Value,MemoryRange(tree['cid'].Source) if self.extract_source else None] 
                user = [tree['R'].Value,MemoryRange(tree['R'].Source) if self.extract_source else None]       
                body = [aps['alert'].Value,MemoryRange(aps['alert'].Source) if self.extract_source else None]
                sender_party = Party()                                                    
                sender_party.Identifier.Init(sender[0],sender[1])
                sender_party.Role.Value = PartyRole.From

                user_party = Party()                                                    
                user_party.Identifier.Init(user[0],user[1])
                user_party.Role.Value = PartyRole.To
                twitter_msg_dict = {'Body':body,'Participants':[sender_party],'To':user_party}  
                if len(chat_id[0]) > 0:
                    notes = {'Notes':["Message from chat id: " +str(chat_id[0])]}    # Chat ID's might be 
                    twitter_msg_dict.update(notes)                                                                         
                notification = self.create_aps_notification(twitter_msg_dict)
            
                return notification
        
        else:
            return None

def analyze_apple_push_service(aps_node,extract_deleted,extract_source):
    aps_parser = iphone_aps_parser(aps_node,extract_deleted,extract_source)
    pr = aps_parser.parse()
    pr.Build('苹果推送服务')
    return pr