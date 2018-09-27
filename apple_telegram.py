#coding:utf-8
import clr
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference('model_im')
    clr.AddReference('unity_c37r')
except:
    pass
del clr
import model_im
import os
import sys
from PA_runtime import *
import PA
from System.Text import *
from System.IO import *
import System.Data.SQLite as sql

import logging
import re
import sqlite3 
import hashlib
import struct
import unity_c37r
import random
import json
################################doc dict###########################
DocDict = {"image": ["image/jpeg", 
"image/pcx",
"image/png",
"image/svg+xml",
"image/tiff",
"image/vnd.djvu",
"image/vnd.wap.wbmp",
"image/x-cmu-raster", 
"image/x-coreldraw", 
"image/x-coreldrawpattern", 
"image/x-coreldrawtemplate", 
"image/x-corelphotopaint", 
"image/x-icon", 
"image/x-jg", 
"image/x-jng", 
"image/x-ms-bmp", 
"image/x-photoshop", 
"image/x-portable-anymap", 
"image/x-portable-graymap", 
"image/x-portable-pixmap", 
"image/x-rgb", 
"image/x-xbitmap", 
"image/x-xpixmap", 
"image/x-xwindowdump", 
"image/webp"], 
"audio": ["audio/3gpp",
 "audio/basic", 
 "audio/midi", 
 "audio/mpeg", 
 "audio/mpegurl", 
 "audio/prs.sid", 
 "audio/x-aiff", 
 "audio/x-gsm", 
 "audio/x-mpegurl",
 "audio/x-ms-wma", 
 "audio/x-ms-wax", 
 "audio/x-pn-realaudio", 
 "audio/x-realaudio", 
 "audio/x-scpls", 
 "audio/x-sd2", 
 "audio/x-wav", 
 "audio/ogg", 
 "audio/mp3", 
 "audio/mpg", 
 "audio/mpeg", 
 "audio/wav", 
 "audio/aifc", 
 "audio/aiff", 
 "audio/x-m4a", 
 "audio/x-mp4", 
 "audio/aacp", 
 "audio/m4a", 
 "audio/mp4", 
 "audio/caf", 
 "audio/aac", 
 "audio/ac3", 
 "audio/3gp"], 
 "video": ["video/3gpp",
"video/dl", 
"video/dv", 
"video/fli", 
"video/m4v", 
"video/mpeg", 
"video/mp4", 
"video/quicktime", 
"video/quicktime", 
"video/vnd.mpegurl", 
"video/x-la-asf", 
"video/x-mng", 
"video/x-ms-asf", 
"video/x-ms-wm", 
"video/x-ms-wmv", 
"video/x-ms-wmx", 
"video/x-ms-wvx", 
"video/x-msvideo", 
"video/x-sgi-movie"]
}
###################Telegram Object Keys###########################
PKVString = 1
PKVInt32 = 2
PKVInt64 = 3
PKVCustomClass = 4
PKVArray = 5
PKVData = 6
PKVInt32Array = 7
PKVDictionary = 8
PKVDouble = 9
###################Complex Decoder#################################
def hexdigest_u(num):
    r = hex(num)
    r = r.replace('0x', '')
    r = r.replace('L', '')
    return r

#稳得一笔！！！！！
class TeleBlobDecoder(object):
    def __init__(self, blob):
        self.stream = blob
        self.size = len(blob)
        self.idx = 0
        self.begin = 0
        self.end = self.size
    
    def read_int32(self):
        bts = self.stream[self.idx: self.idx + 4]
        val = struct.unpack('i', bts)[0]
        self.idx += 4
        return val

    def skip_int32(self):
        self.idx += 4
    
    def read_long(self):
        bts = self.stream[self.idx: self.idx + 8]
        self.idx += 8
        return struct.unpack('q', bts)[0]
    
    def skip_long(self):
        self.idx += 8
    
    def read_double(self):
        bts = self.stream[self.idx: self.idx + 8]
        self.idx += 8
        return struct.unpack('d', bts)[0]

    def skip_double(self):
        self.skip_long()
    
    def read_length(self):
        res = 0
        res |= self.stream[self.idx] & 0x7f
        step = 7
        while self.stream[self.idx] & 0x80:
            self.idx += 1
            res |= (self.stream[self.idx]) << step
            step += 7
            if step > 28:
                break
        self.idx += 1
        return res

    #[not implemented... note that!]
    def skip_length(self):
        return False

    def read_string(self):
        length = self.read_length()
        bts = self.stream[self.idx : self.idx + length]
        self.idx += length
        return bytearray(bts).decode('utf-8') # note this.... fuck!

    def skip_string(self):
        length = self.read_length()
        self.idx += length
        return True

    def skip_object(self):
        length = self.read_int32()
        self.idx += (length + 4)

    def skip_data(self): # may be wrong! check this for source code.
        length = self.read_length()
        self.idx += length
    
    def read_bytes(self, maxlen):
        length = self.read_length()
        sz = min(length, maxlen)
        bts = self.stream[self.idx: self.idx + sz]
        self.idx += length
        return  bytearray(bts)
        
    def read_data(self):
        length = self.read_length()
        bts = self.stream[self.idx: self.idx + length]
        self.idx += length
        return bts
    
    def read_object(self):
        length = self.read_int32()
        idx_end = self.idx + length
        idx_offset = 0
        while self.stream[self.idx + idx_offset] != 0:
            idx_offset += 1
        obj_name = bytearray(self.stream[self.idx : self.idx + idx_offset - 1]).decode('utf-8')
        self.idx += idx_offset
        length -= (idx_offset + 4)
        obj_data = self.stream[self.idx: self.idx + length]
        self.idx = idx_end
        return obj_name, obj_data

    @staticmethod
    def fake_mem_cmp(src, dst, offset, length):
        idx = 0
        while idx < length:
            if ord(src[idx]) != dst[idx + offset]:
                return True
            idx += 1
        return False
    
    def skip_value_for_raw_key(self, key, length):
        middle_idx = self.idx
        i = 0
        while i < 2:
            end_idx = self.end
            if i == 1:
                end_idx = middle_idx
                self.idx = self.begin
            while self.idx < end_idx:
                cmp_length = self.read_length()
                if cmp_length != length or self.fake_mem_cmp(key, self.stream, self.idx, length):
                    if cmp_length > 1000:
                        return False
                    self.idx += cmp_length
                    self.skip_field()
                    continue
                self.idx += cmp_length
                return True
            i += 1
        return False
    
    def skip_field(self):
        ftp = self.stream[self.idx]
        self.idx += 1
        if ftp == PKVString:
            self.skip_string()
        elif ftp == PKVInt32:
            self.skip_int32()
        elif ftp == PKVInt64:
            self.skip_long()
        elif ftp == PKVCustomClass: # fix it
            self.skip_object()
        elif ftp == PKVArray:
            self.skip_object()
        elif ftp == PKVData:
            self.skip_data()
        elif ftp == PKVInt32Array:
            pass # source not implemented...
        elif ftp == PKVDictionary:
            self.skip_object()
        elif ftp == PKVDouble:
            self.skip_double()
        else:
            raise TypeError('Data Type Not Supported! %d' % ftp)
        
    def decode_int_for_ckey(self, key):
        if self.skip_value_for_raw_key(key, len(key)): # replace true to skip value for raw key
            b = self.stream[self.idx]
            self.idx += 1
            if b == PKVString:
                # read string....
                r = self.read_string()
                return int(r)
            elif b == PKVInt32:
                val = self.read_int32()
                return val
            elif b == PKVInt64:
                r = self.read_long()
                return r
            else:
                self.skip_field()
                return 0
        return 0

    def decode_long_for_ckey(self, key):
        if self.skip_value_for_raw_key(key, len(key)):
            ftp = self.stream[self.idx]
            self.idx += 1
            if ftp == PKVString:
                r = self.read_string()
                return int(r)
            elif ftp == PKVInt32:
                return self.read_int32()
            elif ftp == PKVInt64:
                return self.read_long()
            else:
                self.skip_field
                return 0
        return 0
    
    # not implemented....
    def decode_arr_for_ckey(self, key):
        pass

    def decode_bytes_for_ckey(self, key, max_len):
        if self.skip_value_for_raw_key(key, len(key)):
            ftp = self.stream[self.idx]
            self.idx += 1
            if ftp == PKVData:
                bts = self.read_bytes(max_len)
                return bts
            else:
                self.skip_field()
        else:
            pass

    def decode_string_for_ckey(self, key):
        if self.skip_value_for_raw_key(key, len(key)):
            ftp = self.stream[self.idx]
            self.idx += 1
            if ftp == PKVString:
                r = self.read_string()
                return r
            elif ftp == PKVInt32:
                num = self.read_int32()
                return str(num)
            elif ftp == PKVInt64:
                num = self.read_long()
                return str(num)
        return ""
    
    def decode_data_for_ckey(self, key):
        if self.skip_value_for_raw_key(key, len(key)):
            ftp = self.stream[self.idx]
            self.idx += 1
            if ftp == PKVData:
                bts = self.read_data()
                return bts
            else:
                self.skip_field()
    
    def decode_object_for_ckey(self, key):
        if self.skip_value_for_raw_key(key, len(key)):
            ftp = self.stream[self.idx]
            self.idx += 1
            if ftp == PKVCustomClass:
                return self.read_object()
            else:
                self.skip_field()

####################################### Media Parser############################################
class SimpleStream(object):
    def __init__(self, blob):
        self.stream = blob
        self.idx = 0
        self.max_size = len(blob)
        self.is_valid = self.max_size != 0
    
    def read_int(self):
        if self.idx + 4 > self.max_size:
            raise IOError('size over flow!')
        bts = self.stream[self.idx: self.idx + 4]
        self.idx += 4
        return struct.unpack('i', bts)[0]
    
    def read_long(self):
        if self.idx + 8 > self.max_size:
            raise IOError('size over flow!')
        bts = self.stream[self.idx: self.idx + 8]
        self.idx += 8
        return struct.unpack('q', bts)[0]
    
    def read_double(self):
        if self.idx + 8 > self.max_size:
            raise IOError('size over flow!')
        bts = self.stream[self.idx: self.idx + 8]
        self.idx += 8
        return struct.unpack('d', bts)[0]
    
    def read_float(self):
        if self.idx + 4 > self.max_size:
            raise IOError('size over flow!')
        bts = self.stream[self.idx: self.idx + 4]
        self.idx += 4
        return struct.unpack('f', bts)[0]

    def read_byte(self):
        if self.idx + 1 > self.max_size:
            raise IOError('size over flow')
        bt = self.stream[self.idx]
        self.idx += 1
        return int(bt)
    
    def read_short(self):
        if self.idx +2 > self.max_size:
            raise IOError('size over flow!')
        bt = self.stream[self.idx: self.idx + 2]
        self.idx += 2
        return struct.unpack('h', bt)[0]
    
    def read_bytes(self, length):
        if self.idx + length > self.max_size:
            raise IOError('size over flow!')
        bts = self.stream[self.idx: self.idx + length]
        self.idx += length
        return bytearray(bts)
    
    def skip(self, length):
        if self.idx + length > self.max_size:
            raise IOError('size over flow!')
        self.idx += length
    
    def reuse(self):
        self.idx = 0
# message types
TGMessageActionNone = 0
TGMessageActionChatEditTitle = 1
TGMessageActionChatAddMember = 2
TGMessageActionChatDeleteMember = 3
TGMessageActionCreateChat = 4
TGMessageActionChatEditPhoto = 5
TGMessageActionContactRequest = 6
TGMessageActionAcceptContactRequest = 7
TGMessageActionContactRegistered = 8
TGMessageActionUserChangedPhoto = 9
TGMessageActionEncryptedChatRequest = 10
TGMessageActionEncryptedChatAccept = 11
TGMessageActionEncryptedChatDecline = 12
TGMessageActionEncryptedChatMessageLifetime = 13
TGMessageActionEncryptedChatScreenshot = 14
TGMessageActionEncryptedChatMessageScreenshot = 15
TGMessageActionCreateBroadcastList = 16
TGMessageActionJoinedByLink = 17
TGMessageActionChannelCreated = 18
TGMessageActionChannelCommentsStatusChanged = 19
TGMessageActionChannelInviter = 20
TGMessageActionGroupMigratedTo = 21
TGMessageActionGroupDeactivated = 22
TGMessageActionGroupActivated = 23
TGMessageActionChannelMigratedFrom = 24
TGMessageActionPinnedMessage = 25
TGMessageActionClearChat = 26
TGMessageActionGameScore = 27
TGMessageActionPhoneCall = 28
TGMessageActionPaymentSent = 29
TGMessageActionCustom = 1000
TGMessageUnkownAction = 3000

def generate_right_number(num):
    val = bytearray()
    val.append(num&0xff)
    val.append((num >> 8 ) & 0xff)
    val.append((num >> 16 ) & 0xff)
    val.append((num >> 24 ) & 0xff)
    return struct.unpack('i', val)[0]


# attachment types
TGAudio = generate_right_number(0x3A0E7A32)
TGVideo = generate_right_number(0x338EAA20)
TGImage = generate_right_number(0x269BD8A8)
TGLocation = generate_right_number(0x0C9ED06E)
TGUnsupportted = generate_right_number(0x3837BEF7)
TGPlist = generate_right_number(0x157b8516)
TGWeb = generate_right_number(0x584197af)
TGEntities = generate_right_number(0x8c2e3cce)
TGAction = generate_right_number(0x1167E28B)
TGDocument = generate_right_number(0xE6C64318)
TGContact = generate_right_number(0xB90A5663)
TGReplyMarker = generate_right_number(0x5678acc1)
TGReplyMessage = 414002169
TGForwardMessage = generate_right_number(0xAA1050C1)
TGViaUser = generate_right_number(0xA3F4C8F5)
TGLocalMessage = generate_right_number(0x944DE6B6)

class MediaParser(object):
    def __init__(self, blob, handler):
        self.res = list()
        if len(blob) == 0:
            return
        self.stream = SimpleStream(blob)
        count = self.stream.read_int()
        for i in range(0, count):
            tp = self.stream.read_int()
            if not handler.__contains__(tp):
                print('unkown type, media parse exit!')
                f = open('D:/webs/unspported%d' %random.randint(0, 0xffffffff), 'wb')
                f.write(blob)
                f.close()
                break
            r = handler[tp](self.stream)
            if type(r) is bytearray:
                print('warning, you want to return a bytes! %d' %tp)
            elif r is not None:
                self.res.append(r)
            else:
                pass
    @staticmethod
    def image_parser(bts):
        """
            bts is a stream
            ret is a image attachment...
        """
        res = dict()
        res['photos'] = list()
        if not bts.is_valid:
            return None
        length = bts.read_int()
        v = 1
        if length == 0x7abacaf1:
            v = bts.read_byte()
            length = bts.read_int()
        image_id = bts.read_long()
        date = bts.read_int()
        hl = bts.read_byte()
        hl = (hl != 0)
        if hl:
            latitude = bts.read_double()
            longitude = bts.read_double()
        hi = True if bts.read_byte() != 0 else False
        if hi:
            cnt = bts.read_int()
            s_v = 0
            if cnt&(1 << 31) != 0:
                cnt &= ~(1 << 31)
                cnt &= 0xffffffff
                s_v = bts.read_short()
            i = 0
            while i < cnt:
                s_length = bts.read_int()
                hash_source = bts.read_bytes(s_length).decode('utf-8')
                res['photos'].append(hash_source)
                bts.skip(8)
                if s_v >= 1:
                    bts.skip(4)
                i += 1
        bts.skip(8)
        if v >= 2:
            cap_length = bts.read_int()
            #bts.skip(cap_length) # caption
            caption = bts.read_bytes(cap_length).decode('utf-8')
            res['title'] = caption
        if v >= 3:
            hs = bts.read_byte()
            dl = bts.read_int()
            bts.skip(dl)
        return res
    
    @staticmethod
    def video_parse(bts):
        res = dict()
        res['video'] = dict()
        length = bts.read_int()
        version = 1
        if length == 0x7abacaf1:
            version = bts.read_byte()
            length = bts.read_int()
        m_vid = bts.read_long()
        #m_video_id = "remote{}.mov" # TODO fix that 16-base
        m_vid = "remote{}.mov".format(hexdigest_u(m_vid))
        res['video']['video_name'] = m_vid
        bts.skip(8)
        local_id = bts.read_long() # 16-base
        l_id = hexdigest_u(local_id)
        hv = 0
        hv = bts.read_byte()
        if hv:
            vi_cnt = bts.read_int()
            i = 0
            while i < vi_cnt:
                _len = bts.read_int()
                m_str = bts.read_bytes(_len).decode('utf-8')
                res['title'] = m_str
                quality = bts.read_int()
                _tsize = bts.read_int()
                i += 1
        ht = 0
        ht = bts.read_byte()
        if ht:
            res['photos'] = list()
            cnt = 0
            v = 0
            cnt = bts.read_int()
            if cnt & (1 << 31):
                cnt &= ~(0x80000000)
                cnt &= 0xffffffff
                v = bts.read_short()
            i = 0
            while i < cnt:
                _len = 0
                _len = bts.read_int()
                t_path = bts.read_bytes(_len).decode('utf-8')
                res['photos'].append(t_path)
                width = struct.unpack('f', bts.read_bytes(4))
                height = struct.unpack('f', bts.read_bytes(4))
                if v >= 1:
                    file_size = bts.read_int()
                i += 1
        duration = bts.read_int()
        bts.skip(4)
        bts.skip(4)
        if version >= 2:
            clen = bts.read_int()
            res['title'] = bts.read_bytes(clen).decode('utf-8')
        if version >= 3:
            hs = bts.read_byte()
            slen = bts.read_int()
            bts.skip(slen)
        if version >= 4:
            bts.skip(1)
        return res

    @staticmethod
    def music_parse(bts):
        length = bts.read_int()
        m_id = bts.read_long()
        bts.skip(12)
        m_remote_id = bts.read_long()
        duration = bts.read_int()

    @staticmethod
    def document_parse(bts):
        res = dict()
        length = bts.read_int()
        v = bts.read_byte()
        if v < 1 or v > 6:
            return None
        if v >= 2:
            l_d_id = bts.read_long()
        d_id = bts.read_long() #file id
        a_hash = bts.read_long()
        dc_id = bts.read_int()
        uid = bts.read_int()
        date = bts.read_int()
        f_len = bts.read_int()
        f_name = bts.read_bytes(f_len).decode('utf-8')
        mime_length = bts.read_int()
        mime_type = bts.read_bytes(mime_length).decode('utf-8')
        k = 'unknow'
        if DocDict['audio'].__contains__(mime_type):
            k = 'daudio'
        elif DocDict['image'].__contains__(mime_type):
            k = 'dphotos'
        elif DocDict['video'].__contains__(mime_type):
            k = 'dvideo'
        full_path = '{}/{}'.format(hexdigest_u(d_id), f_name)
        res[k] = full_path
        sz = bts.read_int()
        ht = bts.read_byte()
        #thumbs
        if ht:
            cnt = 0
            s_v = 0
            cnt = bts.read_int()
            if cnt & 0x80000000:
                cnt &= ~(0x80000000)
                cnt = generate_right_number(cnt)
                s_v = bts.read_short()
            i = 0
            res['photos'] = list()
            while i < cnt:
                s_len = bts.read_int()
                url = bts.read_bytes(s_len).decode('utf-8')
                res['photos'].append(url)
                width = struct.unpack('f', bts.read_bytes(4))[0]
                height = struct.unpack('f', bts.read_bytes(4))[0]
                if v > 1:
                    bts.read_int()
                i += 1
        if v >= 3:
            url_length = bts.read_int()
            if url_length > 0:
                uri = bts.read_bytes(url_length).decode('utf-8')
                res['uri'].append(uri)
        if v >= 4:
            at_size = bts.read_int()
            if at_size > 0:
                attribute = bts.read_bytes(at_size).decode('utf-8', 'ignore')
                res['attr'] = attribute
        if v >= 5:
            cap_length = bts.read_int()
            if cap_length > 0:
                caption = bts.read_bytes(cap_length).decode('utf-8')
                res['title'] = caption
        if v >= 6:
            f_v = bts.read_int()
        return res

    @staticmethod
    def plist_parse(bts):
        length = bts.read_int()
        b = bts.read_bytes(length)
        return dict() # change this to memory data, then BPReader may handle it
    
    @staticmethod
    def web_attachment_parse(bts):
        length = bts.read_int()
        b = bts.read_bytes(length)
        return dict()

    @staticmethod
    def entites_parse(bts):
        return MediaParser.web_attachment_parse(bts)
    
    @staticmethod
    def action_parse(bts):
        """
        complex method, may be wrong for a while!
        """
        length = bts.read_int()
        tp = bts.read_int()
        print('id_tp:%d' %tp)
        length -= 4
        res = dict()
        if tp == TGMessageActionChatAddMember or tp == TGMessageActionChatDeleteMember:
            uid = bts.read_int()
            length -=4 
            res['title'] = ''
            if length >= 4:
                uids_cnt = bts.read_int()
                length -= 4
                i = 0
                while length > 0 and i < uids_cnt:
                    l_uid = bts.read_int()
                    res['title'] += '{} joined or left chat\n'.format(l_uid)
                    length -= 4
                    i += 1
        elif tp == TGMessageActionJoinedByLink:
            uid = bts.read_int()
            res['title'] = '{} joined chat by link'.format(uid)
        elif tp == TGMessageActionChatEditTitle:
            s_length = bts.read_int()
            title = bts.read_bytes(s_length).decode('utf-8')
            res['title'] = 'chat tilte changed: {}'.format(title)
        elif tp == TGMessageActionCreateChat:
            s_length = 0
            s_length = bts.read_int()
            #TODO add title to attchament title
            title = bts.read_bytes(s_length) 
            res['title'] = 'chat {} created, uid list:\n'
            cnt = bts.read_int()
            i = 0
            while i < cnt:
                #TODO add uid to change id list
                uid = bts.read_int()
                res['title'] += str(uid) + ','
                i += 1
        elif tp == TGMessageActionChatEditPhoto:
            image = MediaParser.image_parser(bts) # change photos?
            res = image ###> photos....
        elif tp == TGMessageActionContactRequest:
            hp = bts.read_int()
            res = None
        elif tp == TGMessageActionContactRegistered:
            res = None
        elif tp == TGMessageActionUserChangedPhoto:
            image = MediaParser.image_parser(bts)
            res = image
        elif tp == TGMessageActionEncryptedChatRequest:
            res = None
        elif tp == TGMessageActionEncryptedChatMessageLifetime:
            msg_life_time = bts.read_int()
            res = None
        elif tp == TGMessageActionChannelCreated:
            s_length = bts.read_int()
            title = bts.read_bytes(s_length).decode('utf-8')
            res['title'] = 'chat {} created'.format(title)
        elif tp == TGMessageActionChannelCommentsStatusChanged:
            en = bts.read_byte()
            res = None
        elif tp == TGMessageActionChannelInviter:
            uid =  bts.read_int()
            res = None
        elif tp == TGMessageActionGroupMigratedTo: # migrate channel
            cid = bts.read_int() 
            res = None
        elif tp == TGMessageActionChannelMigratedFrom:
            s_length = bts.read_int()
            title = bts.read_bytes(s_length).decode('utf-8')
            gid = bts.read_int()
            res['title'] = 'this channel is migrated from {}, id: {}'.format(title, gid)
        elif tp == TGMessageActionGameScore:
            game_id = bts.read_int()
            score = bts.read_int()
            res['title'] = 'game_id:{}, score:{}'.format(game_id, score)
        elif tp == TGMessageActionPhoneCall:
            call_id = bts.read_long()
            reason = bts.read_int()
            duration = bts.read_int()
            res['call'] = dict()
            res['call']['call_id'] = call_id
            res['call']['duration'] = duration
        elif tp == TGMessageActionPaymentSent:
            c_length = bts.read_int()
            title = bts.read_bytes(c_length).decode('utf-8')
            t_amount = bts.read_int()
            r = dict()
            r['title'] = title
            r['money'] = t_amount
            res['deal'] = r
        return res
    
    @staticmethod
    def location_parse(bts):
        length = bts.read_int()
        if length == 0:
            return
        latitude = bts.read_double()
        longitude = bts.read_double()
        if length >= 20:
            v_length = bts.read_int()
            bts.skip(v_length)
        res = dict()
        res['location'] = dict()
        res['location']['lati'] = latitude
        res['location']['longti'] = longitude
        return res
    
    @staticmethod
    def contact_parse(bts):
        data_length = bts.read_int()
        uid = bts.read_int()
        flen = bts.read_int()
        first_name = bts.read_bytes(flen).decode('utf-8')
        flen = bts.read_int()
        last_name = bts.read_bytes(flen).decode('utf-8')
        flen = bts.read_int()
        phone = bts.read_bytes(flen).decode('utf-8')
        res = dict()
        res['contact'] = dict()
        res['contact']['uid'] = uid
        res['contact']['name'] = first_name + last_name
        res['contact']['phone'] = phone
        return res
    
    @staticmethod
    def un_implment_parse(bts):
        length = bts.read_int()
        bts.skip(length)
    
    @staticmethod
    def reply_parse(bts):
        at = MediaParser.web_attachment_parse(bts)
        return at
    
    @staticmethod
    def reply_attachment_parse(bts):
        length = bts.read_int()
        sub_blob = bts.read_bytes(length)
        d = TeleBlobDecoder(sub_blob)
        d.decode_object_for_ckey('replyMessage') # may be not useful...
        d.decode_int_for_ckey('replyMessageId')

    @staticmethod
    def forward_parse(bts):
        magic = bts.read_int()
        dlength = 0
        version = 0
        if magic == 0x72413faa:
            version = 2
            dlength = bts.read_int()
        elif magic == 0x72413fab:
            version = 3
            dlength = bts.read_int()
        elif magic == 0x72413fac:
            version = 4
            dlength = bts.read_int()
        elif magic == 0x72413fad:
            version = 5
            dlength = bts.read_int()
        else:
            dlength = magic
        # parse....
        if version >= 2:
            bts.read_long() # forward peer id
        else:
            bts.read_int() # foward peer id
        f_date = bts.read_int()
        f_mid = bts.read_int()
        if version >= 3:
            bts.read_int()
            bts.read_int()
        if version >= 4:
            bts.read_long() # source peer id
        if version >= 5:
            sign_length = bts.read_int()
            sign = bts.read_bytes(sign_length).decode('utf-8')
    
    def construct_parse(self):
        pass
    
    @staticmethod
    def local_message_parse(bts):
        data_length = bts.read_int()
        cnt = bts.read_int()
        res = dict()
        res['uimage'] = list()
        for i in range(0, cnt):
            res['uimage'].append(MediaParser.image_info_parse(bts))
        cnt = bts.read_int()
        res['ulimage'] = list()
        for i in range(0, cnt):
            length = bts.read_int()
            url = bts.read_bytes(length).decode('utf-8')
            res['ulimage'].append(url)
            length = bts.read_int()
            fpath = bts.read_bytes(length).decode('utf-8')
        m_id = bts.read_int()
        return res

    @staticmethod
    def image_info_parse(bts):
        v = 0
        cnt = bts.read_int()
        res = list()
        if cnt & (0x80000000):
            cnt &= ~(0x80000000)
            cnt &= 0xffffffff
            v = bts.read_short()
        for i in range(0, cnt):
            length = bts.read_int()
            url = bts.read_bytes(length).decode('utf-8')
            res.append(url)
            w = bts.read_float()
            h = bts.read_float()
            if v >= 1:
                bts.read_int()
        return res
            


MediaParseDict = dict()
MediaParseDict[TGImage] = MediaParser.image_parser
MediaParseDict[TGVideo] = MediaParser.video_parse
MediaParseDict[TGAudio] = MediaParser.music_parse
MediaParseDict[TGPlist] = MediaParser.plist_parse
MediaParseDict[TGWeb] = MediaParser.web_attachment_parse
MediaParseDict[TGEntities] = MediaParser.entites_parse
MediaParseDict[TGAction] = MediaParser.action_parse
MediaParseDict[TGDocument] = MediaParser.document_parse
MediaParseDict[TGLocation] = MediaParser.location_parse
MediaParseDict[TGContact] = MediaParser.contact_parse
MediaParseDict[TGReplyMarker] = MediaParser.reply_parse
MediaParseDict[TGReplyMessage] = MediaParser.reply_attachment_parse
MediaParseDict[TGForwardMessage] = MediaParser.forward_parse
MediaParseDict[TGViaUser] = MediaParser.un_implment_parse
MediaParseDict[TGLocalMessage] = MediaParser.local_message_parse

def md5(string):
    return hashlib.md5(string).hexdigest()

#####################Get Functions######################################
def GetString(reader, idx):
    return reader.GetString(idx) if not reader.IsDBNull(idx) else ""

def GetInt64(reader, idx):
    return reader.GetInt64(idx) if not reader.IsDBNull(idx) else 0

def GetBlob(reader, idx):
    return reader.GetValue(idx) if not reader.IsDBNull(idx) else None


class Telegram(object):
    
    def __init__(self, root, container_root, extract_deleted, extract_source):
        self.root = root
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.account = None
        self.cache = ds.OpenCachePath('Tg')
        self.im = model_im.IM()
        self.im.db_create(self.cache + '/C37R')
        self.container = container_root
        self.__find_account()

    def __find_account(self):
        def_node = self.root.GetByPath("Documents/standard.defaults")
        if def_node is not None:
            p = BPReader.GetTree(def_node.Data)
            self.account = p["telegraphUserId"].Value
            if self.account is None:
                print ('Get Telegram UserID Failed! Try to Load From App Documents')
            else:
                return
        else:
            print('try to find account from container documents, old version detected...')
            pnode = self.container.GetByPath('Library/Preferences/ph.telegra.Telegraph.plist')
            if pnode is None:
                raise IOError("Can't find Accounts! parse EXIT!")
            p = BPReader.GetTree(pnode.Data)
            self.account = p['telegraphUserId'].Value
            if self.account is None:
                raise IOError("Can't find Accounts! parse EXIT!")
            else:
                return
    
    def __get_photo(self, photo_id):
        pass

    def get_photo_from_id(self, photo_id):
        hash_str = unity_c37r.md5(photo_id)
        f_name = os.path.join(self.root.PathWithMountPoint, 'Caches/%s' %hash_str)
        if os.path.exists(f_name):
            return '{}/Caches/{}'.format(self.root.AbsolutePath, hash_str)
        else:
            return ""
    
    def get_file_from_document(self, doc_path):
        pname = os.path.join(self.root.PathWithMountPoint, 'Documents/files/%s' %doc_path)
        if os.path.exists(pname):
            return os.path.join(self.root.AbsolutePath, 'Documents/files/%s' %doc_path)
        else:
            return ""

    def get_video(self, vi_path):
        pname = os.path.join(self.root.PathWithMountPoint, 'Documents/video/%s' % vi_path)
        if os.path.exists(pname):
            return os.path.join(self.root.AbsolutePath, 'Documents/video/%s' % vi_path)
        else:
            return ""

    @staticmethod
    def create_empty_message(src_msg):
        msg = model_im.Message()
        msg.msg_id = src_msg.msg_id + random.randint(0, 0xffffffff)
        msg.send_time = src_msg.send_time
        msg.sender_id = src_msg.sender_id
        msg.is_sender = src_msg.is_sender
        msg.talker_id = src_msg.talker_id
        return msg
    
    def parse_media_attachments(self, blob, src_msg):
        m = src_msg
        mp = MediaParser(blob, MediaParseDict)
        for ms in mp.res:
            t = ms.get('title')
            if t is not None and t is not '':
                msg = self.create_empty_message(m)
                msg.content = t
                msg.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                self.im.db_insert_table_message(msg)
            l = ms.get('photos')
            if l is not None:
                for pid in l:
                    pname = self.get_photo_from_id(pid)
                    if pname == "":
                        continue
                    msg = self.create_empty_message(m)
                    msg.media_path = pname
                    msg.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                    self.im.db_insert_table_message(msg)
            if ms.__contains__('dphotos'):
                p = ms.get('dphotos')
                if p is None or p is '':
                    pass
                else:
                    msg = self.create_empty_message(m)
                    pres = self.get_file_from_document(p)
                    if pres is '':
                        msg.content = 'document attachment image not cached...'
                        msg.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    else:
                        msg.media_path = pres
                        msg.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                    self.im.db_insert_table_message(msg)
            elif ms.__contains__('dvideo'):
                p = ms.get('dvideo')
                if p is None or p is '':
                    pass
                else:
                    msg = self.create_empty_message(m)
                    pres = self.get_file_from_document(p)
                    if pres is '':
                        msg.content = 'document attachment video not cached...'
                        msg.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    else:
                        msg.media_path = pres
                        msg.type = model_im.MESSAGE_CONTENT_TYPE_VIDEO
                    self.im.db_insert_table_message(msg)
            elif ms.__contains__('daudio'):
                p = ms.get('daudio')
                if p is None or p is '':
                    pass
                else:
                    msg =self.create_empty_message(m)
                    pres = self.get_file_from_document(p)
                    if pres is '':
                        msg.content = 'document attahcment audio not cached...'
                        msg.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    else:
                        msg.media_path = pres
                        msg.type = model_im.MESSAGE_CONTENT_TYPE_VOICE
                    self.im.db_insert_table_message(msg)
            elif ms.__contains__('uri'):
                p = ms.get('uri')
                if p is None or p is '':
                    pass
                else:
                    msg = self.create_empty_message(m)
                    msg.content = 'uri:%s' %ms.get('uri')
                    msg.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    self.im.db_insert_table_message(msg)
            elif ms.__contains__('location'):
                p = ms.get('location')
                if p is None or len(p) == 0:
                    pass
                else:
                    m.type = model_im.MESSAGE_CONTENT_TYPE_LOCATION
                    locate = model_im.Location()
                    locate.latitude = p.get('lati')
                    locate.longitude = p.get('longti')
                    locate.location_id = random.randint(0,0xffffffff)
                    m.extra_id = locate.location_id
                    self.im.db_insert_table_location(locate)
            if ms.__contains__('contact'):
                p = ms.get('contact')
                if p is None or len(p) == 0:
                    pass
                else:
                    m.type = model_im.MESSAGE_CONTENT_TYPE_CONTACT_CARD
                    m.content = json.dumps(p)
            if ms.__contains__('video'):
                p = ms.get('video')
                if p is None or len(p) is 0:
                    pass
                else:
                    msg = self.create_empty_message(m)
                    msg.type = model_im.MESSAGE_CONTENT_TYPE_VIDEO
                    msg.media_path = self.get_video(p.get('video_name'))
                    self.im.db_insert_table_message(msg)
            if ms.__contains__('call'):
                p = ms.get('call')
                if p is None or len(p) is 0:
                    pass
                else:
                    src_msg.content = 'call id:{}, duration:{} second(s)'.format(p.get('call_id'), p.get('duration'))
            if ms.__contains__('deal'):
                p = ms.get('deal')
                if p is None or len(p) == 0:
                    pass
                else :
                    deal = model_im.Deal()
                    deal.description = p.get('title')
                    deal.money = p.get('money')
                    src_msg.extra_id = deal.deal_id
                    src_msg.type = model_im.MESSAGE_CONTENT_TYPE_ATTACHMENT
                    src_msg.content = 'f'
                    self.im.db_insert_table_deal(deal)
            if ms.__contains__('uimage'):
                p = ms.get('uimage')
                if len(p) == 0:
                    pass
                else:
                    for q in p:
                        msg = self.create_empty_message(m)
                        msg.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                        msg.media_path = q
                        self.im.db_insert_table_message(msg)
            if ms.__contains__('ulimage'):
                p = ms.get('ulimage')
                if len(p) == 0:
                    pass
                else:
                    for q in p:
                        msg = self.create_empty_message(m)
                        msg.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                        msg.media_path = q
                        self.im.db_insert_table_message(msg)
                        
    def parse(self):
        if self.account is None:
            print('Get Account Id Failed, Parse Ret!')
            return
        
        tg_node = self.root.GetByPath('Documents/tgdata.db')
        conn = sql.SQLiteConnection('Data Source = {}; Readonly=True'.format(tg_node.PathWithMountPoint))
        conn.Open()
        cmd = sql.SQLiteCommand(conn)
        cmd.CommandText = '''
        select uid, first_name, last_name, phone_number, photo_small, photo_medium, photo_big, last_seen, username from users_v29
        where uid = {}'''.format(self.account)
        reader = cmd.ExecuteReader()
        if reader.Read():
            a = model_im.Account()
            idx = 0
            a.account_id = GetInt64(reader, 0)
            a.nickname = GetString(reader, 2) + GetString(reader, 1)
            a.telephone = GetString(reader, 3)
            p = self.get_photo_from_id(GetString(reader, 6))
            if p is "":
                p = self.get_photo_from_id(GetString(reader, 5))
            if p is "":
                p = self.get_photo_from_id(GetString(reader, 4))
            a.photo = p
            a.username = GetString(reader, 8)
            self.im.db_insert_table_account(a)
        else:
            print("this is not the right group!")
            return
        cmd.Dispose()
        cmd.CommandText = '''
        select uid, first_name, last_name, phone_number, photo_small, photo_medium, photo_big, username from users_v29
        where uid != {}
        '''.format(self.account)
        reader = cmd.ExecuteReader()
        while reader.Read():
            f = model_im.Friend()
            f.friend_id = GetInt64(reader, 0)
            f.nickname = GetString(reader, 2) + GetString(reader, 1)
            f.telephone = GetString(reader, 3)
            p = self.get_photo_from_id(GetString(reader, 6))
            if p is "":
                p = self.get_photo_from_id(GetString(reader, 5))
            if p is "":
                p = self.get_photo_from_id(GetString(reader, 4))
            f.photo = p
            # user name?
            f.account_id = self.account
            self.im.db_insert_table_friend(f)
        cmd.Dispose()
        self.im.db_commit()
        # channels...
        cmd.CommandText = '''
            select cid, data from channel_conversations_v29
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            g = model_im.Chatroom()
            g.account_id = self.account
            g.chatroom_id = GetInt64(reader, 0)
            blob = bytearray(GetBlob(reader, 1))
            decoder = TeleBlobDecoder(blob)
            decoder.decode_long_for_ckey('i') # cid
            decoder.decode_long_for_ckey('ah') # acess hash
            decoder.decode_int_for_ckey('dv')
            decoder.decode_int_for_ckey('kind')
            decoder.decode_int_for_ckey('pts')
            decoder.decode_bytes_for_ckey('vsort', 9)
            decoder.decode_bytes_for_ckey('isort', 9)
            decoder.decode_bytes_for_ckey('usort', 9)
            decoder.decode_int_for_ckey('mread')
            decoder.decode_int_for_ckey('moutread')
            decoder.decode_int_for_ckey('mknown')
            decoder.decode_int_for_ckey('mlr')
            decoder.decode_int_for_ckey('mrd')
            decoder.decode_int_for_ckey('mrod')
            about = decoder.decode_string_for_ckey('about')
            g.description = about
            decoder.decode_string_for_ckey('username')
            decoder.decode_int_for_ckey('out')
            decoder.decode_int_for_ckey('unr')
            decoder.decode_int_for_ckey('der')
            decoder.decode_int_for_ckey('ds')
            decoder.decode_int_for_ckey('date')
            decoder.decode_int_for_ckey('from')
            decoder.decode_string_for_ckey('text')
            decoder.decode_data_for_ckey('media') # note that...
            decoder.decode_int_for_ckey('ucount')
            decoder.decode_int_for_ckey('sucount')
            g.name = decoder.decode_string_for_ckey('ct') # nick name
            ps = self.get_photo_from_id(decoder.decode_string_for_ckey('cp.s')) # photo small
            pm = self.get_photo_from_id(decoder.decode_string_for_ckey('cp.m')) # photo medium
            pb = self.get_photo_from_id(decoder.decode_string_for_ckey('cp.l')) # photo big
            if pb is not "":
                g.photo = pb
            elif pm is not "":
                g.photo = pm
            else:
                g.photo = ps
            decoder.decode_int_for_ckey('ver')
            decoder.decode_int_for_ckey('adm')
            decoder.decode_int_for_ckey('role')
            decoder.decode_int_for_ckey('ro')
            decoder.decode_long_for_ckey('flags')
            decoder.decode_int_for_ckey('lef')
            decoder.decode_int_for_ckey('kk')
            decoder.decode_int_for_ckey('mtci')
            decoder.decode_long_for_ckey('mtch')
            decoder.decode_string_for_ckey('rr')
            decoder.decode_int_for_ckey('pmi')
            decoder.decode_int_for_ckey('ccd')
            decoder.decode_int_for_ckey('pdt')
            self.im.db_insert_table_chatroom(g)
        cmd.Dispose()
        # Friend messages
        # 稳的一笔 C37R
        cmd.CommandText = '''
            select mid, cid, message, media, from_id, to_id,  outgoing, date from messages_v29
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            m = model_im.Message()
            m.msg_id = GetInt64(reader, 0)
            m.is_sender = GetInt64(reader, 6)
            m.content = GetString(reader, 2)
            m.sender_id = GetInt64(reader, 4)
            m.talker_id = GetInt64(reader, 5) if m.is_sender == 1 else m.sender_id
            m.send_time = GetInt64(reader, 7)
            # parse media.... excited.
            try:
                blob = bytearray(GetBlob(reader, 3))
                if blob is None or len(blob) == 0:
                    pass
                else:
                    self.parse_media_attachments(blob, m)
            except Exception as e:
                print(e)
            if m.content == '' or m.content is None:
                continue
            self.im.db_insert_table_message(m)
        self.im.db_commit()
        cmd.Dispose()
        cmd.CommandText = '''
            select cid, data, mid from channel_messages_v29
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            m = model_im.Message()
            m.talker_id = GetInt64(reader, 0)
            bts = GetBlob(reader, 1)
            m.msg_id = GetInt64(reader, 2)
            decoder = TeleBlobDecoder(bts)
            decoder.decode_int_for_ckey('i')
            decoder.decode_bytes_for_ckey('sk', 17)
            decoder.decode_int_for_ckey('pts')
            decoder.decode_int_for_ckey('unr')
            decoder.decode_int_for_ckey('out')
            decoder.decode_int_for_ckey('ds')
            m.sender_id = decoder.decode_long_for_ckey('fi')
            to_id = decoder.decode_long_for_ckey('ti')
            cid = decoder.decode_long_for_ckey('ci')
            m.content = decoder.decode_string_for_ckey('t')
            m.send_time = decoder.decode_int_for_ckey('d')
            medias = decoder.decode_data_for_ckey('md')
            try:
                if len(medias) == 0 or medias is None:
                    pass
                else:
                    self.parse_media_attachments(medias, m)
            except Exception as e:
                print(e)
            decoder.decode_int_for_ckey('rd')
            decoder.decode_long_for_ckey('ri')
            decoder.decode_int_for_ckey('lt')
            decoder.decode_long_for_ckey('f')
            decoder.decode_int_for_ckey('sqi')
            decoder.decode_int_for_ckey('sqo')
            if m.content is '' or m.content is None:
                continue
            self.im.db_insert_table_message(m)
        self.im.db_commit()

def try_to_get_telegram_group(grps):
    res = list()
    for g in grps:
        try:
            node = g.GetByPath('Documents/tgdata.db')
            if node is None:
                continue
            res.append(g)
        except:
            continue
    return res

def parse_telegram(root, extract_deleted, extract_source):
    group_container_nodes = ds.GroupContainers.ToArray()
    r_nodes = try_to_get_telegram_group(group_container_nodes)
    try:
        if len(r_nodes) is 0:
            print('''can't find group node''')
            raise IOError('E')
        res = list()
        for r in r_nodes:
            try:
                t = Telegram(r, root, False, False)
                t.parse()
                models = model_im.GenerateModel(t.cache + '/C37R').get_models()
                res.append(models)
            except:
                if canceller.IsCancellationRequested:
                    raise IOError('E')
                else:
                    continue
        mlm = ModelListMerger()
        pr = ParserResults()
        pr.Categories = DescripCategories.QQ
        pr.Models.AddRange(list(mlm.GetUnique(res)))
        pr.Build('钉钉')
    except Exception as e:
        print(e)
        pr = ParserResults()
    return pr
    