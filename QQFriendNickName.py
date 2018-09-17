# coding:utf-8
import os
import sys
import clr
try:
    clr.AddReference('friendlist_pb2')
except:
    pass
del clr
from friendlist_pb2 import QQDicGroupModel
from friendlist_pb2 import QQGroupModel
import collections

class TeaCipher(object):
    def __init__(self, key, round):
        self.key = key
        self.round = 16
        self.sum = 0
        self.delta = 0x9e3779b9

    def decrypt_block(self, in_bytes, out_bytes, pos):
        k_list = list()
        i = 0
        while i < 16:
            k_list.append(self.key[i + 3] | self.key[i + 2] << 8 | self.key[i + 1] << 16 | self.key[i + 0] << 24)
            i += 4
        i = 0
        y = in_bytes[pos + 3] | in_bytes[pos + 2] << 8 | in_bytes[pos + 1] << 16 | in_bytes[pos + 0] << 24
        z = in_bytes[pos + 7] | in_bytes[pos + 6] << 8 | in_bytes[pos + 5] << 16 | in_bytes[pos + 4] << 24
        sum = (self.delta << 4) & 0xffffffff
        while i < self.round:
            z -= ((y << 4) & 0xffffffff) + k_list[2] ^ y + sum ^(y >> 5) + k_list[3]
            z = z & 0xffffffff
            y -= ((z << 4) & 0xffffffff) + k_list[0] ^ z + sum ^(z >> 5) + k_list[1]
            y = y & 0xffffffff
            sum -= self.delta
            i += 1
        i = 3
        while i >= 0:
            out_bytes[pos + i] = y & 0xff
            out_bytes[pos + 4 + i] = z & 0xff
            z >>= 8
            y >>= 8
            i -= 1

    def decrypt_blob(self, in_buffer):
        sz = len(in_buffer)
        print  (sz)
        if sz % 8 != 0 or sz < 16:
            raise IOError
        dest_bytes = [0] * 8

        self.decrypt_block(in_buffer, dest_bytes, 0)

        n_pad_len = dest_bytes[0] & 0x7
        i = sz - 1 - n_pad_len - 2 - 7 # talk about format later...
        if i < 0:
            raise IOError
        sz_out = i
        zero_bytes = [0] * 8

        idx_pos = 8
        idx_next = 1
        idx_next += n_pad_len # filter padding
        i = 1
        idx_prev = 0
        idx_cur = 0
        first_bytes = True
        # filtering salt...
        while i <= 2:
            if idx_next <8:
                idx_next += 1
                i += 1
            elif idx_next == 8:
                first_bytes = False
                idx_prev = idx_pos - 8
                idx_cur = idx_pos
                j = 0
                while j < 8:
                    if idx_pos + j >= sz:
                        raise IOError
                    dest_bytes[j] ^= in_buffer[j + idx_pos]
                    j += 1
                self.decrypt_block(dest_bytes, dest_bytes, 0)
                idx_pos += 8
                idx_next = 0


        n_plain = sz_out
        out_bytes = bytearray([0] * n_plain)
        idx_out_pos = 0
        while n_plain:
            if idx_next < 8:
                out_bytes[idx_out_pos] = dest_bytes[idx_next] ^ (zero_bytes[idx_next] if first_bytes  else in_buffer[idx_prev + idx_next])
                idx_next += 1
                idx_out_pos += 1
                n_plain -= 1
            elif idx_next == 8:
                first_bytes = False
                idx_prev = idx_pos - 8 # BUG!!!! add a variable to control first bytes decode!!1
                idx_cur = idx_pos
                j = 0
                while j < 8:
                    if idx_pos +j >= sz:
                        raise IOError
                    dest_bytes[j] ^= in_buffer[j + idx_pos]
                    j += 1
                self.decrypt_block(dest_bytes, dest_bytes, 0)
                idx_pos += 8
                idx_next = 0
        i = 1
        while i < 8:
            if idx_next < 8:
                if dest_bytes[idx_next] ^ in_buffer[idx_prev + idx_next]:
                    raise IOError
                idx_next += 1
                i += 1
            elif idx_next == 8:
                idx_prev = idx_pos - 8
                idx_cur = idx_pos
                j = 0
                while j < 8:
                    if idx_pos + j > sz:
                        raise  IOError
                    dest_bytes[j] ^= in_buffer[j + idx_pos]
                    j += 1
                self.decrypt_block(dest_bytes, dest_bytes, 0)
                idx_pos += 8
                idx_next = 0
        return sz_out, out_bytes

    def decrypt_file(self, fname, f_out):
        f = open(fname, 'rb')
        f.seek(0,2)
        sz = f.tell()
        f.seek(0)
        val = f.read(sz)
        f.close()
        sz, buf = self.decrypt_blob(bytearray(val))
        f_o = open(f_out, 'wb')
        f_o.write(buf)
        f_o.close()

    def decrypt_file_buffer(self, fname):
        f = open(fname, 'rb')
        f.seek(0,2)
        sz = f.tell()
        f.seek(0)
        val = f.read(sz)
        f.close()
        return self.decrypt_blob(bytearray(val))

def decode(serialdata):
        groups = QQDicGroupModel()
        groups.ParseFromString(str(serialdata))    
        groupinfo = collections.defaultdict()
        friendsinfo = collections.defaultdict()
        for group in groups.groupList:
            name =(group.groupName)        
            groupinfo[name] = group.friendList
            for friend in group.friendList:
                #friendsinfo[friend.fuin] = friend.realNickName
                friendsinfo[friend.fuin] = (friend.nick,name)
        print (friendsinfo)
        return groupinfo,friendsinfo

def getFriendNickName(db):
        t = TeaCipher(bytearray("QQFriendListSave"), 16)
        #"QQFriendList_v3.plist"
        buf  = t.decrypt_file_buffer(db)    
        return decode(buf[1])
       
if __name__ == "__main__":
        #t.decrypt_file("QQFriendList_v3.plist","xxx")
        getFriendNickName("QQFriendList_v3.plist")
