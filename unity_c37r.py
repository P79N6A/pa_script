#coding:utf-8
#
# Fucking code, Do NOT READ ANY OF THEM
#

import shutil

def format_mac_timestamp(mac_time, v = 10):
    """
    from mac-timestamp generate unix time stamp
    """
    date = 0
    date_2 = mac_time
    if mac_time < 1000000000:
        date = mac_time + 978307200
    else:
        date = mac_time
        date_2 = date_2 - 978278400 - 8 * 3600
    s_ret = date if v > 5 else date_2
    return int(s_ret)

#
#   C SHARP GET FUCKING
#   you must ensure that your scripts code have import sqlite already!
#   and reader is a sqlite-reader object!
#
def c_sharp_get_string(reader, idx):
    return reader.GetString(idx) if not reader.IsDBNull(idx) else ""

def c_sharp_get_long(reader, idx):
    return reader.GetInt64(idx) if not reader.IsDBNull(idx) else 0

def c_sharp_get_blob(reader, idx):
    return bytearray(reader.GetValue(idx)) if not reader.IsDBNull(idx) else 0

def c_sharp_get_real(reader, idx):
    return reader.GetDouble(idx) if not reader.IsDBNull(idx) else 0.0

# using shutils to copy file
def mapping_file_with_copy(src, dst):
    if src is None or src is "":
        print('file not copied as src is empty!')
        return False
    if dst is None or dst is "":
        print('file not copied as dst is lost!')
        return False
    shutil.copy(src, dst)

# not implemented right now...
def mapping_file_with_safe_read(src, dst):
    pass