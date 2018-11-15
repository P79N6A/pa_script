#coding:utf-8
#分析IOS System Ime的假脚本
import clr
try:
    clr.AddReference('input_method_all_in_one')
except:
    print("No module named input method all in one, parser exits!")

import input_method_all_in_one

def parse(root, extract_source, extract_deleted):
    return input_method_all_in_one.go(root, extract_source, extract_deleted)