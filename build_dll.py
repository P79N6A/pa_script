import clr
import os

DIR = "Builds/"

def GetIthmbWidth(name):
    pixMap = {
        "3303":(24,22),
        "3306":(39,39),
        "3309":(64,64),
        "3319":(125,125),
        "3319":(160,157),
        "3141":(160,158),
        "3041":(80,79),
        "4131":(240,240),
        "4031":(120,120),
        "4132":(64,64),
        "4032":(32,32),
        "4140":(336,332),
        "4040":(168,166)
    }
    return pixMap.get(name,(110,110))

rows,cols = GetIthmbWidth("3041")

for (root,dirs,files) in os.walk('.'):
    for filename in files:
        splits = os.path.splitext(filename)
        if splits[1] == ".py":
            clr.CompileModules(DIR+splits[0], filename)

clr.CompileModules(DIR+"PA_runtime.dll", "PA_runtime.py")
clr.CompileModules(DIR+"apple_apps.dll", "apple_apps.py")
clr.CompileModules(DIR+"apple_ab.dll", "apple_ab.py")
clr.CompileModules(DIR+"apple_notes.dll", "apple_notes.py")
clr.CompileModules(DIR+"apple_locations.dll", "apple_locations.py")
clr.CompileModules(DIR+"apple_calenders.dll", "apple_calenders.py")
clr.CompileModules(DIR+"apple_safari.dll", "apple_safari.py")
clr.CompileModules(DIR+"apple_mails.dll", "apple_mails.py")
clr.CompileModules(DIR+"apple_recents.dll", "apple_recents.py")
clr.CompileModules(DIR+"apple_cookies.dll", "apple_cookies.py")
clr.CompileModules(DIR+"apple_calls.dll", "apple_calls.py")
clr.CompileModules(DIR+"apple_mails.dll", "apple_mails.py")
clr.CompileModules(DIR+"apple_installs.dll", "apple_installs.py")
clr.CompileModules(DIR+"apple_sms.dll", "apple_sms.py")
clr.CompileModules(DIR+"apple_wechat.dll", "apple_wechat.py")
clr.CompileModules(DIR+"apple_qq.dll", "apple_qq.py")
clr.CompileModules(DIR+"fs_exFat.dll", "fs_exFat.py")
clr.AddReference
print('Build Finished!!!')