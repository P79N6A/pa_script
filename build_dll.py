import clr
DIR = "Builds/"

clr.CompileModules(DIR+"PA_runtime.dll", "PA_runtime.py")
clr.CompileModules("apple_apps.dll", "apple_apps.py")
clr.CompileModules("apple_ab.dll", "apple_ab.py")
clr.CompileModules("apple_notes.dll", "apple_notes.py")
clr.CompileModules("apple_locations.dll", "apple_locations.py")
clr.CompileModules("apple_calenders.dll", "apple_calenders.py")
clr.CompileModules("apple_safari.dll", "apple_safari.py")
clr.CompileModules("apple_mails.dll", "apple_mails.py")
clr.CompileModules("apple_recents.dll", "apple_recents.py")
clr.CompileModules("apple_cookies.dll", "apple_cookies.py")
clr.CompileModules("apple_calls.dll", "apple_calls.py")
clr.CompileModules("apple_mails.dll", "apple_mails.py")
clr.CompileModules("apple_mails.dll", "apple_mails.py")
clr.CompileModules("apple_installs.dll", "apple_installs.py")
clr.CompileModules("apple_sms.dll", "apple_sms.py")
clr.CompileModules("apple_wechat.dll", "apple_wechat.py")
clr.CompileModules("apple_qq.dll", "apple_qq.py")

import PA_runtime
print('Build Finished!!!')