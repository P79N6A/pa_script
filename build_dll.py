import clr
DIR = "Builds/"

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
clr.CompileModules(DIR+"apple_mails.dll", "apple_mails.py")
clr.CompileModules(DIR+"apple_installs.dll", "apple_installs.py")
clr.CompileModules(DIR+"apple_sms.dll", "apple_sms.py")
clr.CompileModules(DIR+"apple_wechat.dll", "apple_wechat.py")
clr.CompileModules(DIR+"apple_qq.dll", "apple_qq.py")

import PA_runtime
print('Build Finished!!!')