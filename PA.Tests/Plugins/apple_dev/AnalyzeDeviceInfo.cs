using PA.Common.Logger;
using PA.Formats.NextStep;
using PA.InfraLib.Data;
using PA.InfraLib.Extensions;
using PA.InfraLib.Files;
using PA.InfraLib.Plugins;
using PA.InfraLib.Utils;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace apple_dev
{
    /// <summary>
    /// c#插件,非常简单,这是一个使用c#来编写PNFA数据解析插件的示例
    /// 在config.dat的配置如下:
    /*<dict>
      <key>Module</key>
      <string>apple_dev</string>
      <key>Runners</key>
      <array>
        <dict>
          <key>EntryPoint</key>
          <string>apple_dev.AnalyzeDeviceInfo</string>
          <key>ExcuteOrder</key>
          <!-- ExcuteOrder 越大, 执行优先级越高 -->
          <integer>999</integer>
          <key>RunnerType</key>
          <integer>0</integer>
          <!-- ModuleType = 1 代表这是一个c#语言写的插件 -->
          <key> ModuleType</key>
          <integer>1</integer>
          <key>Patterns</key>
          <array>
            <!-- # 表示直接取文件系统根节点 -->
            <string>#</string>
          </array>
          <key>Category</key>
          <string>DeviceInfo</string>
          <key>Name</key>
          <string>设备信息(苹果)</string>
          <key>Filter</key>
          <string>基础数据</string>
        </dict>
      </array>
    </dict>
    /// */
    /// </summary>
    public class AnalyzeDeviceInfo : INativePlugin
    {
        static void FetchDeviceInfo(DeviceInfo devInfo,NSDictionary plist,string key,string plistKey)
        {
            if(plist.ContainsKey(plistKey))
            {
                devInfo.Add(key, plist[plistKey].ToString());
            }
        }

        public object Process(Node node, bool extractDeleted, bool extractSource)
        {
            ParserResults pr = new ParserResults();
            if (node is FileSystem fs) //这个插件期望node是文件系统
            {
                MetaData meta = new MetaData();
                var ds = fs.DataStore;
                try
                {
                    //analyze_csidata
                    //private/var/root/Library/Lockdown/data_ark.plist
                    var data_ark = fs.Search("/Library/Lockdown/data_ark.plist$").FirstOrDefault();
                    if (data_ark != null && data_ark.Type == NodeType.File)
                    {
                        var plist = PlistHelper.ReadPlist<NSDictionary>(data_ark);
                        FetchDeviceInfo(ds.DeviceInfo, plist, DeviceInfoKeys.Language.ToString(), "com.apple.international-Language");
                        FetchDeviceInfo(ds.DeviceInfo, plist, DeviceInfoKeys.BuildVersion.ToString(), "com.apple.who-uptodate_build");
                        FetchDeviceInfo(ds.DeviceInfo, plist, DeviceInfoKeys.LastBackupComputerName.ToString(), "com.apple.iTunes.backup-LastBackupComputerName");
                        FetchDeviceInfo(ds.DeviceInfo, plist, DeviceInfoKeys.DeviceName.ToString(), "-DeviceName");
                        FetchDeviceInfo(ds.DeviceInfo, plist, DeviceInfoKeys.TimeZone.ToString(), "-TimeZone");
                        FetchDeviceInfo(ds.DeviceInfo, plist, DeviceInfoKeys.ActivationStateAcknowledged.ToString(), "-ActivationStateAcknowledged");
                        FetchDeviceInfo(ds.DeviceInfo, plist, DeviceInfoKeys.Locale.ToString(), "com.apple.international-Locale");
                        FetchDeviceInfo(ds.DeviceInfo, plist, DeviceInfoKeys.LastBackupComputerType.ToString(), "com.apple.iTunes.backup-LastBackupComputerType");
                    }

                    //System/Library/LaunchDaemons/com.apple.atc.plist
                    var atcNode = fs.Search("/Preferences/com.apple.atc.plist$").FirstOrDefault();
                    if(atcNode!=null && atcNode.Type==NodeType.File)
                    {
                        var plist = PlistHelper.ReadPlist<NSDictionary>(atcNode);
                        if(plist!=null)
                        {
                            var diskUsage = plist.SafeGetObject<NSDictionary>("DiskUsage");

                            FileSize GetDiskUsageSize(NSDictionary dict, string key)
                            {
                                var subdict = dict.SafeGetObject<NSDictionary>(key);
                                if (subdict != null)
                                {
                                    return new FileSize(subdict.SafeGetInt64("_PhysicalSize"));
                                }
                                return new FileSize(0);
                            }
                            ds.DeviceInfo.Add(new MetaDataField(DeviceInfoKeys.VoiceMemoSize.ToString(),
                                GetDiskUsageSize(diskUsage, "VoiceMemo")));
                            ds.DeviceInfo.Add(new MetaDataField(DeviceInfoKeys.UserDataSize.ToString(),
                                GetDiskUsageSize(diskUsage, "UserData")));
                            ds.DeviceInfo.Add(new MetaDataField(DeviceInfoKeys.BookDataSize.ToString(),
                                GetDiskUsageSize(diskUsage, "Book")));
                            ds.DeviceInfo.Add(new MetaDataField(DeviceInfoKeys.MediaDataSize.ToString(),
                                GetDiskUsageSize(diskUsage, "Media")));
                            ds.DeviceInfo.Add(new MetaDataField(DeviceInfoKeys.ApplicationSize.ToString(),
                                GetDiskUsageSize(diskUsage, "Application")));
                            ds.DeviceInfo.Add(new MetaDataField(DeviceInfoKeys.LogsSize.ToString(),
                                GetDiskUsageSize(diskUsage, "Logs")));
                            ds.DeviceInfo.Add(new MetaDataField(DeviceInfoKeys.RingtoneSize.ToString(),
                                GetDiskUsageSize(diskUsage, "Ringtone")));

                            ds.DeviceInfo.Add(new MetaDataField(DeviceInfoKeys.TotalDiskSize.ToString(),
                                new FileSize(diskUsage.SafeGetInt64("_PhysicalSize"))));
                            ds.DeviceInfo.Add(new MetaDataField(DeviceInfoKeys.FreeDiskSize.ToString(),
                                new FileSize(diskUsage.SafeGetInt64("_FreeSize"))));
                        }
                    }

                    var projFile = ds.ProjectState.ProjectFile;
                    if (!string.IsNullOrEmpty(projFile) && File.Exists(projFile))
                    {
                        var plist = PlistHelper.ReadPlist<NSDictionary>(projFile);
                        if (plist != null)
                        {
                            var deviceInfo = plist.SafeGetObject<NSDictionary>("Device");
                            if (deviceInfo != null)
                            {
                                foreach (var p in deviceInfo)
                                {
                                    var key = p.Key;
                                    var obj = p.Value;
                                    if (obj is NSString nss)
                                    {
                                        ds.DeviceInfo.Add(new MetaDataField(key, nss.Content));
                                    }
                                    else if (obj is NSNumber nsn)
                                    {
                                        if (nsn.isBoolean())
                                        {
                                            ds.DeviceInfo.Add(new MetaDataField(key, nsn.ToBool()));
                                        }
                                        else if (nsn.isInteger())
                                        {
                                            ds.DeviceInfo.Add(new MetaDataField(key, nsn.ToLong()));
                                        }
                                        else if (nsn.isReal())
                                        {
                                            ds.DeviceInfo.Add(new MetaDataField(key, nsn.ToDouble()));
                                        }
                                    }
                                }
                            }
                        }
                    }

                    var lockd = fs.GetByPath("/private/Lockdown.plist");
                    if (lockd != null && lockd.Type==NodeType.File)
                    {
                        var plist = PlistHelper.ReadPlist<NSDictionary>(lockd);
                        if (plist != null)
                        {
                            var keys = Enum.GetNames(typeof(DeviceInfoKeys));
                            foreach (var key in keys)
                            {
                                if (plist.ContainsKey(key))
                                {
                                    var obj = plist.SafeGetObject<NSObject>(key);
                                    if (obj is NSString nss)
                                    {
                                        ds.DeviceInfo.Add(new MetaDataField(key, nss.Content));
                                    }
                                    else if (obj is NSNumber nsn)
                                    {
                                        if (nsn.isBoolean())
                                        {
                                            ds.DeviceInfo.Add(new MetaDataField(key, nsn.ToBool()));
                                        }
                                        else if (nsn.isInteger())
                                        {
                                            ds.DeviceInfo.Add(new MetaDataField(key, nsn.ToLong()));
                                        }
                                        else if (nsn.isReal())
                                        {
                                            ds.DeviceInfo.Add(new MetaDataField(key, nsn.ToDouble()));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    TraceService.TraceError(e);
                }
            }

            //返回空的pr,实际的数据已经存在ds.DeviceInfo里面了
            return pr;
        }
    }
}
