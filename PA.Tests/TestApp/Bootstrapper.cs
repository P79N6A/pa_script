using TestApp.Views;
using System.Windows;
using Prism.Modularity;
using Microsoft.Practices.Unity;
using Prism.Unity;
using PA.InfraLib.Data;
using PA.InfraLib.Services.Python;
using PA.InfraLib.Services;
using System.IO;
using PA.InfraLib.Plugins;
using PA.InfraLib;
using System.Threading;
using System;
using PA.Common.Logger;
using PA.Engine;
using PA.Engine.Python;
using PA.iPhoneApps.Parsers;
using System.Collections.Generic;
using SqliteExp;

namespace TestApp
{
    class Bootstrapper : UnityBootstrapper
    {
        protected override DependencyObject CreateShell()
        {
            return Container.Resolve<MainWindow>();
        }

        protected override void InitializeShell()
        {
            Application.Current.MainWindow.Show();
        }

        internal static IParserProperties[] InstalAppNodes =
        {
            new CSharpParserProperties(new string[]{""},new ParserFactory<FullDumpAppsParser>(), "","Applications","应用列表",DescripCategories.Applications),
        };

        protected override void ConfigureServiceLocator()
        {
            base.ConfigureServiceLocator();

            PA.InfraLib.Services.Registor.RegAllServices(Container);
            PA.Logic.Services.Registor.RegAllServices(Container);

            byte[] key = new byte[] {0xb6, 0x96, 0xa6, 0x17, 0x65, 0xc9, 0xa4, 0x10, 0xd6, 0xcc, 0x5d, 0xdf, 0x3b, 0x51, 0x40, 0x2f, 0x78, 0x18, 0x28, 0x54, 0x99, 0xd9, 0x37, 0x02,
                  0x2f, 0x15, 0xd1, 0x13, 0x82, 0x34, 0xd3, 0x11, 0x17, 0xa3, 0x37, 0xcf, 0x0c, 0xe0, 0xe9, 0x74, 0xfe, 0xf2, 0xa2, 0x39, 0xd4, 0xf0, 0x8a, 0xf2 };
            string db = @"D:\中文\signal.sqlite";
            string outdb = @"D:\中文\designal.sqlite";
            var b = NativeMethod.ExportSqlCipherDatabase(db, outdb, key, 32);

            //这个路径改成你们电脑上的实际案例路径

            //新的测试用法
            //1.在Bin/Plugins目录下找到config.dat,修改里面的内容,把你的脚本的配置加进去(如果没有加的话)
            //2.如果只需要跑你指定的脚本,将Bin/Plugins的其他脚本移走,只留下你要测试的脚本和其依赖脚本
            //3.测试
            //4.
            //这个路径改成你们电脑上的实际案例路径,支持多镜像案例(比如安卓的全盘包括data.img和external_data.img)
            string casePath = @"E:\Cases\iPhone 5s_9.0.2_5012906926512_full\Manifest.PGFD";
            var pack = CasePackage.FromPath(casePath);
            if(pack!=null && pack.RpcClient.Connect())
            {
                var task = pack.LoadData();
                task.Wait();
                Console.WriteLine("程序走到这里,表明脚本跑完了,程序将会退出");
            }
            else
            {
                Console.WriteLine("案例配置不正确,换个案例");
            }
        }

        protected void OnModuleImported(PythonDataPlugin plugin,DataStore ds, ScriptWrapper sc)
        {
            try
            {
                //获取脚本方函数对象,函数的原型是请参考apple_apps.py里面的run
                var func = sc.GetScopeFunction<DataStore, bool, IDescriptiveProgress, CancellationToken, ParserResults>($"run");
                if (func != null)
                {
                    //运行此函数(建议在脚本里断点调试)
                    var results = func(ds, true, plugin.Progress, plugin.CancellationToken);
                }
            }
            catch (Exception e)
            {
                TraceService.Trace(TraceLevel.Error, e.Message);
            }
        }
    }
}
