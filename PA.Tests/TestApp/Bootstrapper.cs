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

            //这个路径改成你们电脑上的实际案例路径

            //新的测试用法
            //1.在Bin/Plugins目录下找到config.dat,修改里面的内容,把你的脚本的配置加进去(如果没有加的话)
            //2.如果只需要跑你指定的脚本,将Bin/Plugins的其他脚本移走,只留下你要测试的脚本和其依赖脚本
            //3.测试
            //4.
            //这个路径改成你们电脑上的实际案例路径,支持多镜像案例(比如安卓的全盘包括data.img和external_data.img)
            string casePath = @"E:\Cases\iPhone 7 plus_11.4.1_Mine\Manifest.PGFD";
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
