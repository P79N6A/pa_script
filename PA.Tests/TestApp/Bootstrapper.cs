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

        protected override void ConfigureServiceLocator()
        {
            base.ConfigureServiceLocator();

            PA.InfraLib.Services.Registor.RegAllServices(Container);
            PA.Logic.Services.Registor.RegAllServices(Container);

            //这个路径改成你们电脑上的实际案例路径
            string casePath = @"E:\Cases\iPhone 6_11.1.2_133217541373990_full\Manifest.pnfa"; 
            var pack = CasePackage.FromPath(casePath);
            if(pack!=null)
            {
                
                var ds = pack.DataStore; //案例的DataStore对象
                var progress = pack.Progress; //案例所关联的进度指示上下文
                var appService = ServiceGetter.Get<IApplicationService>();

                var tarFile = Path.Combine(pack.ProjectDir.FullName, pack.Info.ImageFile);
                var mntService = ServiceGetter.Get<IMountService>();
                var fsMnt = mntService.MountTarFile(tarFile);
                if (fsMnt != null)
                {
                    ds.FileSystems.Add(fsMnt);

                    //指定脚本所在的路径,apple_apps.py可以换成你需要测试的脚本
                    var scriptPath = Path.Combine(appService.RunPath, "Plugins", "apple_apps.py");

                    PythonDataPlugin dataPlugin = new PythonDataPlugin(scriptPath, true, OnModuleImported);
                    dataPlugin.Init(ds, progress, CancellationToken.None);
                    dataPlugin.Run();
                }
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
