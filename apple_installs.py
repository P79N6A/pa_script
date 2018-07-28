# coding=utf-8
import os
import PA_runtime
import re
import time
import inspect

def get_app_version(value, app, extractSource):
    if value.ContainsKey('CFBundleShortVersionString'):
        data = value['CFBundleShortVersionString']
        if data:
            app.Version.Value = data.Value
            if extractSource:
                app.Version.Source = MemoryRange(data.Source)
    elif value.ContainsKey('bundleShortVersionString'):
        data = value['bundleShortVersionString']
        if data:
            app.Version.Value = data.Value
            if extractSource:
                app.Version.Source = MemoryRange(data.Source)
    if not app.Version.Value and value.ContainsKey('CFBundleVersion'):
        data = value['CFBundleVersion']
        if data:
            val = data.Value
            len_val = len(val)
            if not '.' in val and len_val == 3 or len_val == 4:
                app.Version.Value = '.'.join([val[:-2], val[-2], val[-1]])
            else:
                app.Version.Value = val
            if extractSource:
                app.Version.Source = MemoryRange(data.Source)


def analyze_installation(f, extractDeleted, extractSource):
    results = []
    paths = defaultdict(list)
    groupPaths = defaultdict(list)
    if f.Data is None:
        return
    try:
        p = BPReader(f.Data).top
    except:
        p = None
    if p != None:
        for dictName in ['System', 'User']:
            if dictName not in p.Keys:
                continue
            for key in p[dictName].Keys:
                value = p[dictName][key]
                res = InstalledApplication()
                res.Deleted = DeletedState.Intact
                res.DecodingStatus.Value = DecodingStatus.NotDecoded
                res.Identifier.Value = key
                get_app_version(value, res, extractSource)
                if 'CFBundleDisplayName' in value.Keys:
                    res.Name.Value = value['CFBundleDisplayName'].Value
                    if extractSource:
                        res.Name.Source = MemoryRange(
                            value['CFBundleDisplayName'].Source)
                elif 'CFBundleName' in value.Keys:
                    res.Name.Value = value['CFBundleName'].Value
                    if extractSource:
                        res.Name.Source = MemoryRange(
                            value['CFBundleName'].Source)
                if 'Path' in value.Keys:
                    pathValues = value['Path'].Value.split('/')
                    if res.Name.Value == None and len(pathValues) > 0:
                        res.Name.Value = pathValues[-1][:-4]
                        if extractSource:
                            res.Name.Source = MemoryRange(value['Path'].Source)
                    if len(pathValues) == 7:
                        res.AppGUID.Value = pathValues[-2]
                        if extractSource:
                            res.AppGUID.Source = MemoryRange(
                                value['Path'].Source)
                if 'GroupContainers' in value.Keys and value['GroupContainers'] is not None:
                    for groupKey in value['GroupContainers'].Keys:
                        groupPath = value['GroupContainers'][groupKey].Value
                        groupPaths[res.Identifier.Value].append(
                            groupPath[groupPath.find('Containers/'):])

                paths[res.Identifier.Value].append(
                    'Applications/{0}'.format(res.Identifier.Value))
                if res.AppGUID.Value:
                    paths[res.Identifier.Value].append(
                        'Applications/{0}'.format(res.AppGUID.Value))
                    if "DeviceKeys" in env:
                        env["DeviceKeys"].Instance.AddGuidToAppGuids(
                            res.Identifier.Value, res.AppGUID.Value)
                results.append(res)
    else:
        plist = PList()
        f.seek(0)
        try:
            p = plist.Parse(f.Data)
        except:
            p = None
        if p is None:
            return
        p = p[0]
        if p is not None:
            for dictName in ['System', 'User']:
                if dictName not in p:
                    continue
                for dic in p[dictName]:
                    res = InstalledApplication()
                    res.Deleted = DeletedState.Intact
                    res.DecodingStatus.Value = DecodingStatus.NotDecoded
                    res.Identifier.Value = dic.Key
                    value = dic.Value
                    get_app_version(value, res, extractSource)
                    if 'CFBundleDisplayName' in value:
                        res.Name.Value = value['CFBundleDisplayName'].Value
                        if extractSource:
                            res.Name.Source = MemoryRange(
                                value['CFBundleDisplayName'].Source)
                    elif 'CFBundleName' in value:
                        res.Name.Value = value['CFBundleName'].Value
                        if extractSource:
                            res.Name.Source = MemoryRange(
                                value['CFBundleName'].Source)
                    if 'Path' in value:
                        pathValues = value['Path'].Value.split('/')
                        if res.Name.Value == None and len(pathValues) > 0:
                            res.Name.Value = pathValues[-1][:-4]
                            if extractSource:
                                res.Name.Source = MemoryRange(
                                    value['Path'].Source)
                        if len(pathValues) == 7:
                            res.AppGUID.Value = pathValues[-2]
                            if extractSource:
                                res.AppGUID.Source = MemoryRange(
                                    value['Path'].Source)
                    if 'GroupContainers' in value:
                        for groupKey in value['GroupContainers']:
                            groupPath = value['GroupContainers'][groupKey].Value
                            groupPaths[res.Identifier.Value].append(
                                groupPath[groupPath.find('Containers/'):])
                    paths[res.Identifier.Value].append(
                        'Applications/{0}'.format(res.Identifier.Value))
                    if res.AppGUID.Value:
                        paths[res.Identifier.Value].append(
                            'Applications/{0}'.format(res.AppGUID.Value))
                    results.append(res)
    pr = ParserResults()
    pr.Models.AddRange(results)
    return pr, paths, groupPaths


def analyze_last_launch_services_map(f, extractDeleted, extractSource):
    results = []
    paths = defaultdict(list)
    groupPaths = defaultdict(list)

    if f.Data is None:
        return
    p = BPReader(f.Data).top
    if p != None:
        for dictName in ['System', 'User']:
            if dictName not in p.Keys:
                continue
            for key in p[dictName].Keys:
                value = p[dictName][key]
                res = InstalledApplication()
                res.Deleted = DeletedState.Intact
                res.DecodingStatus.Value = DecodingStatus.NotDecoded
                res.Identifier.Value = key
                if 'CFBundlIdentifier' in value.Keys:
                    res.Name.Value = value['CFBundlIdentifier'].Value
                    if extractSource:
                        res.Name.Source = MemoryRange(
                            value['CFBundlIdentifier'].Source)
                if 'Container' in value.Keys:
                    res.AppGUID.Value = list(
                        value['Container'].Value.split('/'))[-1]
                    if extractSource:
                        res.AppGUID.Source = MemoryRange(
                            value['Container'].Source)

                if 'GroupContainers' in value.Keys and value['GroupContainers'] is not None:
                    for groupKey in value['GroupContainers'].Keys:
                        groupPath = value['GroupContainers'][groupKey].Value
                        groupPaths[res.Identifier.Value].append(
                            groupPath[groupPath.find('Containers/'):])

                paths[res.Identifier.Value].append(
                    '(Application|Applications|AppsData)/{0}'.format(res.Identifier.Value))
                if res.AppGUID.Value:
                    paths[res.Identifier.Value].append(
                        '(Application|Applications|AppsData)/{0}'.format(res.AppGUID.Value))
                    if "DeviceKeys" in env:
                        env["DeviceKeys"].Instance.AddGuidToAppGuids(
                            res.Identifier.Value, res.AppGUID.Value)
                results.append(res)
    else:
        plist = PList()
        p = plist.Parse(f.Data)
        if p is None:
            return
        p = p[0]
        if p is not None:
            for dictName in ['System', 'User']:
                if dictName not in p:
                    continue
                for dic in p[dictName]:
                    res = InstalledApplication()
                    res.Deleted = DeletedState.Intact
                    res.DecodingStatus.Value = DecodingStatus.NotDecoded
                    res.Identifier.Value = dic.Key
                    value = dic.Value
                    get_app_version(value, res, extractSource)
                    if 'CFBundlIdentifier' in value:
                        res.Name.Value = value['CFBundlIdentifier'].Value
                        if extractSource:
                            res.Name.Source = MemoryRange(
                                value['CFBundlIdentifier'].Source)
                    if 'Container' in value.Keys:
                        res.AppGUID.Value = list(
                            value['Container'].Value.split('/'))[-1]
                        if extractSource:
                            res.AppGUID.Source = MemoryRange(
                                value['Container'].Source)
                    if 'GroupContainers' in value.Keys and value['GroupContainers'] is not None:
                        for groupKey in value['GroupContainers'].Keys:
                            groupPath = value['GroupContainers'][groupKey].Value
                            groupPaths[res.Identifier.Value].append(
                                groupPath[groupPath.find('Containers/'):])

                    paths[res.Identifier.Value].append(
                        '(Application|Applications|AppsData)/{0}'.format(res.Identifier.Value))
                    if res.AppGUID.Value:
                        paths[res.Identifier.Value].append(
                            '(Application|Applications|AppsData)/{0}'.format(res.AppGUID.Value))
                        if "DeviceKeys" in env:
                            env["DeviceKeys"].Instance.AddGuidToAppGuids(
                                res.Identifier.Value, res.AppGUID.Value)
                    results.append(res)

    pr = ParserResults()
    pr.Models.AddRange(results)
    return pr, paths, groupPaths


def analyze_metadata(f, extractDeleted, extractSource):
    f.Data.seek(0)
    p = BPReader(f.Data).top
    results = []
    pr = ParserResults()
    if p != None:
        if not 'softwareVersionBundleId' in p.Keys:
            return
        key = p['softwareVersionBundleId'].Value

        app = InstalledApplication()
        app.Deleted = DeletedState.Intact
        app.DecodingStatus.Value = DecodingStatus.NotDecoded
        app.Identifier.Value = key
        if p.ContainsKey('copyright'):
            app.Copyright.Value = p['copyright'].Value
            if extractSource:
                app.Copyright.Source = MemoryRange(p['copyright'].Source)

        if 'purchaseDate' in p.Keys:
            purchase_date = p['purchaseDate']
        elif 'com.apple.iTunesStore.downloadInfo' in p.Keys:
            purchase_date = p['com.apple.iTunesStore.downloadInfo']['purchaseDate']
        else:
            purchase_date = None
        if purchase_date is not None:
            purchase_date_value = purchase_date.Value
            if type(purchase_date) == BPAsciiString:
                try:
                    purchase_date_value = XmlConvert.ToDateTimeOffset(
                        purchase_date_value)
                except SystemError:
                    purchase_date_value = None
            if purchase_date_value is not None:
                app.PurchaseDate.Value = TimeStamp(purchase_date_value, True)
                if extractSource:
                    app.PurchaseDate.Source = MemoryRange(purchase_date.Source)

        if p.ContainsKey('bundleDisplayName'):
            app.Name.Value = p['bundleDisplayName'].Value
            if extractSource:
                app.Name.Source = MemoryRange(p['bundleDisplayName'].Source)
        get_app_version(p, app, extractSource)

        pr.Models.Add(app)
        return pr

    else:
        plist = PList()
        try:
            p = plist.Parse(f.Data)
        except:
            return
        if p is None:
            return
        p = p[0]

        if not 'softwareVersionBundleId' in p:
            return
        key = p['softwareVersionBundleId'].Value

        app = InstalledApplication()
        app.Deleted = DeletedState.Intact
        app.DecodingStatus.Value = DecodingStatus.NotDecoded
        app.Identifier.Value = key
        app.Copyright.Value = p['copyright'].Value
        if extractSource:
            app.Copyright.Source = MemoryRange(p['copyright'].Source)

        purchase_date = None
        if 'purchaseDate' in p:
            purchase_date = p['purchaseDate']
        elif 'com.apple.iTunesStore.downloadInfo' in p:
            purchase_date = p['com.apple.iTunesStore.downloadInfo']['purchaseDate']

        if isinstance(purchase_date.Value, str):
            try:
                purchase_date_value = XmlConvert.ToDateTimeOffset(
                    purchase_date.Value)
            except SystemError:
                purchase_date_value = None
            if purchase_date_value != None:
                app.PurchaseDate.Value = TimeStamp(purchase_date_value, True)
                if extractSource:
                    app.PurchaseDate.Source = MemoryRange(purchase_date.Source)
        pr.Models.Add(app)
        return pr


def analyze_uninstalled(f, extractDeleted, extractSource):
    results = []
    bp = BPReader(f.Data).top
    if bp is None:
        return
    for key in bp.Keys:
        app = InstalledApplication()
        app.Deleted = DeletedState.Deleted
        app.Identifier.Value = key
        app.DecodingStatus.Value = DecodingStatus.NotDecoded
        app.DeletedDate.Value = TimeStamp(bp[key].Value, True)
        if extractSource:
            app.DeletedDate.Source = MemoryRange(bp[key].Source)
        results.append(app)
    pr = ParserResults()
    pr.Models.AddRange(results)
    return pr


def analyze_safeharbor(node, extract_deleted, extract_source):
    """
    SafeHarbor 是固件升级过程中保留用户数据而引入的一种机制
    应用的数据保存在 Library/SafeHarbor folder, Library/MobileInstallation 关联了
    其中每个文件夹对应的应用名称,版本,以及原始路径
    """
    if node.Data is None:
        return
    bp = BPReader(node.Data).top
    if bp is None:
        return
    results = []
    paths = defaultdict(list)
    for app_name in bp.Keys:
        app = InstalledApplication()
        app.Deleted = DeletedState.Unknown
        app.DecodingStatus.Value = DecodingStatus.NotDecoded
        if 'CFBundleIdentifier' in bp[app_name].Keys:
            src = MemoryRange(
                bp[app_name]['CFBundleIdentifier'].Source) if extract_source else None
            app.Identifier.Init(bp[app_name]['CFBundleIdentifier'].Value, src)
        else:
            app.Identifier.Value = app_name
        get_app_version(bp[app_name], app, extract_source)
        if 'Path' in bp[app_name].Keys:
            src = MemoryRange(
                bp[app_name]['Path'].Source) if extract_source else None
            guid = bp[app_name]['Path'].Value.split('/')[-2]
            app.AppGUID.Init(guid, src)
        if 'Container' in bp[app_name].Keys:
            # 路径以 "/var" 开头, 由于我们是模糊匹配,这里可以去掉 '/var'
            paths[app.Identifier.Value].append(
                bp[app_name]['Container'].Value.lstrip('/var'))

    pr = ParserResults()
    pr.Models.AddRange(results)
    return pr, paths


def analyze_container_manager(node, extractDeleted, extractSource):
    results = ParserResults()
    paths = {}
    bp = BPReader.GetTree(node)
    if bp is None:
        return [], {}
    iApp = InstalledApplication()
    iApp.Deleted = DeletedState.Intact
    iApp.Identifier.Init(bp['MCMMetadataIdentifier'].Value, MemoryRange(
        bp['MCMMetadataIdentifier'].Source) if extractSource else None)
    app_id = iApp.Identifier.Value
    if (app_id == 'group.odnoklassniki.iphone'):
        app_id = 'ru.odnoklassniki.iphone'
        iApp.Identifier.Value = app_id
    elif (app_id.startswith('group.') and app_id != 'group.net.whatsapp.WhatsApp.shared'):
        app_id = app_id[len('group.'):]
        iApp.Identifier.Value = app_id

    if iApp.Identifier.Value not in paths:
        paths[iApp.Identifier.Value] = []
    group_container_ids = None

    if bp.ContainsKey('MCMMetadataContentClass') and bp['MCMMetadataContentClass'].Value == 2:
        iApp.AppGUID.Value = node.Parent.Name
        if bp.ContainsKey('MCMMetadataInfo') and bp['MCMMetadataInfo'].ContainsKey('com.apple.MobileInstallation.GroupContainerIDs'):
            a = list(bp['MCMMetadataInfo']
                        ['com.apple.MobileInstallation.GroupContainerIDs'])
            group_container_ids = []
            for i in a:
                group_container_ids.append(i.Value)

        paths[iApp.Identifier.Value].append(
            '(Application|Applications|AppsData)/{0}'.format(iApp.AppGUID.Value))
    elif bp.ContainsKey('MCMMetadataContentClass') and bp['MCMMetadataContentClass'].Value == 7:
        iApp.AppGUID.Value = node.Parent.Name
        paths[iApp.Identifier.Value].append(
            'Shared/AppGroup/{0}'.format(iApp.AppGUID.Value))
    results.Models.Add(iApp)
    if group_container_ids != None:
        return results, paths, group_container_ids
    else:
        return results, paths


def add_group_container_paths_from_mcm(app_groupPaths, ds):
    app_ids_to_group_ids = {}
    group_ids_to_paths = {}
    app_ids_to_group_paths = {}
    mcm_code_signing_info_node = None
    for fs in ds.FileSystems:
        if mcm_code_signing_info_node == None:
            mcm_code_signing_info_node = fs.GetByPath(
                "/private/var/root/Library/MobileContainerManager/mcm_code_signing_info.plist")
    if mcm_code_signing_info_node == None:
        return

    bp = BPReader.GetTree(mcm_code_signing_info_node)
    if bp is None:
        return None
    for app_knode in bp:
        app_id = app_knode.Key
        if app_knode['CodeSigningInfo'] is not None and \
                app_knode['CodeSigningInfo']['com.apple.MobileContainerManager.Entitlements'] is not None and \
                app_knode['CodeSigningInfo']['com.apple.MobileContainerManager.Entitlements']['com.apple.security.application-groups'] is not None:
            app_ids_to_group_ids[app_id] = []
            for group_id in app_knode['CodeSigningInfo']['com.apple.MobileContainerManager.Entitlements']['com.apple.security.application-groups'].Value:
                app_ids_to_group_ids[app_id].append(group_id.Value)

    container_folders = mcm_code_signing_info_node.FileSystem.GetByPath(
        r'/private/var/mobile/Containers/Shared/AppGroup')
    for container_folder in container_folders:
        mobile_container_manager_node = container_folder.GetByPath(
            '/.com.apple.mobile_container_manager.metadata.plist')
        if mobile_container_manager_node == None:
            continue
        mobile_container_manager_bplist = BPReader.GetTree(
            mobile_container_manager_node)
        if mobile_container_manager_bplist is None or mobile_container_manager_bplist['MCMMetadataIdentifier'] is None:
            continue
        group_id = mobile_container_manager_bplist['MCMMetadataIdentifier'].Value
        group_path = container_folder.AbsolutePath
        group_ids_to_paths[group_id] = group_path

    for app_id in app_ids_to_group_ids:
        group_ids = app_ids_to_group_ids[app_id]
        for group_id in group_ids:
            if group_id in group_ids_to_paths.keys():
                app_ids_to_group_paths[app_id] = group_ids_to_paths[group_id]
    for app in ds.Models[InstalledApplication]:
        app_id = app.Identifier.Value
        if app_id is not None and app.AppGUID.Value is not None and app_id in app_ids_to_group_paths:
            if app_id not in app_groupPaths:
                app_groupPaths[app_id] = set()
            app_groupPaths[app_id].add(app_ids_to_group_paths[app_id])


def analyze_installation_display_version(f, extractDeleted, extractSource, installedApps):
    result = {}
    main_table_name = 'software_update'
    data_column = 'store_item_data'
    app_id_path = 'bundleId'
    version_path = 'offers/0/version/display'
    name_path = 'name'
    updated_column = 'update_state'
    installed_apps = dict([(ia.Identifier.Value, ia) for ia in installedApps])

    ts = SQLiteParser.TableSignature(main_table_name)
    db = SQLiteParser.Database.FromNode(f)
    if not db:
        return
    if main_table_name not in db.Tables:
        return
    for rec in db.ReadTableRecords(ts, False):
        if data_column not in rec or updated_column not in rec:
            break
        # 只关注更新的app
        if rec[updated_column].Value != 1:
            continue
        try:
            json = JsonParser.JNode.Parse("".join(
                map(chr, rec[data_column].Value)).decode('utf-8'), rec[data_column].Source)
        except Exception, e:
            continue
        if not json[app_id_path] or not json[version_path]:
            continue
        app_id = json[app_id_path].Value
        version = json[version_path].Value, MemoryRange(
            json[version_path].Source) if extractSource else None
        name = json[name_path].Value, MemoryRange(
            json[name_path].Source) if extractSource else None
        # 覆盖不带'.'的版本号
        if app_id in installed_apps:
            if type(installed_apps[app_id].Version.Value) is str and '.' not in installed_apps[app_id].Version.Value:
                installed_apps[app_id].Version.Init(version[0], version[1])
            installed_apps[app_id].Name.Init(name[0], name[1])


# 扫描所有的应用信息
INSTALLED_APPS = [
    ("/Caches/com\.apple\.mobile\.installation\.plist$",
     analyze_installation, "InstalledApps"),
    ("/MobileInstallation/UninstalledApplications\.plist$",
     analyze_uninstalled, "UninstalledApps"),
    ("/Applications?/.*/iTunesMetadata.plist$", analyze_metadata, "MetaData"),
    ("/MobileInstallation/SafeHarbor.plist$", analyze_safeharbor, "SafeHarbor"),
    ("/\.com\.apple\.mobile_container_manager\.metadata\.plist$",
     analyze_container_manager, "ContainerManager"),
    ("/MobileInstallation/LastLaunchServicesMap\.plist$",
     analyze_last_launch_services_map, "LastLaunchServicesMap"),
    # # 保险起见,额外分析下 updates.sqlitedb
    ("/com\.apple\.itunesstored/updates\.sqlitedb$",
     analyze_installation_display_version, "InstalledAppsUpdates"),
]


def get_installed_apps(ds, extract_deleted, extract_source):
    """
    获取所有的应用信息
    """
    results = ParserResults()
    app_paths = defaultdict(set)
    app_groupPaths = defaultdict(set)
    app_to_group_containers = defaultdict(set)

    for fs in ds.FileSystems:
        if not fs.IsTopLevel:
            continue  # 如果不是顶级文件系统,不符合要求
        for pattern, func, name in INSTALLED_APPS:
            first_time = True
            for node in list(fs.Search(pattern)):
                if first_time:
                    TraceService.Trace(
                        TraceLevel.Info, "正在分析 {0}".format(name))
                    first_time = False
                try:
                    time_start = time.time()
                    TraceService.Trace(
                        TraceLevel.Debug, "正在分析: {0}     节点: {1}".format(name, node.AbsolutePath))
                    if len(inspect.getargspec(func).args) == 4:
                        node_results = func(
                            node, extract_deleted, extract_source, ds.Models[InstalledApplication])
                    else:
                        node_results = func(
                            node, extract_deleted, extract_source)
                    TraceService.Trace(TraceLevel.Debug, "分析完成: {0}    耗时: {1} 秒    节点: {2}".format(
                        name, time.time() - time_start, node.AbsolutePath))
                    if node_results is None:
                        continue
                    elif isinstance(node_results, ParserResults):
                        results += node_results
                    else:
                        # 也可能返回的是应用的路径字典
                        results += node_results[0]
                        node_paths = node_results[1]
                        if node_paths:
                            for app, paths in node_paths.iteritems():
                                app_paths[app] = app_paths[app].union(paths)

                        # groupPaths
                        if len(node_results) >= 3 and node_results[2] is not None:
                            node_groupPaths = node_results[2]
                            if isinstance(node_groupPaths, defaultdict):
                                for app, groupPaths in node_groupPaths.iteritems():
                                    app_groupPaths[app] = app_groupPaths[app].union(
                                        groupPaths)
                            elif isinstance(node_groupPaths, list):
                                app = node_results[0].Models[0].Identifier.Value
                                app_to_group_containers[app] = node_groupPaths

                except:
                    TraceService.Trace(
                        TraceLevel.Error, "处理文件:{0}过程中出现了错误".format(name))

    # Todo : 如果是备份文件系统,则不符合这种路径规则,需要额外处理

    return results, app_paths, app_groupPaths


def run(ds, extract_deleted, progress, canceller):
    """
    插件的主入口
    ds: DataStore : 对象实例,包含当前宿主进程中.Net代码方所有的数据模型和对象
    extract_deleted : 是否解析删除记录
    extract_source : 是否释放源数据(保留)
    progress : IDescriptiveProgress 进度反馈和控制
    canceller : 取消操作
    """
    results = ParserResults()
    installed_apps, app_paths, app_groupPaths = get_installed_apps(
        ds, extract_deleted, False)

    return results
