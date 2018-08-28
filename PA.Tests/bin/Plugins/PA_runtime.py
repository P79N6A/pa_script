#coding=utf-8
import clr
clr.AddReference('PA.Common')
clr.AddReference('System.Xml')
clr.AddReference('PA.InfraLib')
clr.AddReference('PA.InfraLib.Exts')
clr.AddReference('PA.InfraLib.Plugins')
clr.AddReference('PA.Formats')
clr.AddReference('PA.Formats.Exts')
clr.AddReference('PA.Engine')
clr.AddReference('SQLiteSpy')
del clr

import re
import sys,traceback
import time
import struct
import traceback
import SQLiteParser

from System import *
from System.IO import *
from System.Xml import *
from System.Text import *
from collections import defaultdict, Sequence, namedtuple
from System.Convert import IsDBNull
from PA.Formats.KTree import KNodeTools,KNode,KType
from PA.Formats.Exts import ParserHelperTools
from PA.Formats.XMLParserHelper import XMLParserTools
from PA.Common.Logger import TraceService
from PA.Common.Logger import TraceLevel

from PA.Formats import *
from PA.Formats.Apple import *
from PA.Formats.Apple.BPListTypes import *
from PA.Formats.Protobuf import *
from PA.InfraLib import *
from PA.InfraLib.Data import SourceEvent

from PA.InfraLib.Data import *
from PA.InfraLib.Files import *
from PA.InfraLib.Streams import *
from PA.InfraLib.Models import *
from PA.InfraLib.Models.Calls import *
from PA.InfraLib.Models.Common import *
from PA.InfraLib.Models.Generic import *
from PA.InfraLib.Models.Apps import *
from PA.InfraLib.Models.Locations import *
from PA.InfraLib.Models.Contacts import *
import PA.InfraLib.Models.Common.User
from PA.InfraLib.Models.Generic import *
from PA.Common.Utilities.Types import *
from PA.Common.Utilities.Data import Endianity,EndianityReverse
from PA.Formats.Apple import *
from PA.Engine import *
from PA.Engine.Python import *

try:
    from PA_sys import *
except:
    pass

chatTimes = namedtuple("chatTimes", "start end")

epoch = TimeStampFormats.GetTimeStampEpoch1Jan2001(0)
TextNotNull = SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
IntNotNull = SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Int, SQLiteParser.FieldConstraints.NotNull)
BlobNotNull = SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Blob, SQLiteParser.FieldConstraints.NotNull)

def LinkModels(a,b):
    a.AddJumpTarget(b)
    b.AddJumpTarget(a)

def to_byte_array(s):
    return Encoding.UTF8.GetBytes(s)

def update_app_model(parser_results, installed_apps, app_id):
    if parser_results is not None and (len(parser_results.Models) > 0) and app_id in installed_apps:
        app = installed_apps[app_id]
        app.DecodingStatus.Value = DecodingStatus.Decoded
        for model in parser_results.Models:
            model.AddJumpTarget(app)
            app.AddJumpTarget(model)

def update_name_tag(name, identifier, installed_apps):
    if identifier is None:
        return name
    if identifier in installed_apps:
        app = installed_apps[identifier]
        if app.Version.Value:
            return "{0}_{1}".format(name, app.Version.Value)
    return name

def create_apps_dictionary(ds):
    apps = {}
    for app in ds.Models[InstalledApplication]:
        apps[app.Identifier.Value] = app
    return apps

def CreateSourceEvent(node,model):
    SourceEvent.CreateSourceEvent(node,model)