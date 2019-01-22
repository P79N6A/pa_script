import clr
try:
    clr.AddReference('javaobj')
except:
    pass
del clr

import struct
import javaobj
import time
import json
import re
import pdb

class tencent_struct:
    @staticmethod
    def format2(bbb):
        res = {}
        for obj in bbb:
            if(isinstance(obj, javaobj.JavaObject)):
                res[type(obj)] = {}
                for name in dir(obj):
                    if(re.match('__.*__', name)):
                        continue
                    res[type(obj)][name] = getattr(obj, name)
            else:
                res[type(obj)] = obj
        return res

    def __setVals__(self, data, off):
        if(data):
            self.__data = data
        if(self.__data):
            self.__size = len(self.__data)
        self.__off = off

    def getAbsStructMsgTextElement(self):
        data_type = self.__getInt()
        res = {}
        res['a'] = data_type
        if(data_type == 1):
            res['v'] = self.__getString()
        if(data_type >= 2):
            res['p'] = self.__getString()
            res['r'] = self.__getString()
            res['s'] = self.__getString()
            res['v'] = self.__getString()
            if(data_type >= 3):
                res['u'] = self.__getString()
                if(data_type >= 4):
                    res['w'] = self.__getString()
        return res
    def getStructMsgItemCover(self):
        data_type = self.__getInt()
        res = {}
        res['a'] = data_type
        res['p'] = self.__getString()
        if(data_type > 3):
            res['c'] = self.__getString()
            res['b'] = self.__getString()
        if(data_type > 5):
            res['j'] = self.__getString()
            res['k'] = self.__getString()
            res['l'] = self.__getString()
            res['e'] = self.__getInt()
            res['f'] = self.__getInt()
        if(data_type > 8):
            res['q'] = self.__getString()
        return res
    def getAbsStructMsgItem(self):
        res = {}
        data_type = self.__getInt()
        if(data_type <= 0 ):
            return None
        res['a'] = data_type
        res['b'] = self.__getString()
        res['c'] = self.__getString()
        res['d'] = self.__getString()
        res['e'] = self.__getString()
        res['f'] = self.__getString()
        res['g'] = self.__getString()
        res['i_c'] = self.__getInt()
        res['i_e'] = self.__getInt()
        if(data_type >= 2 ):
            res['i_f'] = self.__getInt()
        count = self.__getInt()
        res['List'] = self.getItemList(count)
        if(data_type >=3 ):
            res['h'] = self.__getString()
            if(data_type > 5):
                res['j'] = self.__getString()
                res['k'] = self.__getString()
                res['l'] = self.__getString()
                if(data_type > 8):
                    res['i'] = self.__getString()
                if(data_type > 9):
                    res['n'] = self.__getString()
                    res['o'] = self.__getString()
                    res['p'] = self.__getString()
                    res['m'] = self.__getString()
                    res['q'] = self.__getString()
        return res
    def getStructMsgItemHr(self):
        res = {}
        data_type = self.__getInt()
        res['a'] = data_type
        if(data_type > 4):
            res['bool'] = self.__getString()
        if(data_type >= 9):
            res['i_e'] = self.__getInt()
        return res
    def getStructMsgItemPAVideo(self):
        res = {}
        data_type = self.__getInt()
        res['a'] = data_type
        res['p'] = self.__getString()
        res['q'] = self.__getString()
        if(data_type > 5):
            res['r'] = self.__getString()
            res['j'] = self.__getString()
            res['k'] = self.__getString()
            res['l'] = self.__getString()
        return res
    def getStructMsgItemMore(self):
        res = {}
        data_type = self.__getInt()
        res['a'] = data_type
        res['p'] = self.__getString() 
        return res
    def getStructMsgItemVideo(self):
        res = {}
        data_type = self.__getInt()
        res['a'] = data_type
        res['p'] = self.__getString()
        res['q'] = self.__getString()
        res['e'] = self.__getString()
        if(data_type > 7):
            res['r'] = self.__getString()
            res['s'] = self.__getString()
            res['g'] = self.__getInt()
            res['h'] = self.__getInt()
            res['i'] = self.__getInt()
            res['j'] = self.__getInt()
            res['t'] = self.__getString()
            res['k'] = self.__getInt()
        if(data_type > 8):
            res['w'] = self.__getString()
        if(data_type > 9):
            res['D'] = self.__getString()
            res['d'] = self.__getString()
        if(data_type > 10):
            res['f'] = self.__getInt()
        return res
    def getStructMsgItemTimer(self):
        res = self.getAbsStructMsgTextElement()
        data_type = res['a']
        res['z'] = self.__getString()
        res['A'] = self.__getString()
        res['l_c'] = self.__getLong()
        res['g'] = self.__getInt()
        res['h'] = self.__getInt()
        res['l_d'] = self.__getLong()
        res['l_e'] = self.__getLong()
        res['b_d'] = self.__getBool()
        return res
    def getStructMsgItemVoteCover(self):
        return self.getStructMsgItemCover()
    def getStructMsgItemImage(self):
        res = {}
        data_type = self.__getInt()
        res['a'] = data_type
        res['p'] = self.__getString()
        res['q'] = self.__getString()
        res['r'] = self.__getString()
        res['c'] = self.__getLong()
        res['d'] = self.__getLong()
        res['e'] = self.__getLong()
        return res
    def getStructMsgItemProgress(self):
        res = {}
        data_type = self.__getInt()
        res['a'] = data_type
        res['e'] = self.__getInt()
        return res
    def getStructMsgItemVote(self):
        res = self.getAbsStructMsgTextElement()
        data_type = res['a']
        res['e'] = self.__getInt()
        res['f'] = self.__getInt()
        res['g'] = self.__getInt()
        for loop in range(res['g']):
            tmp = []
            for i in range(2):
                tmp.append(self.__getString())
            res['a'].append(tuple(tmp))
        return res
    def getStructMsgItemPrice(self):
        res = self.getAbsStructMsgTextElement()
        data_type = res['a']
        res['z'] = self.__getString()
        return res
    def getStructMsgItemButton(self):
        res = self.getAbsStructMsgTextElement()
        data_type = res['a']
        res['f_b'] = self.__getString()
        res['f_c'] = self.__getString()
        res['f_d'] = self.__getString()
        res['f_e'] = self.__getString()
        res['f_f'] = self.__getString()
        if(data_type > 5):
            res['f_j'] = self.__getString()
            res['f_k'] = self.__getString()
            res['f_l'] = self.__getString()
        return res
    def getStructMsgItemRemark(self):
        res = self.getAbsStructMsgTextElement()
        data_type = res['a']
        res['z'] = self.__getString()
        return res
    def getStructMsgItemPAAudio(self):
        res = {}
        data_type = self.__getInt()
        res['a'] = data_type
        res['p'] = self.__getString()
        res['e'] = self.__getInt()
        res['f'] = self.__getInt()
        res['q'] = self.__getString()
        res['r'] = self.__getString()
        res['g'] = self.__getInt()
        res['s'] = self.__getString()
        return res

    def getStructMsgItemTextButton(self):
        res = {}
        data_type = self.__getInt()
        res['a'] = data_type
        res['p'] = self.__getString()
        res['q'] = self.__getString()
        res['r'] = self.__getString()
        res['b'] = self.__getString()
        return res
    def getStructMsgItemContent(self):
        return self.getAbsStructMsgTextElement()

    def getStructMsgItemTips(self):
        return self.getAbsStructMsgTextElement()

    def getStructMsgItemLive(self):
        data_type = self.__getInt()
        res = {}
        res['a'] = data_type
        res['q'] = self.__getString()
        res['r'] = self.__getString()
        res['s'] = self.__getString()
        res['e'] = self.__getInt()
        res['f'] = self.__getInt()
        res['g'] = self.__getInt()
        res['t'] = self.__getString()
        res['p'] = self.__getString()
        res['u'] = self.__getString()
        return res
    def getStructMsgItemTr(self):
        data_type = self.__getInt()
        res = {}
        res['a'] = data_type
        res['g'] = self.__getInt()
        res['e'] = self.__getInt()
        res['f'] = self.__getInt()
        count = self.__getInt()
        res['s_b'] = self.__getString()
        res['array_a'] = []
        for loop in range(count):
            name = self.__getString()
            if(name == "td"):
                res['array_a'].append(self.getStructMsgItemTd())

        return res
    def getStructMsgItemTd(self):
        res = self.getAbsStructMsgTextElement()
        data_type = res['a']
        res['h'] = self.__getInt()
        res['f'] = self.__getInt()
        return res
    def getStructMsgItemAvatar(self):
        res['a'] = self.__getInt()
        res['p'] = self.__getString()
        res['q'] = self.__getString()
        return res
    def getStructMsgItemTag(self):
        data_type = self.__getInt()
        res = {}
        res['a'] = data_type
        res['r'] = self.__getString()
        res['t'] = self.__getString()
        res['u'] = self.__getString()
        res['p'] = self.__getString()
        res['q'] = self.__getString()
        res['f'] = self.__getInt()
        return res
    def getStructMsgItemTagList(self):
        data_type = self.__getInt()
        res = {}
        res['a'] = data_type
        count = self.__getInt()
        res['array_a'] = self.getItemList(count)
        return res
    def getStructMsgGroupElement(self):
        data_type = self.__getInt()
        res = {}
        res['a'] = data_type
        count = self.__getInt()
        res['array_a'] = self.getItemList(count)
        return res
    def getStructMsgGroupItemElement(self):
        data_type = self.__getInt()
        res = {}
        res['a'] = data_type
        res['b'] = self.__getString()
        res['c'] = self.__getString()
        res['d'] = self.__getString()
        res['e'] = self.__getString()
        res['f'] = self.__getString()
        res['s_b1'] = self.__getString()
        res['s_b2'] = self.__getString()
        res['l_c'] = self.__getLong()
        count = self.__getInt()
        res['array_a'] = self.getItemList(count)
        return res
    def getStructMsgItemType(self):
        res = {}
        res['a'] = self.__getInt()
        res['e'] = self.__getInt()
        return res
    def getStructMsgNotItem(self, name):
        res = None
        if(name == 'pavideo'):
            res= self.getStructMsgItemPAVideo()
        elif(name == 'video'):
            res =  self.getStructMsgItemVideo()
        elif(name == 'picture'):
            res = self.getStructMsgItemCover()
        elif(name == 'title'):
            res = self.getAbsStructMsgTextElement()
        elif(name == 'summary'):
            res = self.getAbsStructMsgTextElement()
        elif(name == 'timer'):
            res = self.getStructMsgItemTimer()
        elif(name == 'hr'):
            res = self.getStructMsgItemHr()
        elif(name == 'image'):
            res = self.getStructMsgItemImage()
        elif(name == 'more'):
            res = self.getStructMsgItemMore()
        elif(name == 'progress'):
            res = self.getStructMsgItemProgress()
        elif(name == 'checklist'):
            res = self.getStructMsgItemVote()
        elif(name == 'vote'):
            res = self.getStructMsgItemVoteCover()
        elif(name == 'price'):
            res = self.getStructMsgItemPrice()
        elif(name == 'button'):
            res = self.getStructMsgItemButton()
        elif(name == 'remark'):
            res = self.getStructMsgItemRemark()
        elif(name == 'paaudio'):
            res = self.getStructMsgItemPAAudio()
        elif(name == 'textButton'):
            res = self.getStructMsgItemTextButton()
        elif(name == 'content'):
            res = self.getStructMsgItemContent()
        elif(name == 'tips'):
            res = self.getStructMsgItemTips()
        elif(name == 'live'):
            res = self.getStructMsgItemLive()
        elif(name == 'tr'):
            res = self.getStructMsgItemTr()
        elif(name == 'td'):
            res = self.getStructMsgItemTd()
        elif(name == 'head'):
            res = self.getStructMsgItemAvatar()
        elif(name == 'tag'):
            res = self.getStructMsgItemTag()
        elif(name == 'taglist'):
            res = self.getStructMsgItemTagList()
        elif(name == 'group'):
            res = self.getStructMsgGroupElement()
        elif(name == 'groupitem'):
            res = self.getStructMsgGroupItemElement()
        elif(name == 'type'):
            res = self.getStructMsgItemType()
        else:
            pass
        return res
    def getShareData(self):
        res = {}
        res['version'] = self.__readChar()
        res['appInfoStatus'] = self.__readChar()
        res['imageUrlStatus'] = self.__readChar()
        res['shortUrlStatus'] = self.__readChar()
        res['status'] = self.__getInt()
        res['pkgName'] = self.__getString()
        res['sourceIconBig'] = self.__getString()
        return res
    def getItemList(self, count):
        res = []
        for loop  in range(count):
            name = self.__getString()
            if(name == 'item'):
                sparse_key = self.__getInt()
                if(sparse_key <= 27 and sparse_key >= 0):
                    res.append(self.getAbsStructMsgItem())
            elif(name in self.__qq_StructMsgElementFactory__):
                # self.__getInt()
                res.append(self.getStructMsgNotItem(name))
            else:
                pass
                self.__getInt()
        return res
    def getStructMsgForAudioShare(self):
        res = {}
        data_type = self.__getInt()
        res['mVersion'] = data_type
        if(data_type == 1):
            res['mMsgTemplateID'] = self.__getInt()
            res['mMsgAction'] = self.__getString()
            res['mMsgActionData'] = self.__getString()
            res['mMsg_A_ActionData'] = self.__getString()
            res['mMsg_I_ActionData'] = self.__getString()
            res['mMsgUrl'] = self.__getString()
            res['mMsgBrief'] = self.__getString()
            res['mContentLayout'] = self.__getInt()
            res['mContentCover'] = self.__getString()
            res['mContentSrc'] = self.__getString()
            res['mContentTitle'] = self.__getString()
            res['mContentSummary'] = self.__getString()
            res['mSourceAppid'] = self.__getLong()
            res['mSourceIcon'] = self.__getString()
            res['mSourceName'] = self.__getString()
            res['mSourceUrl'] = self.__getString()
            res['mSourceAction'] = self.__getString()
            res['mSourceActionData'] = self.__getString()
            res['mSource_A_ActionData'] = self.__getString()
            res['mSource_I_ActionData'] = self.__getString()
            res['fwFlag'] = self.__getInt()
        elif(data_type >= 2):
            res['mMsgTemplateID'] = self.__getInt()
            res['mMsgAction'] = self.__getString()
            res['mMsgActionData'] = self.__getString()
            res['mMsg_A_ActionData'] = self.__getString()
            res['mMsg_I_ActionData'] = self.__getString()
            res['mMsgUrl'] = self.__getString()
            res['mMsgBrief'] = self.__getString()
            res['mContentLayout'] = self.__getInt()
            res['mContentCover'] = self.__getString()
            res['mContentSrc'] = self.__getString()
            res['mContentTitle'] = self.__getString()
            res['mContentSummary'] = self.__getString()
            res['mSourceAppid'] = self.__getLong()
            res['mSourceIcon'] = self.__getString()
            res['mSourceName'] = self.__getString()
            res['mSourceUrl'] = self.__getString()
            res['mSourceAction'] = self.__getString()
            res['mSourceActionData'] = self.__getString()
            res['mSource_A_ActionData'] = self.__getString()
            res['mSource_I_ActionData'] = self.__getString()
            res['fwFlag'] = self.__getInt()
            res['mFlag'] = self.__getInt()
            res['mHasSource'] = self.__getBool()
            res['mCommentText'] = self.__getString()
            if(data_type >= 3):
                res['mCompatibleText'] = self.__getString()
                res['msgId'] = self.__getLong()
                res['mPromotionType'] = self.__getInt()
                res['mPromotionMsg'] = self.__getString()
                res['mPromotionMenus'] = self.__getString()
                res['mPromotioinMenuDestructiveIndex'] = self.__getInt()
            if(data_type >= 5):
                res['source_puin'] = self.__getString()
            if(data_type >= 7):
                res['adverSign'] = self.__getInt()
                res['adverKey'] = self.__getString()
                res['index'] = self.__getString()
                res['index_name'] = self.__getString()
                res['index_type'] = self.__getString()
            if(data_type >= 15):
                res['f_forwardType'] = self.__getInt()
                res['shareData'] = self.getShareData()
            if(data_type >= 16):
                pass
        return res
    def getStructMsgForGeneralShare(self):
        res = {}
        data_type = self.__getInt()
        res['mVersion'] = data_type
        if(data_type == 1):
            res['mMsgTemplateID'] = self.__getInt()
            res['mMsgAction'] = self.__getString()
            res['mMsgActionData'] = self.__getString()
            res['mMsg_A_ActionData'] = self.__getString()
            res['mMsg_I_ActionData'] = self.__getString()
            res['mMsgUrl'] = self.__getString()
            res['mMsgBrief'] = self.__getString()
            res['mContentLayout'] = self.__getInt()
            res['mContentCover'] = self.__getString()
            res['mContentTitle'] = self.__getString()
            res['mContentSummary'] = self.__getString()
            res['mSourceAppid'] = self.__getLong()
            res['mSourceIcon'] = self.__getString()
            res['mSourceName'] = self.__getString()
            res['mSourceUrl'] = self.__getString()
            res['mSourceAction'] = self.__getString()
            res['mSourceActionData'] = self.__getString()
            res['mSource_A_ActionData'] = self.__getString()
            res['mSource_I_ActionData'] = self.__getString()
            res['fwFlag'] = self.__getInt()
        elif(data_type >= 2):
            res['mMsgTemplateID'] = self.__getInt()
            res['mMsgAction'] = self.__getString()
            res['mMsgActionData'] = self.__getString()
            res['mMsg_A_ActionData'] = self.__getString()
            res['mMsg_I_ActionData'] = self.__getString()
            res['mMsgUrl'] = self.__getString()
            res['mMsgBrief'] = self.__getString()
            count = self.__getInt()
            res['mStructMsgItemLists'] = self.getItemList(count)
            res['mSourceAppid'] = self.__getLong()
            res['mSourceIcon'] = self.__getString()
            res['mSourceName'] = self.__getString()
            res['mSourceUrl'] = self.__getString()
            res['mSourceAction'] = self.__getString()
            res['mSourceActionData'] = self.__getString()
            res['mSource_A_ActionData'] = self.__getString()
            res['mSource_I_ActionData'] = self.__getString()
            res['fwFlag'] = self.__getInt()
            res['mFlag'] = self.__getInt()
            res['mResid'] = self.__getString()
            res['mFileName'] = self.__getString()
            res['mFileSize'] = self.__getLong()
            res['mCommentText'] = self.__getString()
            if(data_type >= 3):
                res['mCompatibleText'] = self.__getString()
                res['msgId'] = self.__getLong()
                res['mPromotionType'] = self.__getInt()
                res['mPromotionMsg'] = self.__getString()
                res['mPromotionMenus'] = self.__getString()
                res['mPromotionMenuDestructiveIndex'] = self.__getInt()
            if(data_type >= 4):
                res['dynamicMsgMergeKey'] = self.__getString()
                res['dynamicMsgMergeIndex'] = self.__getInt()
            if(data_type >= 5):
                res['source_puin'] = self.__getString()
            if(data_type >= 6):
                res['mSType'] = self.__getString()
            if(data_type >= 7):
                res['adverSign'] = self.__getInt()
                res['adverKey'] = self.__getString()
                res['index'] = self.__getString()
                res['index_name'] = self.__getString()
                res['index_type'] = self.__getString()
                res['flong_bid'] = self.__getLong()
                res['fString_pid'] = self.__getString()
                res['flong_pVersion'] = self.__getLong()
                res['fboolean_isFull'] = self.__getBool()
                res['flong_likeNum'] = self.__getLong()
                res['flong_commentNum'] = self.__getLong()
                res['fboolean_isLike'] = self.__getBool()
                res['fString_author'] = self.__getString()
            if(data_type >= 8):
                res['mArticleIds'] = self.__getString()
                res['mOrangeWord'] = self.__getString()
                res['mAlgorithmIds'] = self.__getString()
                res['mStrategyIds'] = self.__getString()
            if(data_type >= 9):
                res['mExtraData'] = self.__getString()
                res['mCreateTime'] = self.__getString()
                res['mTagName'] = self.__getString()
            if(data_type >= 10):
                res['fString_eventId'] = self.__getString()
                res['fString_remindBrief'] = self.__getString()
                res['fString_eventType'] = self.__getString()
            if(data_type >= 11):
                res['fString_tips'] = self.__getString()
            if(data_type >= 12):
                res['mInnerUniqIds'] = self.__getString()
            if(data_type >= 13):
                res['mSourceThirdName'] = self.__getString()
                res['mQQStoryExtra'] = self.__getString()
                res['mNeedRound'] = self.__getString()
            if(data_type >= 14):
                res['mQidianBulkTaskId'] = self.__getString()
                res['reportEventFolderStatusValue'] = self.__getString()
            if(data_type >= 15):
                res['f_forwardType'] = self.__getInt()
                res['f_shareData'] = self.getShareData()
            if(data_type >= 17):
                res['f_mAdSourceName'] = self.__getString()
                res['mCommonData'] = self.__getString()
        return res
    def getStructMsgForHypertext(self):
        res = {}
        data_type = self.__getInt()
        res['mVersion'] = data_type
        if(data_type == 1):
            res['mMsgTemplateID'] = self.__getInt()
            res['mMsgAction'] = self.__getString()
            res['mMsgActionData'] = self.__getString()
            res['mMsg_A_ActionData'] = self.__getString()
            res['mMsg_I_ActionData'] = self.__getString()
            res['mMsgUrl'] = self.__getString()
            res['mMsgBrief'] = self.__getString()
            count = self.__getInt()
            res['mHpertextArray'] = []
            for loop in range(count):
                tmp = []
                for i in range(6):
                    tmp.append(self.__getString())
                res['mHpertextArray'].append(tuple(tmp))

            res['fwFlag'] = self.__getInt()
            res['mSourceName'] = self.__getString()
            res['mSourceIcon'] = self.__getString()
            res['mSourceUrl'] = self.__getString()
            res['msgId'] = self.__getLong()
            res['mPromotionType'] = self.__getInt()
            res['mPromotionMsg'] = self.__getString()
            res['mPromotionMenus'] = self.__getString()
            res['mPromotioinMenuDestructiveIndex'] = self.__getInt()
           
    def getStructMsgForImageShare(self):
        res = {}
        data_type = self.__getInt()
        res['mVersion'] = data_type
        if(data_type == 1):
            res['mMsgTemplateID'] = self.__getInt()
            res['mMsgAction'] = self.__getString()
            res['mMsgActionData'] = self.__getString()
            res['mMsg_A_ActionData'] = self.__getString()
            res['mMsg_I_ActionData'] = self.__getString()
            res['mMsgUrl'] = self.__getString()
            res['mMsgBrief'] = self.__getString()
            res['mContentLayout'] = self.__getInt()
            res['mContentCover'] = self.__getString()
            res['mContentTitle'] = self.__getString()
            res['mContentSummary'] = self.__getString()
            res['mSourceAppid'] = self.__getLong()
            res['mSourceIcon'] = self.__getString()
            res['mSourceName'] = self.__getString()
            res['mSourceUrl'] = self.__getString()
            res['mSourceAction'] = self.__getString()
            res['mSourceActionData'] = self.__getString()
            res['mSource_A_ActionData'] = self.__getString()
            res['mSource_I_ActionData'] = self.__getString()
            res['fwFlag'] = self.__getInt()
        elif(data_type >= 2):
            res['mMsgTemplateID'] = self.__getInt()
            res['mMsgAction'] = self.__getString()
            res['mMsgActionData'] = self.__getString()
            res['mMsg_A_ActionData'] = self.__getString()
            res['mMsg_I_ActionData'] = self.__getString()
            res['mMsgUrl'] = self.__getString()
            res['mMsgBrief'] = self.__getString()
            count = self.__getInt()
            # pdb.set_trace()
            res['mStructMsgItemLists'] = self.getItemList(count)
            res['mSourceAppid'] = self.__getLong()
            res['mSourceIcon'] = self.__getString()
            res['mSourceName'] = self.__getString()
            res['mSourceUrl'] = self.__getString()
            res['mSourceAction'] = self.__getString()
            res['mSourceActionData'] = self.__getString()
            res['mSource_A_ActionData'] = self.__getString()
            res['mSource_I_ActionData'] = self.__getString()
            res['fwFlag'] = self.__getInt()
            res['mFlag'] = self.__getInt()
            res['mHasSource'] = self.__getBool()
            res['mCommentText'] = self.__getString()
            if(data_type >= 3):
                res['mCompatibleText'] = self.__getString()
                res['msgId'] = self.__getLong()
                res['mPromotionType'] = self.__getInt()
                res['mPromotionMsg'] = self.__getString()
                res['mPromotionMenus'] = self.__getString()
                res['mPromotionMenuDestructiveIndex'] = self.__getInt()
            if(data_type >= 5):
                res['source_puin'] = self.__getString()
            if(data_type >= 7):
                res['adverSign'] = self.__getInt()
                res['adverKey'] = self.__getString()
                res['index'] = self.__getString()
                res['index_name'] = self.__getString()
                res['index_type'] = self.__getString()
            if(data_type >= 16):
                res['thumbWidth'] = self.__getInt()
                res['thumbHeigth'] = self.__getInt()
                res['mImageBizType'] = self.__getInt()
        return res
    #about extstr
    #waiting for patch
    def getQQMessage(self, msgtype, data = None, off = 0, extStr = False):
        self.__setVals__(data, off)
        try:

            if(msgtype == -2018 or msgtype == -2050):
                return self.readStruct('__qq_StructMsg__')

            elif(msgtype == -2011 or msgtype == -2054 or msgtype == -2059):
                res = javaobj.load_all(self.__data)
                self.__data = b''
                for r in res:
                    self.__data += r
                data_type = self.__getInt()
                
                if(data_type == 2):
                    return self.getStructMsgForAudioShare()
                elif(data_type == 3 or data_type == 82):
                    return self.getStructMsgForHypertext()
                elif(data_type == 5):
                    return self.getStructMsgForImageShare()
                else:
                    return self.getStructMsgForGeneralShare()
            
            elif(msgtype == -5003):
                return (self.readStruct('__qq_MsgBody__'))
            elif(msgtype == -1000):
                if(extStr == True):
                    return (self.readStruct('__qq_FoldMsg__'))
                else:
                    # print (self.__data)
                    return self.__data
            elif(msgtype == -3006):
                obj = javaobj.load_all(self.__data)
                return self.format2(obj)
            
            elif(msgtype == -5040 or msgtype == -5020 or msgtype == -5021 or msgtype == -5022 or msgtype == -5023):
                return self.readStruct('__qq_UniteGrayTipMsg__')
            elif(msgtype == -1034):
                return json.loads(self.__data.decode())
            elif(msgtype == -1035):
                return (self.readStruct(self.__qq_Msg__))
            elif(msgtype == -5008 or msgtype == -2007):
                res = None
                obj = (javaobj.load_all(self.__data))
                if(msgtype == -2007):
                    obj = self.format2(obj)
                    res = {}
                    for i in obj:
                        for j in obj[i]:
                            res[j] = obj[i][j]
                elif(msgtype == -5008):
                    res = {}
                    for i in obj:
                        for j in obj[i]:
                            res[j] = obj[i][j]
                return res

            elif(msgtype == -2000):
                obj = (self.readStruct(self.__qq_PicRec__))
                res = {}
                for i in obj:
                    res[obj[i][0]] = obj[i][1]
                return res
            elif(msgtype == -2006):
                return self.__data.decode()
            elif(msgtype == -2022):
                return (self.readStruct(self.__qq_VideoFile__))
            elif(msgtype == -2053):
                try:
                    obj = (javaobj.load_all(self.__data))
                    return format(obj)
                except:
                    return(self.__data.decode())
            elif(msgtype == -1049):
                try:
                    return (self.__data.decode())
                except:
                    return self.__data
            elif(msgtype == -2025):
                res = javaobj.load_all(self.__data)
                self.__data = b''
                obj = None
                for r in res:
                    if(isinstance(r, bytes)):
                        self.__data += r
                    else:
                        obj = r
                return (self.getMessageForQQWalletMsg())
            elif(msgtype == -2038):
                res = javaobj.load_all(self.__data)
                self.__data = b''
                for r in res:
                    self.__data += r
                return(self.getMessageForTroopGift())
                # pri)
            else:
                #print(msgtype)
                #print(self.__data)
                return None
        except:
            raise
            return None

    def getMessageForTroopGift(self):
        res = {}
        res['animationPackageId'] = self.__getInt()
        res['remindBrief'] = self.__getString()
        res['animationBrief'] = self.__getString()
        res['gitfCount'] = self.__getInt()
        res['senderUin'] = self.__getLong()
        res['receiveUin'] = self.__getLong()
        res['title'] = self.__getString()
        res['subtitle'] = self.__getString()
        res['message'] = self.__getString()
        res['giftPicId'] = self.__getInt()
        res['comefrom'] = self.__getString()
        res['exflag'] = self.__getInt()
        res['isReported'] = self.__getBool()
        res['summary'] = self.__getString()
        res['jumpUrl'] = self.__getString()
        res['isFromNearby'] = self.__getBool()
        res['sendScore'] = self.__getInt()
        res['recvScore'] = self.__getInt()
        res['charmHeroism'] = self.__getString()
        res['btFlag'] = self.__getInt()
        res['objColor'] = self.__getInt()
        res['senderName'] = self.__getString()
        try:
            res['version'] = self.__getInt()
            if(res['version'] >= 1):
                res['bagId'] = self.__getString()
        except:
            pass
        return res
    def getQQWalletTransferMsg(self, data_type):
        res = {}
        res['elem_background'] = self.__getInt()
        res['elem_icon'] = self.__getInt()
        res['elem_title'] = self.__getString()
        res['elem_subTitle'] = self.__getString()
        res['elem_content'] = self.__getString()
        res['elem_linkUrl'] = self.__getString()
        res['elem_blackStripe'] = self.__getString()
        res['elem_notice'] = self.__getString()
        res['channelId'] = self.__getInt()
        res['templateId'] = self.__getInt()
        res['resend'] = self.__getInt()
        if(data_type == 1):
            pass
        elif(data_type == 2):
            res['elem_titleColor'] = self.__getInt()
            res['elem_subtitleColor'] = self.__getInt()
            res['elem_actionsPriority'] = self.__getString()
            res['elem_jumpUrl'] = self.__getString()
            res['elem_nativeAndroid'] = self.__getString()

        elif(data_type == 3):
            res['elem_titleColor'] = self.__getInt()
            res['elem_subtitleColor'] = self.__getInt()
            res['elem_actionsPriority'] = self.__getString()
            res['elem_jumpUrl'] = self.__getString()
            res['elem_nativeAndroid'] = self.__getString()
            res['elem_iconUrl'] = self.__getString()
            res['elem_contentColor'] = self.__getInt()
            res['elem_contentBgColor'] = self.__getInt()
            res['elem_aioImageLeft'] = self.__getString()
            res['elem_aioImageRight'] = self.__getString()
            res['elem_cftImage'] = self.__getString()
        return res

    def getQQWalletRedPacketMsg(self):
        res = {}
        res['elem_background'] = self.__getInt()
        res['elem_icon'] = self.__getInt()
        res['elem_title'] = self.__getString()
        res['elem_subTitle'] = self.__getString()
        res['elem_content'] = self.__getString()
        res['elem_linkUrl'] = self.__getString()
        res['elem_blackStripe'] = self.__getString()
        res['elem_notice'] = self.__getString()
        res['channelId'] = self.__getInt()
        res['templateId'] = self.__getInt()
        res['resend'] = self.__getInt()
        res['redtype'] = self.__getInt()
        res['redPackedId'] = self.__getString()
        res['authkey'] = self.__getString()
        res['isOpened'] = self.__getBool()
        res['elem_titleColor'] = self.__getInt()
        res['elem_subtitleColor'] = self.__getInt()
        res['elem_actionsPriority'] = self.__getString()
        res['elem_jumpUrl'] = self.__getString()
        res['elem_nativeAndroid'] = self.__getString()

        try:
            res['elem_iconUrl'] = self.__getString()
            res['elem_contentColor'] = self.__getInt()
            res['elem_contentBgColor'] = self.__getInt()
            res['elem_aioImageLeft'] = self.__getString()
            res['elem_aioImageRight'] = self.__getString()
            res['elem_cftImage'] = self.__getString()
            res['envelopeid'] = self.__getInt()
            res['envelopeName'] = self.__getString()
            res['conftype'] = self.__getInt()
            res['msgFrom'] = self.__getInt()
            res['redPacketIndex'] = self.__getString()
            res['redChannel'] = self.__getInt()
        except:            
            pass
        return res
    def getMessageForQQWalletMsg(self):
        res = {}
        version = self.__getInt()
        if(version == 1 or version == 2):
            data_type = self.__getInt()
            if(data_type == 1):
                return self.getQQWalletTransferMsg(data_type)
        elif(version == 17):
            return self.getQQWalletRedPacketMsg()
        elif(version >= 32):
            data_type = self.__getInt()
            data_type2 = self.__getInt()
            data_type3 = self.__getInt()
            if(data_type == 1):
                return self.getQQWalletTransferMsg(data_type2)
            elif(data_type == 2):
                return self.getQQWalletRedPacketMsg()
        return None

    def getSystemStructMsg(self, data = None, off = 0):
        self.__setVals__(data, off)
        try:
            return self.readStruct('__qq_StructMsg__')
        except:
            raise
        pass

    def getStructMsg(self, data = None, off = 0):
        self.__setVals__(data, off)
        # stream = javaobj.loadStream(data)
        return None

        

    def getSnsAttrBuf(self, data = None, off = 0):
        self.__setVals__(data, off)
        try:
            return self.readStruct('__bfy__')
        except:
            raise

    def getSnsContent(self, data = None, off = 0):
        self.__setVals__(data, off)
        try:
            return self.readStruct('__bjs__')
        except:
            raise
    
    def getRcontactLvbuff(self, data = None, off = 0):
        self.__setVals__(data, off)
        res = {}
        self.__add()
        count = 0
        for i in self.__lvbuff__:
            count = count + 1
            res[count] = self.__lvbufftype__[i](self)
            if(self.__data[self.__off] == '}' or self.__off + 1 >= self.__size):
                break
        return res
    def __getString(self):
        try:
            length = self.__getShort()
            res = self.__data[self.__off : self.__off + length]
            try:
                res = res.decode()
            except:
                pass
            self.__add(length)
        except:
            raise
        return res

    def __getShort(self):
        try:
            res = self.__readChar() * 0x100 + self.__readChar()
        except:
            raise
        return res

    def __getBool(self):
        try:
            res = (self.__readChar() != 0)
        except:
            raise
        return res
    def __getLong(self):
        try:
            res = 0
            i = 8
            while i:
                i = i - 1
                res = res << 8
                res += self.__readChar()
        except:
            raise
        return res

    def __getInt(self):
        try:
            res = self.__readChar() * 0x1000000 + self.__readChar() * 0x10000 + self.__readChar() * 0x100 + self.__readChar()
        except:
            raise
        return res


    def readStruct(self, struct_type):
        current_dict = None
        if(isinstance(struct_type, str)):
            current_dict = getattr(self, struct_type)
        else:
            current_dict = struct_type
        res = {}
        try:
            while(self.__off < self.__size):
                key = self.__readUleb()
                key = key >> 3
                if(key == 0):
                    break
                op = None
                fieldName = ''
                if(key in current_dict):
                    op = current_dict[key][1]
                    fieldName = current_dict[key][0]
                else:
                    break
                if(isinstance(op, dict)):
                    if(not key in res):
                        res[key] = []
                    current_struct = self.__readData()
                    recursion = tencent_struct(current_struct)
                    res[key].append((fieldName, recursion.readStruct(op)))
                elif(op != ''):

                    res[key] = (fieldName, self.__contenttype__[op](self))
                else:
                    break
        except:
            raise
        return res

    def getStruct(self, struct_type):
        pass

    def __readString(self):
        try:
            length = self.__readUleb()
            res = self.__data[self.__off : self.__off + length]
            self.__add(length)
        except:
            raise
        return res.decode('utf-8')

    def __readUleb(self):
        try:
            i = ord(self.__data[self.__off])
            self.__add()
            if(i & 0x80):
                j = ord(self.__data[self.__off])
                i = i & 0x7f
                i = i | (j << 7)
                self.__add()
                if(i & 0x4000):
                    j = ord(self.__data[self.__off])
                    i = i & 0x3fff
                    i = i | (j << 14)
                    self.__add()
                    if(i & 0x200000):
                        j = ord(self.__data[self.__off])
                        i = i & 0x1fffff
                        i = i | (j << 21)
                        self.__add()
                        if(i & 0x10000000):
                            j = ord(self.__data[self.__off])
                            i = i & 0xfffffff
                            i = i | (j << 28)
                            self.__add()
            return i
        except :
            raise


    def __readFloat(self):
        try:
            f = struct.unpack('f', self.__data[self.__off : self.__off + 4])
            self.__add(4)
            return f[0]
        except:
            raise

    def __readData(self):
        try:
            length = self.__readUleb()
            data = self.__data[self.__off : self.__off + length]
            self.__add(length)
            return data
        except:
            raise
        return None


    def __readChar(self):
        c = None
        try:
            c = ord(self.__data[self.__off])
            self.__add()
        except:
            raise
        return c

    def __readBool(self):
        try:
            return self.__readUleb() != 0
        except:
            raise
        return None
    def __readUlong(self):
        i = 0
        j = 0
        l = 0
        while True:
            assert(i < 64)
            try:
                j = self.__readChar()
            except:
                raise
            l = l | (j & 0x7f) << i
            if((j & 0x80) == 0):
                return l
            i = i + 7
    def __readSSleb(self):
        res = self.__readUleb()
        return res

    def __readSSSleb(self):
        res = self.__readUleb()
        return ((res >> 1) ^ (-(res & 1)))

    def __readSSlong(self):
        res = self.__readUlong()
        return res
    def __readSSSlong(self):
        res = self.__readUlong()
        return ((res >> 1) ^ (-(res & 1)))

    def __init__(self, data = None, off = 0):
        self.__data = data
        self.__off = off
        if(self.__data):
            self.__size = len(self.__data)
        else:
            self.__size = 0

    def __add(self, value = 1):
        self.__off += value
        if(self.__off > self.__size):
            raise(self.__tencentException("array bound"))
   



    class __tencentException(Exception):
        def __init__(self,err):
            Exception.__init__(self, err)
   

    __cp__ = {
        1 : ('', 'I'),
        2 : ('', 'I'),

    }

    __cw__ = {
        1 : ('', 's'),
        2 : ('', 's'),
    }
    __cv__  = {
        1 : ('', 's'),
        2 : ('', 's'),
        3 : ('', 's'),
    } 
    __ar__ = {
        1 : ('', 's'),
        2 : ('', 's'),
        3 : ('', 's'),
        4 : ('', 's'),
    }
    __aqt__ = {
        1 : ('', 'f'),
        2 : ('', 'f'),
        3 : ('', 'f'),
    }
    __aqr__ = {
        1 : ('', 's'),
        2 : ('', 'I'),
        3 : ('', 's'),
        4 : ('', 's'),
        5 : ('', 'I'),
        6 : ('', 's'),
        7 : ('', 'I'),
        8 : ('', 'I'),
        9 : ('', 's'),
        10 : ('', __aqt__),
        11 : ('', 's'),
        12 : ('', 'I'),
        13 : ('', 'I'),
        14 : ('', 'I'),
        15 : ('', 's'),   
        16 : ('', 'I'),
        17 : ('', 's'),   
        18 : ('', 's'),   
        19 : ('', 's'),   
        20 : ('', 's'),  
        21 : ('', 'I'),
        22 : ('', 's'),   
        23 : ('', 's'), 
        25 : ('', 'I'),
        26 : ('', 'L'),
        27 : ('', 's'),
        28 : ('', 's'),
        29 : ('', 'I'),
        30 : ('', 's'),
        31 : ('', 's'),
        32 : ('', 'I'),
        33 : ('', 's'),
        34 : ('', 's'),
        35 : ('', 'b'),

    }
    __amq__ = {  # location
        1 : ('', 'f'),  # longitude
        2 : ('', 'f'),  # latitude
        3 : ('', 's'),
        4 : ('', 's'),
        5 : ('', 's'),
        6 : ('', 's'),
        7 : ('', 'I'),
        8 : ('', 's'),
        9 : ('', 'I'),
        10 : ('', 'I'),
        11 : ('', 'I'),
        12 : ('', 'f'),
        13 : ('', 'P'),
        14 : ('', 'I'),
        15 : ('', 's'),
        16 : ('', 's'),

    }

    __cr__ = {
        1 : ('', 's'),
        2 : ('', 's'),
        3 : ('', 's'),
        4 : ('', 's'),
        5 : ('', 's'),
        6 : ('', 'I'),
    }

    __od__ = {
        1 : ('', 's'),    
        2 : ('', 'I'),    
        3 : ('', 's'),    
        4 : ('', 's'),    
        5 : ('', __aqr__),    
        6 : ('', 'I'),    
        7 : ('', 's'),    

    }

    __as__ = {
        1 : ('', 'I'),    
        2 : ('', 's'),    
        3 : ('', 'I'),    
        4 : ('', 's'),    
        5 : ('', 's'),    
        6 : ('', 's'),    
        7 : ('', __ar__),
        8 : ('', 's'),    
        9 : ('', __cp__),
        10 : ('', __cw__),
        11 : ('', __cv__),
        12 : ('', __cv__),





    }
    __bih__ = {
        1 : ('', 's'),    
        2 : ('', 's'),    
    }
    __bsl__ = {
        1 : ('', 's'),    
        2 : ('', 's'),    
    }
    __bzu__ = {
        1 : ('', 's'),
        2 : ('', 's'),
        3 : ('', 's'),
        4 : ('', 's'),
        5 : ('', 's'),
        6 : ('', 's'),
        7 : ('', 's'),
        8 : ('', 's'),
    }
    __bjs__ = {  # wechat sns feed
        1 : ('', 's'),
        2 : ('', 's'),
        3 : ('', 'I'),
        4 : ('', 'I'),
        5 : ('', 's'), 
        6 : ('', __amq__),
        7 : ('', __cr__),
        8 : ('', __od__),
        9 : ('', 's'), 
        10 : ('', 's'),
        11 : ('', 's'),
        12 : ('', 'I'),
        13 : ('', 'I'),
        14 : ('', 's'),
        15 : ('', __as__),
        16 : ('', 'I'),
        17 : ('', __bih__),
        18 : ('', 's'),
        19 : ('', 's'),
        20 : ('', __bsl__),
        21 : ('', 'I'),
        22 : ('', __bzu__),
    }


    
    __bae__ = {
        1 : ('', 's'),
    }
    __ew__ = {
        1 : ('', 'I'),
        2 : ('', __bae__),
    }
    __bft__ = {
        1 : ('', 'L'),
    }
    __bjn__ = {
        1 : ('', __ew__),
    }
    __auu__ = {
        1 : ('', 'I'),
        2 : ('', 'I'), 
        3 : ('', 's'),
    }
    __bfj__ = {
        1 : ('', 's'),
        2 : ('', 'I'),
        3 : ('', 's'),
        4 : ('', 's'),
        5 : ('', 'I'),
        6 : ('', 'I'),
    }
    __bfn__ = {
        1 : ('', 's'),
        2 : ('', 's'),
        3 : ('', 'I'),
        4 : ('', 'I'),
        5 : ('', 's'),
        6 : ('', 'I'),
        7 : ('', 'I'),
        8 : ('', 'I'),
        9 : ('', 's'),
        10 : ('', 'I'),
        11 : ('', 'L'),
        12 : ('', 'L'),
        13 : ('', 'I'),
        14 : ('', 'I'),
    }
    __bad__ = {
        1 : ('', 'I'),
        2 : ('', 'P'),
    }
    __bfy__ = {
        1 : ('', 'L'),
        2 : ('', 's'),
        3 : ('', 's'),
        4 : ('', 'I'),
        5 : ('', __bad__),
        6 : ('', 'I'),
        7 : ('', 'I'),
        8 : ('', 'I'),
        9 : ('', __bfn__),
        10 : ('', 'I'),
        11 : ('', 'I'),
        12 : ('', __bfn__),
        13 : ('', 'I'),
        14 : ('', 'I'),
        15 : ('', __bfn__),
        16 : ('', 'I'),
        17 : ('', 'I'),
        18 : ('', 'I'),
        19 : ('', __bft__),
        20 : ('', 'I'),
        21 : ('', 's'),
        22 : ('', 'L'),
        23 : ('', 'I'),
        24 : ('', __bae__),
        25 : ('', 'I'),
        26 : ('', 'I'),
        27 : ('', __bae__),
        28 : ('', __bad__),
        29 : ('', __bjn__),
        30 : ('', __auu__),
        31 : ('', __bfj__),
    }
    __qq_AddFrdSNInfo__ = {
        1 : ('uint32_not_see_dynamic', 'I'),
        2 : ('uint32_set_sn', 'I'),
    }
    __qq_SystemMsgActionInfo__ = {
        1 : ('type', 'I'),
        2 : ('group_code', 'L'),
        3 : ('sig', 'P'),
        50 : ('msg', 's'),
        51 : ('group_id', 'I'),
        52 : ('remark', 's'), 
        53 : ('blacklist', 'b'),
        54 : ('addFrdSNInfo', __qq_AddFrdSNInfo__),
    }
    __qq_SystemMsgAction__ = {
        1 : ('name', 's'),
        2 : ('result', 's'),
        3 : ('action', 'I'),
        4 : ('action_info', __qq_SystemMsgActionInfo__),
        5 : ('detail_name', 's'),
    }
    __qq_FriendInfo__ = {
        1 : ('msg_joint_friend', 's'),
        2 : ('msg_blacklist', 's'),
    }
    __qq_GroupInfo__ = {
        1 : ('group_auth_type', 'I'),
        2 : ('display_action', 'I'),
        3 : ('msg_alert', 's'),
        4 : ('msg_detail_alert', 's'),
        5 : ('msg_other_admin_done', 's'),
        6 : ('uint32_app_privilege_flag', 'I'),
    }
    __qq_MsgInviteExt__ = {
        1 : ('uint32_src_type', 'I'),
        2 : ('uint64_src_code', 'L'),
        3 : ('uint32_wait_state', 'I'),
    }
    __qq_MsgPayGroupExt__ = {
        1 : ('uint64_join_grp_time', 'L'),
        2 : ('uint64_quit_grp_time', 'L'),
    }
    
    __qq_SystemMsg__ = {
        1 : ('sub_type', 'I'),
        2 : ('msg_title', 's'),
        3 : ('msg_describe', 's'),
        4 : ('msg_additional', 's'),
        5 : ('msg_source', 's'),
        6 : ('msg_decided', 's'),
        7 : ('src_id', 'I'),
        8 : ('sub_src_id', 'I'),
        9 : ('actions', __qq_SystemMsgAction__),
        10 : ('group_code', 'L'),
        11 : ('action_uin', 'L'),
        12 : ('group_msg_type', 'I'),
        13 : ('group_inviter_role', 'I'),
        14 : ('friend_info', __qq_FriendInfo__),
        15 : ('group_info', __qq_GroupInfo__),
        16 : ('actor_uin', 'L'),
        17 : ('msg_actor_describe', 's'),
        18 : ('msg_additional_list', 's'),
        19 : ('relation', 'I'),
        20 : ('reqsubtype', 'I'),
        21 : ('clone_uin', 'L'),
        22 : ('uint64_discuss_uin', 'L'),
        23 : ('uint64_eim_group_id', 'L'),
        24 : ('msg_invite_extinfo', __qq_MsgInviteExt__),
        25 : ('msg_pay_group_extinfo', __qq_MsgPayGroupExt__),
        50 : ('req_uin_faceid', 'i'),
        51 : ('req_uin_nick', 's'),
        52 : ('group_name', 's'),
        53 : ('action_uin_nick', 's'),
        54 : ('msg_qna', 's'),
        55 : ('msg_detail', 's'),
        57 : ('group_ext_flag', 'I'),
        58 : ('actor_uin_nick', 's'),
        59 : ('pic_url', 'P'),
        60 : ('clone_uin_nick', 's'),
        61 : ('req_uin_business_card', 'P'),
        63 : ('eim_group_id_name', 'P'),
        64 : ('req_uin_pre_remark', 'P'),
        65 : ('action_uin_qq_nick', 'P'),
        66 : ('action_uin_remark', 'P'),
        67 : ('req_uin_gender', 'I'),
        68 : ('req_uin_age', 'I'),
        101 : ('card_switch', 'I'),


    }
    
    __qq_StructMsg__ = {
        1 : ('version', 'I'),
        2 : ('msg_type', 'I'),
        3 : ('msg_seq', 'L'),
        4 : ('msg_time', 'L'),
        5 : ('req_uin', 'L'),
        6 : ('uint32_unread_flag', 'I'),
        50 : ('msg', __qq_SystemMsg__),
    }
    __qq_OneGeoGraphicFriend__ = {
        1 : ('uint64_uin', 'L')
    }
    __qq_GeoGraphicNotify__ = {
        1 : ('bytes_local_city', 'P'),
        2 : ('rpt_msg_one_friend', __qq_OneGeoGraphicFriend__),
    }
    __qq_OneBirthdayFriend__ = {
        1 : ('uint64_uin', 'L'),
        2 : ('bool_lunar_birty', 'b'),
        3 : ('uint32_birth_mounth', 'I'),
        4 : ('uint32_birth_date', 'I'),
        5 : ('uint64_msg_send_time', 'I'),
        6 : ('uint32_birth_year', 'I'),
    }
    __qq_BirthdayNotify__ = {
        1 : ('rpt_msg_one_friend', __qq_OneBirthdayFriend__),
    }
    __qq_OneMemorialDayInfo__ = {
        1 : ('uint64_uin', 'L'),
        2 : ('uint32_type', 'I'),
        3 : ('uint32_memorial_time', 'I'),
        11 : ('bytes_main_wording_nick', 'P'),
        12 : ('bytes_main_wording_event', 'P'),
        13 : ('bytes_sub_wording', 'P'),
        14 : ('bytes_greetings', 'P'),
        15 : ('uint32_friend_gender', 'I'),
    }
    __qq_MemorialDayNotify__ = {
        1 : ('rpt_anniversary_info', __qq_OneMemorialDayInfo__),
    }
    __qq_MsgBody__ = {
        1 : ('uint32_msg_type', 'I'),
        2 : ('bool_strong_notify', 'b'),
        3 : ('uint32_push_time', 'I'),
        4 : ('msg_geographic_notify', __qq_GeoGraphicNotify__),
        5 : ('msg_birthday_notify', __qq_BirthdayNotify__),
        6 : ('bytes_notify_wording', 'P'),
        7 : ('msg_memorialday_notify', __qq_MemorialDayNotify__),
    }
    

    __qq_FoldMsg__ = {
        1 : ('fold_flags', 'I'),
        2 : ('redbag_sender_uin', 'L'),
        3 : ('redbag_id', 'P'),
        4 : ('msg_content', 'P'),
        5 : ('redbag_index', 'P')
    }
    __qq_HightlightParam__ = {
        1 : ('start', 'I'),
        2 : ('end', 'I'),
        3 : ('uin', 'L'),
        4 : ('needUpdateNick', 'I'),
        5 : ('actionType', 'I'),
        6 : ('icon', 's'),
        7 : ('textColor', 'I'),
        8 : ('mMsgActionData', 's'),
        9 : ('mMsg_A_ActionData', 's'),
    }
    __qq_BusinessData__ = {
        1 : ('haveRead', 'I'),
        2 : ('subType', 'I'),
        3 : ('masterUin', 's'),
        4 : ('extUin', 's'),
        5 : ('taskId', 's'),
    }
    __qq_UniteGrayTipMsg__ = {
        1 : ('graytip_id', 'I'),
        2 : ('graytip_level', 'I'),
        3 : ('graytip_mutex_id', 'I'),
        4 : ('graytip_key', 's'),
        5 : ('content', 's'),
        6 : ('business_related', 'P'),
        7 : ('hightlight_item', __qq_HightlightParam__),
        8 : ('isLocalTroopMsg', 'I'),
        9 : ('business_data', __qq_BusinessData__),
    }

    __qq_ForwardExtra__ = {
        1 : ('foward_orgId','I'),
        2 : ('foward_orgUin','s'),
        3 : ('foward_orgUinType','i'),
        4 : ('foward_orgUrl','s'),
        5 : ('foward_thumbPath','s'),
        6 : ('foward_orgFileSizeType','i'),
    }

    __qq_PicRec__ = {
        1 : ('localPath','s'),
        2 : ('size','L'),
        3 : ('type','I'),
        4 : ('isRead','b'),
        5 : ('uuid','s'),
        6 : ('md5','s'),
        7 : ('serverStorageSource','s'),
        8 : ('thumbMsgUrl','s'),
        9 : ('bigMsgUrl','s'),
        10 : ('rawMsgUrl','s'),
        11 : ('fileSizeFlag','i'),
        12 : ('uiOperatorFlag','i'),
        13 : ('fowardInfo',__qq_ForwardExtra__),
        15 : ('version','i'),
        16 : ('isReport','i'),
        17 : ('groupFileID','L'),
        18 : ('localUUID','s'),
        19 : ('preDownState','i'),
        20 : ('preDownNetwork','i'),
        21 : ('previewed','i'),
        22 : ('uint32_thumb_width','I'),
        23 : ('uint32_thumb_height','I'),
        24 : ('uint32_width','I'),
        25 : ('uint32_height','I'),
        26 : ('uint32_image_type','I'),
        27 : ('uint32_show_len','I'),
        28 : ('uint32_download_len','I'),
        29 : ('uint32_current_len','I'),
        30 : ('notPredownloadReason','I'),
        31 : ('enableEnc','b'),
        32 : ('bigthumbMsgUrl','s'),
        33 : ('bytes_pb_reserved','P'),
    }
    __qq_VideoFile__ = {
        1 : ('bytes_file_uuid', 'P'),
        2 : ('bytes_file_md5', 'P'),
        3 : ('bytes_file_name', 'P'),
        4 : ('uint32_file_format', 'I'),
        5 : ('uint32_file_time', 'I'),
        6 : ('uint32_file_size', 'I'),
        7 : ('uint32_thumb_width', 'I'),
        8 : ('uint32_thumb_height', 'I'),
        9 : ('uint32_file_status', 'I'),
        10 : ('uint32_file_progress', 'I'),
        11 : ('uint32_file_type', 'I'),
        12 : ('bytes_thumb_file_md5', 'P'),
        13 : ('bytes_source', 'P'),
        14 : ('file_lastmodified', 'L'),
        15 : ('uint32_thumb_file_size', 'I'),
        16 : ('uint32_busi_type', 'I'),
        17 : ('uin32_from_chat_type', 'I'),
        18 : ('uin32_to_chat_type', 'I'),
        19 : ('uin32_uiOperatorFlag', 'I'),
        20 : ('bytes_video_file_source_dir', 'P'),
        21 : ('bool_support_progressive', 'b'),
        22 : ('uint32_file_width', 'I'),
        23 : ('uint32_file_height', 'I'),
        24 : ('bytes_local_file_md5', 'P'),
        25 : ('uint32_is_local_video', 'I'),
        26 : ('uint32_transfered_size', 'I'),
        27 : ('uint32_sub_busi_type', 'I'),
        28 : ('uint32_video_attr', 'I'),
        29 : ('uint32_video_binary_set', 'I'),
        30 : ('bool_is_mediacodec_encode', 'b'),
    }
    __qq_Elem__ = {
        1 : ('textMsg', 's'),
        2 : ('picMsg', __qq_PicRec__),
        3 : ('markfaceMsg', ''),
        4 : ('sourceMsgInfo', 's'),
    }
    __qq_Msg__ = {
        1 : ('elems', __qq_Elem__),
    }
    __contenttype__ = {
        's' : __readString,
        'i' : __readSSleb,
        'I' : __readUleb,
        'f' : __readFloat,
        'P' : __readData,
        'l' : __readSSlong,
        'L' : __readUlong,
        'b' : __readBool,
    }


    __lvbuff__ = ('I', 'I', 's', 'L', 'I', 's', 's', 'I', 'I', 's', 's', 'I', 'I', 's', 's', 's', 's', 'I', 'I', 's', 'I', 's', 's', 'I', 'I', 's', 's', 's', 's', 's', 's', 's', 's', 'I')
    __lvbufftype__ = {
        's' : __getString,
        'I' : __getInt,
        'L' : __getLong,
    }
    __qq_StructMsgElementFactory__ = ('pavideo', 'video', 'picture', 'title', 'summary', 'timer', 'hr', 'image', 'more', 'progress', 'checklist', 'vote', 'price', 'button', 'remark', 'paaudio', 'textButton', 'content', 'tips', 'live', 'tr', 'td', 'head', 'tag', 'taglist', 'group', 'groupitem', 'type')

 