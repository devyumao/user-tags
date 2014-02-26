#coding=utf-8
'''
Created on 2014-2-16

@author: yuzhang
'''

import urllib2, json, re, os, math, time
import jieba.posseg as jbp
import jieba as jb
import logging
from stopWords import removeStopWordsJbg
import MySQLdb as mdb
from langconv import Converter

jb.enable_parallel()

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

weiboAppKey = '82966982'
tagsDict = {}
a = 1
b = 2
L = 2
cvt = Converter('zh-hans')

def setEleInDict(d, e, n):
    if e not in d:
        d[e] = n
    else:
        d[e] += n

def setTagInDict(d, i, t, n):
    if i not in d:
        d[i] = [t, n]
    else:
        d[i][1] += n

def getWordsFromWeibo(text):
    text = text.lower()
    userNameRE = re.compile(ur'@[\u4e00-\u9fa5|\w|\-|_]+')
    text = userNameRE.sub(' ', text)
    expressionRE = re.compile(r'\[.*?\]')
    text = expressionRE.sub(' ', text)
    shortUrlRE = re.compile(r'http://t.cn/\w+')
    text = shortUrlRE.sub(' ', text)
    return removeStopWordsJbg(jbp.cut(text))

def getWordsFromWiki(text):
    text = text.lower()
    htmlRE = re.compile(r'<.*?>')
    text = htmlRE.sub(' ', text)
    urlRE = re.compile(r'[a-z]+://[^\s]*')
    text = urlRE.sub(' ', text)
    return removeStopWordsJbg(jbp.cut(text))

def isWordWithMaxFlag(flag):
    return flag != 'x'

def isWordWithMidFlag(flag):
    return 'n' == flag[0] or flag in ['v', 'vn', 'a', 'an', 'eng']

def isWordWithMinFlag(flag):
    return 'n' == flag[0] or flag in ['vn', 'an', 'eng']

def collectTags(text):
    words = getWordsFromWeibo(text)
    wordTimesDict = {}
    wordsCon = [] 
    disaDict = {}
    ctxCat = []
    
    for w in words:
        if isWordWithMidFlag(w.flag):
            wordsCon.append(w.word)
            if len(w.word) > 1 and w.flag not in ['v', 'a']:
#                 print w.word, w.flag
                setEleInDict(wordTimesDict, w.word, 1)
                                    
    for (word, times) in wordTimesDict.items():
        cur.execute("SELECT pageID FROM PageMapLine WHERE name LIKE %s ORDER BY id", [word])
        row = cur.fetchone()
        if row:
            pageID = row['pageID']
            cur.execute("SELECT name, text, isDisambiguation FROM Page WHERE id = %s", [pageID])
            row1 = cur.fetchone()
            
            if 1 == ord(row1['isDisambiguation']):
                disaDict[pageID] = times
            else:
                cur.execute("SELECT pageID FROM PageMapLine WHERE name LIKE %s ORDER BY id", [word+u'_(消歧义)'])
                row2 = cur.fetchone()
                if row2:
                    cur.execute("SELECT isDisambiguation FROM Page WHERE id = %s", [row2['pageID']])
                    row3 = cur.fetchone()
                    if 1 == ord(row3['isDisambiguation']): 
                        disaDict[row2['pageID']] = times 
                    else:
                        crawlNormalPageCategories(row1['name'], pageID, times, ctxCat)
                else:
                    crawlNormalPageCategories(row1['name'], pageID, times, ctxCat)
    
    ctxSet = set(wordsCon).union(set(ctxCat))              
    for (dID, times) in disaDict.items():
        (name, rows) = disambiguate(dID, ctxSet)
        if name is not None:
            setEleInDict(tagsDict, name, times*a)
            crawlCategories(rows, L, times, 1)
        
def crawlNormalPageCategories(pageName, pageID, times, ctxCat):
    setEleInDict(tagsDict, pageName, times*a)
    cur.execute("SELECT pages AS catID FROM page_categories WHERE id = %s", [pageID])
    crawlCategories(cur.fetchall(), L, times, 1, ctxCat)

def disambiguate(disaID, ctxSet):
    print '【', disaID, '】'
    maxCon = -1
    rows = None
    name = None    
    cur.execute("SELECT candidates FROM page_candidates2 WHERE id = %s", [disaID])
    row = cur.fetchone()
    candidates = json.loads(row['candidates'])
    for cand in candidates:
        cur.execute("SELECT name, isDisambiguation, words FROM Page WHERE id = %s", [cand])
        row1 = cur.fetchone()
        if 0 == ord(row1['isDisambiguation']):
            wikiWordsSet = set([w[0] for w in json.loads(row1['words']) if isWordWithMidFlag(w[1])])             
            cur.execute("SELECT pages AS catID FROM page_categories WHERE id = %s", [cand])
            pcRows = cur.fetchall()
            wikiCats = []
            for pcRow in pcRows:
                catName = getCategoryName(pcRow['catID'])
                if catName is not None:
                    wikiCats.append(catName)
            wikiCtxSet= set(wikiCats).union(wikiWordsSet)
#             print '\t', cand, row1['name'], len(ctxSet.intersection(wikiCtxSet))
            con = len(ctxSet.intersection(wikiCtxSet))
            if con > maxCon:
                maxCon = con
                rows = pcRows
                name = row1['name']
    return (name, rows)

def getCategoryName(catID):
    cur.execute("SELECT name FROM Category WHERE id = %s", [catID])
    catName = cur.fetchone()['name']
    if re.compile(ur'.*[的]+.*').match(catName) or re.compile(ur'.*[\d]+.*').match(catName):
        return None
    else:
        return catName
             
def crawlCategories(rows, levels, times, depth, ctxCat=None):
    if 0 != levels:
        for row in rows:
            catID = row['catID']
            catName = getCategoryName(catID)           
            if catName is None:
                continue
            
            if 1 == depth and ctxCat is not None:
                    ctxCat.append(catName)
            
            stoptags = [u'分类', u'名词', u'动词', u'词汇', u'代词', u'条目', u'人名', u'借词', u'粗劣翻译', u'类别', u'汉语', u'小写标题']
            containsStoptag = False
            for st in stoptags:
                if catName.endswith(st):
                    containsStoptag = True
                    break
            for st in [u'中华人民共和国']:
                if catName.startswith(st):
                    containsStoptag = True
                    break
                
            if not containsStoptag:
                setEleInDict(tagsDict, catName, times/math.pow(b, depth))
                cur.execute("SELECT inLinks AS catID FROM category_inlinks WHERE id = %s", [catID])
                crawlCategories(cur.fetchall(), levels-1, times, depth+1)
    
 
res = urllib2.urlopen('https://api.weibo.com/2/statuses/user_timeline.json?source=' + weiboAppKey + '&uid=1990309453&count=100&trim_user=1')
data = json.loads(cvt.convert(res.read().decode('utf-8')).encode('utf-8'))
statuses = data['statuses']

con = mdb.connect('localhost', 'yumao', 'yumao8899', 'wikidb', charset='utf8');
cur = con.cursor(mdb.cursors.DictCursor)

start = time.clock()
for i in range(1, len(statuses)):
    status = statuses[i]
    text = ' '.join([status['text'], status['retweeted_status']['text']]) if status.has_key('retweeted_status') else status['text']
    print '(' + str(i) + ')', text
    collectTags(text)
#     print
print 'Timeout:', time.clock() - start

cur.close()
con.close()


# stoptags = [u'术语', u'名词', u'概念']
# for w in stoptags:
#     if tagsDict.has_key(w):
#         del tagsDict[w]
# listDel = []
# for (tag, weight) in tagsDict.items():
#     for st in stoptags:
#         if tag.endswith(st):
#             listDel.append((tag, weight))
#             break    
# for t in listDel:
#     setEleInDict(t[0][0:-2], tagsDict, t[1])
#     del tagsDict[t[0]]

stoptags = [u'术语', u'小作品']
for w in stoptags + [u'在世人物']:
    if tagsDict.has_key(w):
        del tagsDict[w]
listDel = []
for (tag, weight) in tagsDict.items():
    for st in stoptags:
        if tag.endswith(st):
            listDel.append([tag, weight, len(st)])
            break
for t in listDel:
    setEleInDict(tagsDict, t[0][0:-t[2]], t[1])
    del tagsDict[t[0]]
                      
for t in sorted(tagsDict.iteritems(), key=lambda d:d[1], reverse = True):
    if len(t[0]):
        print t[0], t[1]
print len(tagsDict)