#coding=utf-8
'''
Created on 2014-2-20

@author: yuzhang
'''
import MySQLdb as mdb
import re, json
import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

con = mdb.connect('localhost', 'yumao', 'yumao8899', 'wikidb', charset='utf8');
cur = con.cursor(mdb.cursors.DictCursor)

# f = open('disambWords.txt', 'w')
# 
# cur.execute("SELECT id FROM Page WHERE isDisambiguation = 1")
# rows = cur.fetchall()
# ids = []
# for row in rows:
#     ids.append(row['id'])
# f.write(json.dumps(ids))
# 
# f.close()

f = open('disambWords.txt')

ids = json.loads(f.read())
l = len(ids)
for i in range(0, l):
    ident = ids[i]
    cur.execute("SELECT name, text FROM Page WHERE id = %s", [ident])
    page = cur.fetchone()
    print i, page['name'], ident
    disaText = page['text'].lower()
    disaNames = [name.split('|')[0].replace(' ', '_')
                 for name in 
                 re.findall(r'\*.*?\[\[(.*?)\]\]', disaText)]
    disaIDs = []
    if page['name'].endswith(u'_(消歧义)'):
        on = page['name'][0:-6]
        cur.execute("SELECT pageID FROM PageMapLine WHERE name LIKE %s ORDER BY id", [on])
        row = cur.fetchone()
        if row:
            disaIDs.append(row['pageID'])
        
    for dn in disaNames:
        cur.execute("SELECT pageID FROM PageMapLine WHERE name LIKE %s ORDER BY id", [dn])
        row1 = cur.fetchone()
        if row1:
            disaIDs.append(row1['pageID'])
    disaIDs = list(set(disaIDs))
#     for dID in disaIDs:
#         res = cur.execute("INSERT INTO page_candidates VALUES (%s, %s)", [ident, dID])
    cur.execute("INSERT INTO page_candidates2 VALUES (%s, %s)", [ident, json.dumps(disaIDs)])
        

f.close()


con.commit()
print 'commit'
   
cur.close()
con.close()