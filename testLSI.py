#coding=utf-8
'''
Created on 2014-2-17

@author: yuzhang
'''

from gensim import corpora, models, similarities
from stopWords import removeStopWordsJb
import logging
import jieba

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

documents = [u'苹果又叫滔婆，仁果类，由结实、多汁的果肉包着有几个种子的果核，与梨同属。商品型苹果原产于西亚或东欧，在世界范围内约有7500个品种，酸甜可口，营养丰富，是老幼皆宜的水果之一。根据其成熟期的早晚将其分为早、中、中晚、晚品种。',
             u'苹果公司（Apple Inc.，NASDAQ：AAPL，LSE：ACP），原称苹果电脑公司（Apple Computer, Inc.）总部位于美国加利福尼亚的库比提诺，核心业务是电子科技产品，目前全球电脑市场占有率为3.8%。苹果的Apple II于1970年代助长了个人电脑革命，其后的Macintosh接力于1980年代持续发展。最知名的产品是其出品的Apple II、Macintosh电脑、iPod数位音乐播放器、iTunes音乐商店和iPhone智能手机，它在高科技企业中以创新而闻名。苹果公司于2007年1月9日旧金山的Macworld Expo上宣布改名。',
             u'苹果/雪森苹果/雪森林檎/雪森柰子/雪森绫晴（ゆきもり りんご）《变身!偶像公主》三主角之一。充满了朝气的女孩子，粉红短发，瞳孔呈橙红色。能够变身成白雪公主的模样。喜欢的食物是苹果派。喜欢的偶像是WISH。家里有七个弟弟，被塞伊说是七个小矮人。']

texts = [removeStopWordsJb(jieba.cut(document.lower())) for document in documents]
dictionary = corpora.Dictionary(texts)
# print dictionary
# print dictionary.token2id

corpus = [dictionary.doc2bow(text) for text in texts]
# print corpus

tfidf = models.TfidfModel(corpus)

corpus_tfidf = tfidf[corpus]
# for doc in corpus_tfidf:
#     print doc

# print tfidf.dfs
# print tfidf.idfs

lsi = models.LsiModel(corpus_tfidf, id2word=dictionary, num_topics=3)
lsi.print_topics(3)
corpus_lsi = lsi[corpus_tfidf]
for doc in corpus_lsi:
    print doc
    
index = similarities.MatrixSimilarity(lsi[corpus])

# query = u'【苹果公司新零售店或位于无锡 向二线城市扩展】根据苹果公司中国官网的招聘信息显示，苹果或许将会在无锡开设一家新的Apple Store零售店，这意味着，苹果在中国正试图向二线城市拓展。目前苹果已经选定在重庆开两家零售店，位置分别在重庆解放碑国泰广场和观音桥北城天街。'
query = u'刚听到楼下有它喜欢的小狗叫，急的满屋子溜达。我穿衣服耽误点时间，到楼下人家已经回家了，白跑一趟......气的哼哼唧唧的回来了，给苹果也不吃。我把苹果放在它跟前，去洗脸，一分钟后回来，发现它吃了......屈辱的吃了......'
query_bow = dictionary.doc2bow(removeStopWordsJb(jieba.cut(query.lower())))
print query_bow

query_lsi = lsi[query_bow]
print query_lsi

sims = index[query_lsi]
# print list(enumerate(sims))
print sorted(enumerate(sims), key=lambda item: -item[1])