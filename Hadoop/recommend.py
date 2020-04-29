from mrjob.job import MRJob
from datetime import datetime
import sys
import csv

RES_FILE = '../DemoResult/result'
TOP_ROW = ['0','','']

class SofRecommend(MRJob):

  def cosine_similarity(self, text1,text2):
    t1 = text1.split()
    t2 = text2.split()
    words = list(set(t1) | set(t2))
    v1 = []
    v2 = []
    for w in words:
      if w in t1:
        v1.append(1)
      else:
        v1.append(0)
      if w in t2:
        v2.append(1)
      else:
        v2.append(0)
    c = 0
    for i in range(len(words)): 
      c+= v1[i]*v2[i] 
    cosine = c / float((sum(v1)*sum(v2))**0.5) 
    return cosine

  def mapper(self, _, line):
    row = line.split(',')
    try:
      postid = int(row[0])
    except:
      return
    cos_kw = 0
    cos_tag = 0
    if TOP_ROW[1] != '' and row[1] != '':
      cos_kw = self.cosine_similarity(row[1], TOP_ROW[1])
    if TOP_ROW[2]  != '' and row[2] != '':
      cos_tag = self.cosine_similarity(row[2], TOP_ROW[2])
    total_cos = cos_kw + cos_tag
    yield postid, (total_cos)

  def reducer(self, postid, cosine):
    for cos in cosine:
      if float(cos) >= 0.8 and float(cos) < 2.0:
        yield postid, cos

if __name__ == '__main__':
  if sys.argv[1].replace('hdfs:///','').replace('.csv','').replace('kw','').replace('../','').isnumeric():
    RES_FILE = RES_FILE[:-3]+'_'+str(int(sys.argv[1].replace('hdfs:///','').replace('.csv','').replace('kw','').replace('../','')))

  with open(RES_FILE, 'r', encoding='utf_16') as f:
    maxtfidf = 0
    maxid = ''
    for line in f:
      k = line.split('\t')
      if maxtfidf < float(k[1]):
        maxtfidf = float(k[1])
        maxid = k[0][2:-1].split(',')[0]
        
  with open(sys.argv[1].replace('hdfs:///',''), 'r', encoding='utf-8') as kw:
    r = csv.DictReader(kw)
    for row in r:
      if row['ID'] == maxid:
        TOP_ROW = [row['ID'], row['KeyWords'], row['Tags']]
        break

  start_time = datetime.now()
  SofRecommend.run()
  end_time = datetime.now()
  elapsed_time = end_time - start_time
  sys.stderr.write(str(elapsed_time))