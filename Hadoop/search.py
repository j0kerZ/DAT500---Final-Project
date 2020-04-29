from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from math import log
from datetime import datetime
import sys

NUM_DOC = 4220647

RE_WORD = re.compile(r"[\w']+")

class SofSearch(MRJob):

  def configure_args(self):
    super(SofSearch, self).configure_args()
    self.add_passthru_arg('--search', type=str, default='', help='Input search string')

  def load_args(self, args):
    super(SofSearch, self).load_args(args)
    self.search = RE_WORD.findall(self.options.search.lower())

  def steps(self):
    return[
      MRStep(mapper=self.mapper1,
             reducer=self.reducer1),
      MRStep(mapper=self.mapper2,
             reducer=self.reducer2),
      MRStep(mapper=self.mapper3,
             reducer=self.reducer3),
      MRStep(mapper=self.mapper4,
             reducer=self.reducer4),
    ]

  def mapper1(self, _, line):
    row = line.split(',')
    try:
      postid = int(row[0])
    except:
      return
    words = RE_WORD.findall(row[1]) + RE_WORD.findall(row[2])
    for word in words:
      if len(word) <15:
        yield (postid, row[3], word), 1

  def reducer1(self, post, count):
    yield (post[0], post[1]), (post[2],sum(count))

  def mapper2(self, post, word):
    yield (post[0], post[1]), (word[0], word[1])

  def reducer2(self, post, words):
    N = 0
    n = 0
    for word in words:
      N += word[1]
      if word[0] in self.search:
        n += word[1]
      yield word[0], (post[0], post[1], n, N)

  def mapper3(self, word, post):
    yield word, (post[0], post[1], post[2], post[3])

  def reducer3(self, word, posts):
    if word in self.search:
      d = 0
      ps = []
      for post in posts:
        d += 1
        ps.append(post)
      for post in ps:
        yield (post[0], post[1]), (post[2], post[3], d)

  def mapper4(self, post, tfidf):
    tf_idf = (float(tfidf[0]) / float(tfidf[1])) * log(NUM_DOC / tfidf[2])
    yield (post[0], post[1]), tf_idf

  def reducer4(self, post, tfidf):
    tf_idf = 0
    c = 0
    for t in tfidf:
      c += 1
      tf_idf += t
    tf_idf = (tf_idf / float(c))
    yield (post[0], post[1]), tf_idf

if __name__ == '__main__':
  if sys.argv[1].replace('hdfs:///','').replace('.csv','').isnumeric():
    NUM_DOC = int(sys.argv[1].replace('hdfs:///','').replace('.csv',''))

  start_time = datetime.now()
  SofSearch.run()
  end_time = datetime.now()
  elapsed_time = end_time - start_time
  sys.stderr.write(str(elapsed_time))
