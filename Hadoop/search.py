
  def mapper(self, _, line):
    
    yield (1, 2, 3), None
  def reducer(self, x, y):
    yield x, y
