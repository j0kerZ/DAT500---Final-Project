
def cosine_similarity(text1,text2):
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

