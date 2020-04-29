import csv
import spacy
import pytextrank
import multiprocessing as mp
import sys
import time


incolumn	= [	"ID", "Body", "Title", "Tags"]
outcolumn	= [	"ID", "KeyWords", "Tags"]

csvdataIn = "../../cleanpost.csv"
csvdataOut = "../../kwtag.csv"

def extract_keywords(text):
	nlp = spacy.load("en_core_web_sm")
	tr = pytextrank.TextRank()
	nlp.add_pipe(tr.PipelineComponent, name="textrank", last=True)

	doc = nlp(text)
	p = ''
	if len(doc._.phrases) > 0:
		p = doc._.phrases[0]
		p = str(p)
	return p

def ranker(queue, row):
	out = []
	out.append(row[incolumn[0]])
	keywords = extract_keywords(row[incolumn[1]])
	out.append(keywords)
	out.append(row[incolumn[3]])
	queue.put(out)
	return out

def writer(queue):
	with open(csvdataOut, "w+", encoding='utf-8') as csvOut:
		writer = csv.DictWriter(csvOut, fieldnames=outcolumn)
		writer.writeheader()
		while 1:
			time.sleep(0.002)
			row = queue.get()
			if 'done' in row:
				break
			writer.writerow({
						outcolumn[0]: row[0],
						outcolumn[1]: row[1],
						outcolumn[2]: row[2],
					})
		csvOut.close()

if __name__ == "__main__":
	with open(csvdataIn, "r+", encoding='utf-8') as csvIn:
		reader = csv.DictReader(csvIn)
		manager = mp.Manager()
		queue = manager.Queue()
		pool = mp.Pool(4)

		write_wait = pool.apply_async(writer, (queue,))
		print('run')
		job = []
		for row in reader:
			job.append(pool.apply_async(ranker, (queue, row)))

		for j in job:
			j.get()

		print('done')
		queue.put(['done','','',''])
		pool.close()
		csvIn.close()