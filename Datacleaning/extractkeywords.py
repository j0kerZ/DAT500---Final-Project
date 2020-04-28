import csv
import spacy
import pytextrank
import multiprocessing as mp


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

def worker(queue, text):
	keywords = extract_keywords(text)
	

def writer(queue):
	with open(csvdataOut, "w+", encoding='utf-8') as csvOut:
		writer = csv.DictWriter(csvOut, fieldnames=outcolumn)
		writer.writeheader()
		while 1:
			row = queue.get()
			
			writer.writerow({
						outcolumn[0]: row[incolumn[0]],
						outcolumn[1]: keywords,
						outcolumn[2]: row[incolumn[3]],
					})
		csvOut.close()

with open(csvdataIn, "r+", encoding='utf-8') as csvIn:
	reader = csv.DictReader(csvIn)
	for row in reader:
	csvIn.close()