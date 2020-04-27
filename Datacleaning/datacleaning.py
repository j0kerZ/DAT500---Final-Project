import csv
from nltk.corpus import stopwords
import re
from bs4 import BeautifulSoup
stop_words = set(stopwords.words('english'))

csvdataIn = "../../postsonly.csv"
csvdataOut = "../../cleanpost.csv"

outcolumn	= [	"ID", "Body", "Title", "Tags"]

re_url = 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'

def remove_htmltags(text): 
    compile1 = re.compile('<.*?>')
    compile2 = re.compile('&.*;')
    text = re.sub(compile1, '', text)
    text = re.sub(compile2, ' ', text)
    return text.lower()

def remove_chars(t): 
    sub = re.sub(r'[?|!|"|#|:|=|+|_|{|}|[|]|-|$|%|^|&|]',r'',t)
    clean = re.sub(r'[.|,|)|(|\|/|-|~|`|>|<|*|$|@|;|→]',r'',sub)
    return  clean

def de_contract(phrase):
    phrase = re.sub(r"won't", "will not", phrase)
    phrase = re.sub(r"can\'t", "can not", phrase)
    phrase = re.sub(r"n\'t", " not", phrase)
    phrase = re.sub(r"\'s", " is", phrase)
    phrase = re.sub(r"\'t", " not", phrase)
    phrase = re.sub(r"\'ve", " have", phrase)
    phrase = re.sub(r"\'m", " am", phrase)
    return phrase

def preprocessing_text(tt):
    string = ""
    for w in tt.split():
        word = ("".join(e for e in w if e.isalnum()))
        if not word in stop_words:
            string += word + " "
    return string

def preprocess_post(post):
    post = remove_htmltags(post)
    post = re.sub(re_url, '', post)
    post = remove_chars(post)
    post = de_contract(post)
    post = preprocessing_text(post)
    return post

def clean_tags(t):
    t = t.replace('<', ' ')
    t = t.replace('>', ' ')
    return t

with open(csvdataIn, "r+", encoding='utf-8') as csvIn:
	reader = csv.DictReader(csvIn)
	with open(csvdataOut, "w+", encoding='utf-8') as csvOut:
		writer = csv.DictWriter(csvOut, fieldnames=outcolumn)
		writer.writeheader()
		count = 0
		for row in reader:
			post = row[outcolumn[1]]
			post = preprocess_post(post)
			title = row[outcolumn[2]]
			title = preprocess_post(title)
			tag = row[outcolumn[3]]
			tag = clean_tags(tag)
			writer.writerow({
						outcolumn[0]: row[outcolumn[0]],
						outcolumn[1]: post,
						outcolumn[2]: title,
						outcolumn[3]: tag,
					})
		csvOut.close()
	csvIn.close()