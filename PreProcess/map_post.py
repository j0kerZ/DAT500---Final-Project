from lxml import etree
import csv

datafile 	= "stackoverflow.com-Posts/Posts.xml"
csvout		= "sof.csv"

outcolumn	= [	"ID", "Score", "ViewCount", "Body", 
				"OwnerUserID", "LastEditorUserID", 
				"LastActivityDate", "Title", "Tags"]

questions 	= []

count = 0
with open(csvout, "w+", encoding='utf-8') as f:
	w = csv.DictWriter(f, fieldnames=outcolumn)
	w.writeheader()
	for event, element in etree.iterparse(datafile):
		if element.tag == "row":
			if int(element.get("LastActivityDate")[:4]) >= 2018:
				if (int(element.get("PostTypeId"))) == 1 or (int(element.get("PostTypeId")) == 2):
					if int(element.get("PostTypeId")) == 1:
						if int(element.get("AnswerCount")) > 0:
							questions.append(int(element.get("Id")))
						else:
							continue

					w.writerow({
								outcolumn[0]: element.get("Id"),
								outcolumn[1]: element.get("Score"),
								outcolumn[2]: element.get("ViewCount"),
								outcolumn[3]: element.get("Body"),
								outcolumn[4]: element.get("OwnerUserId"),
								outcolumn[5]: element.get("LastEditorUserId"),
								outcolumn[6]: element.get("LastActivityDate"),
								outcolumn[7]: element.get("Title"),
								outcolumn[8]: element.get("Tags")
							})
					count += 1
		element.clear()
	f.close()
print(count)