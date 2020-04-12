import csv

csvdataIn = "../../sof.csv"
csvdataOut = "../../postsonly.csv"

outcolumn	= [	"ID", "Score", "ViewCount", "Body", 
				"OwnerUserID", "LastEditorUserID", 
				"Title", "Tags"]

with open(csvdataIn, "r+", encoding='utf-8') as csvIn:
	reader = csv.DictReader(csvIn)
	with open(csvdataOut, "w+", encoding='utf-8') as csvOut:
		writer = csv.DictWriter(csvOut, fieldnames=outcolumn)
		writer.writeheader()
		count = 0
		for row in reader:
			if row["Title"] != "":
				writer.writerow({
							outcolumn[0]: row[outcolumn[0]],
							outcolumn[1]: row[outcolumn[1]],
							outcolumn[2]: row[outcolumn[2]],
							outcolumn[3]: row[outcolumn[3]],
							outcolumn[4]: row[outcolumn[4]],
							outcolumn[5]: row[outcolumn[5]],
							outcolumn[6]: row[outcolumn[6]],
							outcolumn[7]: row[outcolumn[7]],
						})
				count += 1
		csvOut.close()
	csvIn.close()
print(count)