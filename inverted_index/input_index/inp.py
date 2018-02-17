
import csv

csvFile = open('forum_node_2.tsv','w')
with open('forum_node.tsv','r') as tsvin:
	tsvin = csv.reader(tsvin, delimiter='\t')
	csvout = csv.writer(csvFile, delimiter='\t', quotechar='"', quoting=csv.QUOTE_ALL)
	for row in tsvin:
	    row[4] = row[4].replace("\n", " ").replace("\r"," ")
	    csvout.writerow(row)
csvFile.close()