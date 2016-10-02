import csv
import json

csvfile = open('ratings.csv', 'r')
jsonfile = open('ratings.json', 'w')

fieldnames = ("userId", "movieId", "rating", "timestamp")
reader = csv.DictReader(csvfile, fieldnames)

first_line = reader.next()
for row in reader:
    json.dump(row, jsonfile)
    jsonfile.write('\n')
