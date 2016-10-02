import csv
import json

csvfile = open('ratings.csv', 'r')
jsonfile = open('ratings.json', 'w')

fieldnames = ("userId", "movieId", "rating", "timestamp")
reader = csv.DictReader(csvfile, fieldnames)

first_line = reader.next()
for row in reader:
    dictionary = {}
    dictionary[fieldnames[0]] = int(row[fieldnames[0]])
    dictionary[fieldnames[1]] = int(row[fieldnames[1]])
    dictionary[fieldnames[2]] = float(row[fieldnames[2]])
    dictionary[fieldnames[3]] = int(row[fieldnames[3]])
    json.dump(dictionary, jsonfile)
    jsonfile.write('\n')
