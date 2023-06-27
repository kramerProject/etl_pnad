import csv
import json

csv_file_path = 'ufs.csv'
json_file_path = 'ufs.json'

# Read CSV file
csv_data = []
with open(csv_file_path, 'r') as csv_file:
    csv_reader = csv.DictReader(csv_file, delimiter=';')
    for row in csv_reader:
        csv_data.append(row)

# Convert CSV data to JSON
json_data = json.dumps(csv_data, indent=4)

# Write JSON data to file
with open(json_file_path, 'w') as json_file:
    json_file.write(json_data)
