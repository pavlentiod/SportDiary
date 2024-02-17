import json as js

file = 'C:\\Users\\pavel\\PycharmProjects\\SPLITS\\_names_and_sex.json'
with open(file, encoding='utf-8-sig') as f:
    all_names = js.load(f)
