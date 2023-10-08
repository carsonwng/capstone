from csv import DictReader

cat_fields = [
    "PAGE"
]

with open("../data/categories/all-pages.csv", "r") as file:
    dictreader = DictReader(file, cat_fields)

    print(list(dictreader))