from pwiki.wiki import Wiki
from dotenv import load_dotenv
import os
import json
from csv import DictWriter

load_dotenv()

cat_fields = [
    "PAGE"
]

def format_wiki_title(title: str):
    return title.replace(" ", "_")

def query_members(wiki: Wiki, category):
    pages = wiki.category_members(format_wiki_title(category))
    # print("Pg. result", pages)

    nested_categories = list(filter(lambda sub_cat: "Category:" in sub_cat, pages))
    # print("Nested Categories", nested_categories)
    pages = list(set(pages) - set(nested_categories))

    print(f"Queried: {category} \n# of nested: {len(nested_categories)}\n# of pages: {len(pages)}")
    if not nested_categories: return pages

    for i in nested_categories:
        double_nested = query_members(wiki=wiki, category=i)
        # print("Double Nested cats:", i)
        # print("Pages_d2:", double_nested)
        pages += double_nested

    # print("Pages:", pages)
    
    return pages

with open("../data/categories/categories.json", "r") as categories_raw, open("../data/categories/all-pages.csv", mode="a") as pages_list:
    dictwriter = DictWriter(pages_list, fieldnames=cat_fields)
    categories = json.load(categories_raw)

    for wiki_url in categories:
        wiki = Wiki(wiki_url, os.getenv("WIKI_USER"), os.getenv("WIKI_PWD"))

        res = query_members(wiki, "Category:Breads")
        pages = list(map(lambda page: {"PAGE": page.encode("utf-8", errors="ignore")}, res))

        dictwriter.writerows(pages)

    
# wiki = Wiki("en.wikibooks.org", os.getenv("WIKI_USER"), os.getenv("WIKI_PWD"))

# recipes = wiki.category_members("Category:Recipes")

# with open("recipes.json", "w") as file:
#     json.dump(recipes, file)
