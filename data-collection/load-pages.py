from pwiki.wiki import Wiki
from pwiki.mquery import MQuery
from dotenv import load_dotenv
import os
import json
from csv import DictWriter

load_dotenv()
wiki = Wiki("en.wikibooks.org", os.getenv("WIKI_USER"), os.getenv("WIKI_PWD"))

raw = open("../data/recipes.json", "r")
recipes_list = json.load(raw)

dataset_fields = [
    'TITLE',
    'TEXT'
]

def fetch_recipe(title):
    recipe = MQuery.page_text(wiki, title)
    return recipe

with open("../data/recipe-dataset-raw.csv", mode="a") as file:
    # recipes = json.load(file)
    dictwriter = DictWriter(file, fieldnames=dataset_fields)

    i = 0

    while i < (len(recipes_list) // 20) + 1: # fixes rounding down but keeps int. Didn't want to import ceil.
        batch = recipes_list[20 * i:(20 * i) + 20]         

        # print(batch)
        recipe_obj = fetch_recipe(batch)
        # print(recipe_obj)

        dictwriter.writerows(recipe_obj)
        i += 1

    # print(list(map(fetch_recipe, recipes_list[0:4])))
    # json.dump(file, indent=4, sort_keys=True)
    
# recipe = wiki.page_text()

# with open("recipes.json", "w") as file:
#     json.dump(recipes, file)


# for title, index in recipes_list:
#     print(index)
#     # current = recipes_list[i]

#     # recipe = wiki.page_text(current)
#     # print(recipe)

#     if (index > 5): break
#     # recipes.dump()