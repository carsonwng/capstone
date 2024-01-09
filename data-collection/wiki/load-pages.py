import os, multiprocessing, signal
from csv import DictReader
import pywikibot

path = "../data/categories"
o_path = path + "/train"
cat_fieldnames = [
    "REF_IDX",
    "FAMILY",
    "LOCALITY",
    "CATEGORY"
]
page_fieldnames = [
    "FAMILY",
    "LOCALITY",
    "CATEGORY",
    "PAGE"
]

processes = 8
chunk_size = 50

pywikibot.Site("en", "wikibooks") #preload
pywikibot.Site("en", "wikipedia") #preload

def worker(chunk):
    for page in chunk[1]:
        with open(f"{o_path}/{chunk[0]}_{page[0]}_{page[1].pageid}.txt", "w", encoding="utf-8") as file:
            file.write(page[1].text)
            file.close()
    
    return

if __name__ == "__main__":
    pool = multiprocessing.Pool(processes, lambda: signal.signal(signal.SIGINT, signal.SIG_IGN))

    with open(f"{path}/cat-list-pandas.csv", "r", encoding="utf-8") as categories_csv:
        dictreader = DictReader(categories_csv, cat_fieldnames, lineterminator="\r")

        family = ""
        locality = ""

        added = os.listdir(f"{path}/train")
        added.sort(reverse=True)

        cat_counter = int(added[0][0:1]) if len(added) > 0 else 0
        cat_list = list(dictreader)
        try:
            while cat_counter < len(cat_list):
                row = cat_list[cat_counter]

                if family != row["FAMILY"] or locality != row["LOCALITY"]:
                    family = row["FAMILY"]
                    locality = row["LOCALITY"]

                    site = pywikibot.Site(locality, family)

                category = pywikibot.Category(site, row["CATEGORY"])

                page_count = category.categoryinfo["pages"]
                added_count = sum(list(map(lambda p: 1 if int(p.split("_")[0]) == cat_counter else 0, added)))
                
                if added_count == page_count:
                    cat_counter += 1
                    continue

                pages = list(enumerate(category.articles(recurse=False)))

                print(f"Querying {category} | {page_count} pages long.")

                for chunk_i in range(0, page_count, chunk_size):
                    # print([page[1].text for page in pages[chunk_i:chunk_i + 50]])
                    chunk = (cat_counter, pages[chunk_i:chunk_i + 50])
                    task = pool.apply_async(worker, (chunk, ))
                    # print(chunk[0], chunk[1])

                    # print(pages[chunk_i:chunk_i + 50])

                cat_counter += 1

            pool.close()
            pool.join()

        except KeyboardInterrupt:
            print("\rTerminating...")

            pool.terminate()
            pool.join()
