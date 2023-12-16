import os, multiprocessing, signal
import mwparserfromhell
from tqdm import tqdm

path = "../data/categories/train-clean"

processes = 8
chunk_size = 500

articles = os.listdir(path)
progress = tqdm(total=len(articles))

def worker(chunk):
    for page in chunk:
        with open(f"{path}/{page}", "r", encoding="utf-8") as file:
           
            wikitext = mwparserfromhell.parse(file.read())

            for el in wikitext.filter((
               
               mwparserfromhell.nodes.Text,
               mwparserfromhell.nodes.Wikilink,
               mwparserfromhell.nodes.Heading
            )):
                if el.startswith("__"):
                   wikitext.remove(el)

                if isinstance(el, mwparserfromhell.nodes.Wikilink) and el.title.lower().startswith("category:"):
                   wikitext.remove(el)
                
                if isinstance(el, mwparserfromhell.nodes.Heading):
                    wikitext.remove(el)
            
            with open(f"{path}-txt/{page}", "w", encoding="utf-8") as plaintext:
                plaintext.write(wikitext.strip_code())
                plaintext.close()
            
            file.close()
            progress.update(50) # TODO fix jumping prog bar.

if __name__ == "__main__":
    pool = multiprocessing.Pool(processes, lambda: signal.signal(signal.SIGINT, signal.SIG_IGN))

    try:
        for chunk_i in range(0, len(articles), chunk_size):
            chunk = articles[chunk_i:chunk_i + 50]
            task = task = pool.apply_async(worker, (chunk, ))

        pool.close()
        pool.join()
        progress.close()

    except KeyboardInterrupt:
        print("\nTerminating...")

        pool.terminate()
        pool.join()
        progress.close()
    
    except Exception as e:
        print(e.message, e.args)