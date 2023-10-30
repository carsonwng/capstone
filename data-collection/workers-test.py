import os, multiprocessing, time

path = os.path.join("../data/categories/test")
fakedata = []
start = time.time()
processes = multiprocessing.cpu_count() * 2
tasks = 50000

for page in range(tasks):
    fakedata.append(f"Lorem ipsum dolor sit amet, consectetur adipiscing elit. Vestibulum tincidunt risus laoreet quam imperdiet, et semper ex aliquam. Quisque nisl eros, semper quis aliquet interdum, ultrices vel nisl. Proin et euismod velit. Curabitur ac consequat mi. Sed tellus erat, vestibulum vitae elit auctor, vulputate accumsan tellus. Cras ipsum felis, convallis id ornare id, sodales ut eros. Vestibulum vulputate urna vel tellus laoreet tristique. Curabitur ut urna sem. Fusce sed metus scelerisque, malesuada nulla eu, consectetur erat. Etiam pulvinar vestibulum diam, vitae elementum erat laoreet eget. Morbi finibus, lacus nec sagittis scelerisque, lacus diam ultrices enim, ac rhoncus nisi lorem sed nulla. Nunc rhoncus libero urna, vitae sodales nisl imperdiet eget. Mauris pharetra nec magna at posuere. Nam pretium, massa eget sollicitudin ullamcorper, neque erat ornare felis, eget iaculis nibh ligula sed ex. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Vestibulum sit amet massa ac nunc vulputate malesuada eu vel urna.")

fakedata = list(enumerate(fakedata))

def worker(chunk):
    for page in chunk[1]:
        print(f"adding {chunk[0]}_{page[0]} under pid: {os.getpid()}")

        with open(f"{path}/{chunk[0]}_{page[0]}.txt", "w", encoding="utf-8") as file:
            file.write(page[1])
            file.close()

if __name__ == "__main__":
    pool = multiprocessing.Pool(processes)
    data = enumerate(fakedata)

    for chunk_i in range(0, len(fakedata), 25):
        chunk = (f"0_{chunk_i // 25}", fakedata[chunk_i:chunk_i + 50])

        task = pool.apply_async(worker, (chunk,))

    pool.close()
    pool.join()

end = time.time()
print(f"\n{tasks} -> {processes} p's | Compute time: {round(end - start, 2)}s")