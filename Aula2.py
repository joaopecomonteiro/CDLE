# %%
import multiprocessing as mp
import numpy as np
from time import time
import utils as utils

# %%

r = 10
m = 100
n = 10

np.random.seed(100)
arr = np.random.randint(0, r, size=[m,n])
data = arr.tolist()
print(data[:10])


# %%



start_time = time()

results = []

for row in data:
    results.append(utils.how_many_within_range(row, 4, 8))

print(f"{time() - start_time} seconds")
print(results[:10])


# %%
num_cpus = mp.cpu_count()
print('Num cpus = ', num_cpus)

start_time = time()

pool = mp.Pool(num_cpus)

print(f'Time to create pool: {time()-start_time} seconds')

results = [pool.apply(utils.how_many_within_range, args=(row, 4, 8)) for row in data]

pool.close()

print(f'Total time: {time()-start_time} seconds')

print(results[:10])



# %%

start_time = time()
pool = mp.Pool(num_cpus)
results = pool.map(utils.how_many_within_range_row_only, [row for row in data])

pool.close()
print(f'Total time: {time()-start_time} seconds')

print(results[:10])

# %%

# [6, 3, 5, 4, 4, 5, 5, 7, 6, 5]

start_time = time()

pool = mp.Pool(mp.cpu_count())

results = pool.starmap(utils.how_many_within_range, [(row, 4, 8) for row in data])

pool.close()

print(f'Total time: {time()-start_time} seconds')

print(results[:10])


# %%

start_time = time()

pool = mp.Pool(mp.cpu_count())

results_apply_async = []

def collect_result(result):
    #global results_apply_async
    #print(result)
    results_apply_async.append(result)

for i, row in enumerate(data):
    pool.apply_async(utils.how_many_within_range2, args=(i, row, 4, 8, ), callback=collect_result)

pool.close()
pool.join()
print(f'Total time: {time()-start_time} seconds')

results_apply_async.sort(key=lambda x: x[0])
results_apply_async_final = [r for i, r in results_apply_async]

print(results_apply_async_final[:10])


# %%

#results_process = []

def how_many_within_range3(i, row, minimum, maximum):
   """Returns how many numbers lie within `maximum` and `minimum` in a given `row`"""
   global results_process
   count = 0
   for num in row:
      if minimum <= num <= maximum:
         count = count + 1
   print(count)
   results_process[i] = count
   print(results_process)

def howmany_within_range3(i, row, minimum, maximum):
   """Returns how many numbers lie within `maximum` and `minimum` in a given `row`"""
   count = 0
   global results
   for num in row:
      if minimum <= num <= maximum:
         count = count + 1
   results[i] = count

start_time = time()

processes = []

for i, row in enumerate(data):
   p = mp.Process(target=howmany_within_range3, args=(i, row, 4, 8, ))
   processes.append(p)
   p.start()

for process in processes:
   process.join()


print(f'Total time: {time()-start_time} seconds')

print(results_process[:10])




