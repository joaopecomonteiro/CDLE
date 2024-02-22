# %%
import multiprocessing as mp
import numpy as np
from time import time
import utils as utils

# %%

r = 10
m = 1000
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

print(results[:10])

# %%

start_time = time()

pool = mp.Pool(mp.cpu_count())

results = pool.starmap(utils.how_many_within_range, [(row, 4, 8) for row in data])

pool.close()

print(f'Total time: {time()-start_time} seconds')

print(results[:10])



