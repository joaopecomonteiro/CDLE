import multiprocessing as mp
from time import time
def how_many_within_range(row, minimum, maximum):
    """
    Returns how many numbers lie within `maximum` and `minimum` in a given `row`
    """
    print(mp.current_process(), ' ', row, time())  # this will print the process object and the item it is working with
    count = 0
    for num in row:
        if num >= minimum and num <= maximum:
            count += 1
    return count


def how_many_within_range_row_only(row, minimum=4, maximum=8):
  print(mp.current_process(),' ',row, time()) # this will print the process object and the item it is working with
  count = 0
  for num in row:
     if minimum <= num <= maximum:
        count = count + 1
  return count