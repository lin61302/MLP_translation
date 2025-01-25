import time
import pymongo

def safe_mongocall(call):
  def _safe_mongocall(*args, **kwargs):
    for i in range(5):
      try:
        return call(*args, **kwargs)
      except pymongo.AutoReconnect:
        time.sleep(pow(2, i))
    print("Error: Failed operation!")
  return _safe_mongocall
  

def calculate_time(func): 
    # added arguments inside the inner1, 
    # if function takes any arguments, 
    # can be added like this. 
    def time(*args, **kwargs): 
        # storing time before function execution 
        begin = time.time() 
        func(*args, **kwargs) 
        # storing time after function execution 
        end = time.time() 
        print("Total time taken in : ", func.__name__, end - begin) 
    return time 