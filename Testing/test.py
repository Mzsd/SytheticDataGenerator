from multiprocessing import Process, Queue
import time

def worker(queue, i):
    print(f'Process {i} started')
    while not queue.empty():
        item = queue.get()  # Retrieve an item from the queue
        print(f'Process {i} got: {item}')
        time.sleep(1)

if __name__ == '__main__':
    q = Queue()
    q.put(1)  # Put an initial item in the queue
    q.put(2)
    q.put(3)
    q.put(4)
    
    print(q.qsize())
    p1 = Process(target=worker, args=(q, 1))    
    p2 = Process(target=worker, args=(q, 2))
    
    p1.start()
    p2.start()
    
    p1.join()
    p2.join()
    
    # result = q.get()  # Retrieve the processed item
    # print(f'Result: {result}')

