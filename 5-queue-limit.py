from random import random
import asyncio
import time

# coroutine to generate work
async def producer(queue,id):
    print(f'{time.ctime()} Producer: Running')
     # generate work
    for i in range(10):
         # generate a value
        value = random() 
        # block to simulate work
        await asyncio.sleep((id+1)*0.1)
        # add to the queue, may block  
        await queue.put(value)  
    print(f'{time.ctime()} Producer:{id} Done')

# coroutine to 
async def consumer(queue):
    print(f'{time.ctime()} Consumer: Running')
    # consume work
    while True:
        # get a unit of work
        item = await queue.get()
        # report
        print(f'{time.ctime()} > got {item}')
        # block while processing
        if item:
            await asyncio.sleep(item)
            # mark as completed
            queue.task_done()
    # all done
    print(f'{time.ctime()} Consumer: Done')

# entry point coroutine
async def main():
    # create the shared queue
    queue = asyncio.Queue(2)
    # start the consumer
    _ = asyncio.create_task(consumer(queue))
    # create many producers
    producers = [producer(queue,i) for i in range(5)]
    # run and wait for the producers to finish
    await asyncio.gather(*producers)
    # wait for the consumer to process all items
    await queue.join()

# start the asyncio program 
asyncio.run(main())

