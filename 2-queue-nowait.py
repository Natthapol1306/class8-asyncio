from random import random
import asyncio
import time

# coroutine to generate work
async def producer(queqe):
    print(f'{time.ctime()} Producer: Running')
    # generate work
    for i in range(10):
        # generate work
        value = random()
        # block to simulate work
        await asyncio.sleep(value)
        # add to the queqe
        await queqe.put(value)
        #print(f'{time.ctime()} Producer: put {value}')
    # send an all done signal
    await queqe.put(None)
    print(f'{time.ctime()} Producer: Done')
# coroutine to consume work
async def consumer(queqe):
    print(f'{time.ctime()} Consumer: Running')
    # consume work
    while True:
        # get a unit for work
        item = await queqe.get()
        # check for stop signal
        if item is None:
            break
        # report
        print(f'{time.ctime()} >got {item}')
    # all done
    print(f'{time.ctime()} Consumer: Done')

# entry point coroutine
async def main():
    # create the shared queqe
    queqe = asyncio.Queue()
    # run the producer and consumers
    await asyncio.gather(producer(queqe), consumer(queqe))

# start the asyncio program
asyncio.run(main())
