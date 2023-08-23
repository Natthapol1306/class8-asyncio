from random import random
import asyncio
import time

# coroutine to generate work
async def producer(queqe):
    print('Producer: Running')
    # generate work
    for i in range(10):
        # generate a value
        value = random()
        # block to simulate work
        await asyncio.sleep(value)
        # add to the queqe
        await queqe.put(value)
    # send an all done signal
    await queqe.put(None)
    print(f'{time.ctime()} Producer: Done')

# consume work
async def consumer(queqe):
    print(f'{time.ctime()} Consumer: Running')
    # consume work
    while True:
        # get a unit of work
        try:
            # retrieve the get() awaitable
            get_await = queqe.get()
            # await the awaitable with a timeout
            item = await asyncio.wait_for(get_await, 0.5)
        except asyncio.TimeoutError:
            print(f'{time.ctime()} Consumer: gave up waiting...')
            continue
        # check for stop
        if item is None:
            break
        # report
        print(f'{time.ctime()} >got {item}')
    # all done
    print('Consumer: Done')

# entry point coroutine
async def main():
    # create the shared queqe
    queqe = asyncio.Queue()
    # run the producer and consumers
    await asyncio.gather(producer(queqe), consumer(queqe))

# start the asyncio program
asyncio.run(main())
