import os
import requests
import aiohttp
import asyncio
import time


performance = {"Write": {}, "Read": {}}
numOfRW = 10000

async def fetch(session, url, payload):
    async with session.post(url, json=payload) as response:
        return response.status, await response.text()

asyncio.run(performRW(4, 6, 3))
asyncio.run(performRW(4, 6, 6))
asyncio.run(performRW(6, 10, 8))


def printGraph():
    global performance
    print(performance)
    # print the graph
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots()
    ax.bar(performance["Write"].keys(), performance["Write"].values())
    ax.set_ylabel('Time (in seconds)')
    ax.set_xlabel('Configurations')
    ax.set_title('Write Performance')
    # plt.xticks(rotation=90)
    plt.show()
    fig, ax = plt.subplots()
    ax.bar(performance["Read"].keys(), performance["Read"].values())
    ax.set_ylabel('Time (in seconds)')
    ax.set_xlabel('Configurations')
    ax.set_title('Read Performance')
    # plt.xticks(rotation=90)
    plt.show()


printGraph()
