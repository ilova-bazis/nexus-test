import asyncio
from src.nexus_nats import NatsClient

# Example usage:


def callback(msg):
    print(msg.data)
    return {}


async def main():
    nats_client = NatsClient('nats://localhost:4222')
    # await nats_client.connect()

    # response = await nats_client.request('my.subject', 'Hello NATS!')
    await nats_client.subscribe("get.profile", callback)
    # print(response)

    # await nats_client.close()


def get_profile(msg):
    print(msg)
    return {"all": "GOOD"}

loop = asyncio.get_event_loop()
loop.create_task(main())
loop.run_forever()
# asyncio.run(main())
