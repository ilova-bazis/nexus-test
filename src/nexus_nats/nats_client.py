import nats
import json
from uuid import uuid4 as uuid
import asyncio
from enum import Enum

from nats.aio.client import Msg

class RequestType(Enum):
    ENDPOINT = "endpoint"
    INTERNAL = "req"
    NOTIFICATION = "broadcast"


class NatsClient:
    def __init__(self, url: str, entityId: int, name: str, version=1.0):
        self.url = url
        self.nc = None
        self.entityId = entityId
        self.lock = asyncio.Lock()
        self.version = version
        self.name = name

    async def connect(self):
        print("Connecting to NATS")
        with self.lock:
            if not self.nc:
                self.nc = await nats.connect(self.url)

    async def close(self):
        await self.nc.close()

    async def request(self, method: str, params: dict):
        if not self.nc:
            await self.connect()

        subject = self.prepare_subject(method, type=RequestType.INTERNAL)
        metadata = {"sid": self.entityId, "name": self.name, "version": self.version, "method": method}
        request = {
            "metadata": metadata,
            "id": uuid(),
            "version": self.version,
            "method": method,
            "params": params
        }
        message = json.dumps(request)
        response = await self.nc.request(subject, message)
        response = json.loads(response)
        if response.result:
            return response
        elif response.error:
            raise Exception(response.error.reason)
        else:
            raise Exception("Invalid response")

    def prepare_subject(self, method: str, type=RequestType.INTERNAL):
        return f"{self.entityId}.{type.value}.{self.version}.{method}"

    async def subscribe(self, method: str, sub_type: RequestType, callback: callable):
        if not self.nc:
            await self.connect()
        subject = self.prepare_subject(method, sub_type)

        async def message_handler(msg: Msg):
            # reply = f"Replying to: {callback(msg)}"
            response = callback(msg)
            json_response = json.dumps(response)
            await self.nc.publish(msg.reply, json_response.encode())

        await self.nc.subscribe(subject, cb=message_handler)
