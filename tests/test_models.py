from aio_actors import Actor
import pytest
import asyncio


@pytest.mark.asyncio(loop_scope="session")
async def test_single_actor_send_message():
    class TestActor(Actor[str]):
        def __init__(self) -> None:
            super().__init__()
            self.test_value = asyncio.Queue[str]()

        async def handle_message(self, message: str) -> None:
            await self.test_value.put(message)

    actor = TestActor()
    actor.start()
    await actor.send_message("Hello, World!")
    await actor.shutdown()

    assert await actor.test_value.get() == "Hello, World!"


@pytest.mark.asyncio(loop_scope="session")
async def test_two_actors_send_message():
    class SenderActor(Actor[str]):
        def __init__(self, receiver: Actor[str]) -> None:
            super().__init__()
            self._receiver = receiver

        async def handle_message(self, message: str) -> None:
            await self._receiver.send_message(message)

    class ReceiverActor(Actor[str]):
        def __init__(self) -> None:
            super().__init__()
            self.test_value = asyncio.Queue[str]()

        async def handle_message(self, message: str) -> None:
            await self.test_value.put(message)

    receiver_actor = ReceiverActor()
    receiver_actor.start()
    sender_actor = SenderActor(receiver_actor)
    sender_actor.start()
    await sender_actor.send_message("Hello, World!")
    await sender_actor.shutdown()
    await receiver_actor.shutdown()
    assert await receiver_actor.test_value.get() == "Hello, World!"
