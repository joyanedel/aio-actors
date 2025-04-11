# Async IO Actors

A simple library for building actors in asyncio.

## Installation

```bash
pip install aio-actors
```

## Usage

```python
from aio_actors import Actor


class MyActor(Actor[str]):
    async def handle_message(self, message: str) -> None:
        print(f"{self.__class__.__name__}: message:{message}")

class MySecondActor(Actor[str]):
    def __init__(self, first_actor: MyActor) -> None:
        super().__init__()
        self._first_actor = first_actor

    async def handle_message(self, message: str) -> None:
        print(f"{self.__class__.__name__}: message:{message}")
        await self._first_actor.send_message(message + " from second actor")


async def main():
    actor = MyActor()
    await actor.start()

    second_actor = MySecondActor(actor)
    await second_actor.start()
    
    for i in range(10):
        await second_actor.send_message(f"aio-actors is awesome - {i}")

    await asyncio.sleep(1)
    await actor.shutdown()
    await second_actor.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```
