import asyncio
from abc import ABC, abstractmethod
from typing import Generic, TypeVar

T = TypeVar("T")


class Actor(Generic[T], ABC):
    """
    Base class for actors.
    """

    def __init__(self) -> None:
        self._queue = asyncio.Queue[T]()
        self.__running_tasks: set[asyncio.Task[None]] = set()

        self._started = False

    def start(self):
        """
        Starts the actor initializing pseudo background event loop.
        """
        if self._started:
            raise RuntimeError("Actor is already started")

        self._started = True
        bg_task = asyncio.create_task(self.__background_event_loop())
        bg_task.add_done_callback(self.__running_tasks.discard)
        self.__running_tasks.add(bg_task)

    async def send_message(self, message: T) -> None:
        """
        Sends a message to the actor.
        """
        self.__verify_started()
        await self._queue.put(message)

    async def __background_event_loop(self):
        while True:
            task = await self._queue.get()
            if task is None:
                break

            handler_task = asyncio.create_task(self.handle_message(task))
            handler_task.add_done_callback(self.__running_tasks.discard)
            self.__running_tasks.add(handler_task)

    async def shutdown(self):
        """
        Shuts down the actor.
        Waits for all running tasks to complete.
        """

        self.__verify_started()
        await self._queue.put(None)

        await asyncio.sleep(0)
        # Wait for all running tasks to complete
        await asyncio.wait(
            self.__running_tasks | {asyncio.create_task(asyncio.sleep(0))},
            return_when=asyncio.ALL_COMPLETED,
        )
        self._started = False

    def __verify_started(self):
        if not self._started:
            raise RuntimeError("Actor is not started")

    @abstractmethod
    async def handle_message(self, message: T) -> None:
        """
        Handles a message.
        """
        ...
