import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any

logger = logging.getLogger(__name__)


class BaseETL(ABC):
    """
    Abstract ETL with a three-phase async pipeline.

    Subclasses implement extract / transform / load; the synchronous run()
    entry-point bridges into asyncio so Click commands stay sync.
    """

    @abstractmethod
    async def extract(self) -> Any:
        """Fetch or read raw data and return it."""

    @abstractmethod
    async def transform(self, raw: Any) -> Any:
        """Clean, join, and compute derived fields."""

    @abstractmethod
    async def load(self, data: Any) -> None:
        """Persist the result (write files, update a store, etc.)."""

    def run(self) -> None:
        """Run extract → transform → load synchronously."""
        asyncio.run(self._pipeline())

    async def _pipeline(self) -> None:
        raw = await self.extract()
        data = await self.transform(raw)
        await self.load(data)
