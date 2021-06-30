from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class Defect(BaseModel):
    id: int
    name: str
    severity: int


class PredictionMessage(BaseModel):
    id: int
    timestamp: datetime
    defect: Optional[Defect]


@dataclass
class FakeDefectDescriptor:
    """ Describes a fake defect metadata
    """
    id: int
    name: str
    weight: float
    severity: int


DEFAULT_DEFECTS = [
    FakeDefectDescriptor(1, 'LFC', 0.3, 5),
    FakeDefectDescriptor(2, 'Breakout', 0.1, 10),
    FakeDefectDescriptor(3, 'Sliver', 0.2, 7),
    FakeDefectDescriptor(4, 'Fake Defect', 0.4, 2)
]