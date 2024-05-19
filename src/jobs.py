from datetime import datetime
from abc import ABC, abstractmethod
from typing import Literal, List, Any, Callable, Mapping, Collection, Dict
from pydantic import BaseModel, Field
from workflows import WorkflowHoistory


Status = Literal['success', 'progress', 'failed']
STATUS_MAPPING = {'s': 'success', 'p': 'progress', 'f': 'failed'}

class Job(BaseModel):
    """_summary_

    Args:
        BaseModel (_type_): _description_
    """
    id: int
    workflow: int = Field(alias='workflow_id')
    name: str
    created_at: datetime = datetime.now()


class JobHistory(BaseModel):
    id: int
    workflow_history: int = Field(alias='workflow_history_id')
    job: int = Field(alias='job_id')
    created_at: datetime = datetime.now()
    status: Status = 'progress'

    @classmethod
    def from_db(cls, status: str = 'p', **kwargs):  # type: ignore
        """_summary_

        Args:
            status (str, optional): _description_. Defaults to 'p'.

        Returns:
            _type_: _description_
        """
        status_literal = STATUS_MAPPING.get(status)
        assert status_literal in Status.__args__, f"Invalid status: {status_literal}"  # type: ignore
        return cls(status=status_literal, **kwargs)  # type: ignore

workflow_jobs = [
    Job(id=1, workflow_id=1, name="firstjob"),
    Job(id=2, workflow_id=1, name="second_job"),
    Job(id=3, workflow_id=1, name="third_job"),
    Job(id=1, workflow_id=2, name="firstjob"),
    Job(id=2, workflow_id=2, name="second_job"),
    Job(id=3, workflow_id=2, name="third_job")
]

