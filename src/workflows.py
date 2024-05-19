from datetime import datetime
from typing import Literal, List
from pydantic import BaseModel, Field

Status = Literal['success', 'progress', 'failed']
STATUS_MAPPING = {'s': 'success', 'p': 'progress', 'f': 'failed'}

class Workflow(BaseModel):
    """_summary_

    Args:
        BaseModel (_type_): _description_
    """
    id: int
    name: str
    created_at: datetime = datetime.now()


class WorkflowHoistory(BaseModel):
    """_summary_

    Args:
        BaseModel (_type_): _description_

    Returns:
        _type_: _description_
    """
    id: int
    workflow: int = Field(alias='workflow_id')
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


workflow = [
    Workflow(id=1, name="first"),
    Workflow(id=2, name="second")
]

workflow_history_: List[WorkflowHoistory] = []

class WorkflowExection:
    """_summary_
    """

    def __init__(self, workflow_name: str) -> None:
        self.workflow_name: str = workflow_name
        self.workflow_history: WorkflowHoistory = None  # type: ignore

    def __enter__(self):
        print(f"{' Start workflow : ':*^100}")

        w = next((w for w in workflow if w.name == self.workflow_name), None)
        if not w:
            raise Exception("workflow not found")
        wh_ = WorkflowHoistory(id=len(workflow_history_) + 1, workflow_id=w.id)
        self.workflow_history = wh_
        print(f"workflow history id: {self.workflow_history.id}")
        return wh_

    def __exit__(self, exc_type: str, exc_val: str, exc_tb: str):
        self.workflow_history.status = 'success'
        if exc_type or exc_val or exc_tb:
            print(exc_type, exc_val, exc_tb)
            self.workflow_history.status = 'failed'
        workflow_history_.append(self.workflow_history)
        print(f"{' End workflow : ':*^100}")

        return True
