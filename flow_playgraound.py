from pydantic import BaseModel, Field
from typing import ClassVar, Literal, Dict, Any
from enum import Enum
from datetime import datetime

Status = Literal['success', 'progress', 'failed']

STATUS_MAPPING = {'s': 'success', 'p': 'progress', 'f': 'failed'}


class Workflow(BaseModel):
    id : int
    name : str
    created_at: datetime = datetime.now()

class Job(BaseModel):
    id : int
    workflow : int = Field(alias='workflow_id')
    name : str
    created_at: datetime = datetime.now()

class JobDependancy(BaseModel):
    job: int = Field(alias="job_id")
    upstream_job : int = Field(alias="upstream_job_id")

class WorkflowHoistory(BaseModel):
    id : int
    workflow : int = Field(alias='workflow_id')
    created_at: datetime = datetime.now()
    status: Status = 'progress'

    @classmethod
    def from_db(cls, status: str = 'p', **kwargs): # type: ignore
        status_literal = STATUS_MAPPING.get(status)
        assert status_literal in Status.__args__, f"Invalid status: {status_literal}" # type: ignore
        return cls(status=status_literal, **kwargs) # type: ignore


class JobHistory(BaseModel):
    id : int
    workflow_history : int = Field(alias='workflow_history_id')
    job: int = Field(alias='job_id')
    created_at: datetime = datetime.now()
    status: Status = 'progress'

    @classmethod
    def from_db(cls, status: str = 'p', **kwargs): # type: ignore
        status_literal = STATUS_MAPPING.get(status)
        assert status_literal in Status.__args__, f"Invalid status: {status_literal}" # type: ignore
        return cls(status=status_literal, **kwargs) # type: ignore

class Xcom(BaseModel):
    id : int
    data: Dict[Any,Any] = {}
    job: int = Field(alias='job_id')
    workflow: int = Field(alias='workflow_id')
    Workflow_history: int = Field(alias='workflow_history_id')


workflow = [Workflow(id=1, name="first"), Workflow(id=2, name="second")]
workflow_jobs = [Job(id=1, workflow_id=1, name="firstjob"),
     Job(id=2, workflow_id=1, name="secon_job"),
     Job(id=3, workflow_id=1, name="third_job"),
     Job(id=1, workflow_id=2, name="firstjob"),
     Job(id=2, workflow_id=2, name="secon_job"),
     Job(id=3, workflow_id=2, name="third_job")
    ]
wh = []
jh = []
class WorkflowExection:

    def __init__(self, workflow_name: str) -> None:
        self.workflow_name : str = workflow_name
        self.workflow_history : WorkflowHoistory = None # type: ignore

    def __enter__(self):
        w = next((w for w in workflow if w.name==self.workflow_name), None)
        wh_ = WorkflowHoistory(id=len(wh) + 1, workflow_id=w.id)
        self.workflow_history = wh_
        
        return wh_

    def __exit__(self, exc_type, exc_val, exc_tb):
        
        self.workflow_history.status = 'success'
        if exc_type or exc_val or exc_tb:
            self.workflow_history.status = 'failed'
        wh.append(self.workflow_history)
        return True
    
class JobExecuter():
    @staticmethod
    def execute_job(workflow_history: WorkflowHoistory, job_name: str):
        job = next((i for i in workflow_jobs if i.workflow==workflow_history.id and i.name==job_name), None)
        job_history = JobHistory(workflow_history_id=workflow_history.id, job_id=job.id, id=len(jh) + 1)
        job_history.status = 'success'
        jh.append(job_history)
        return jh

with WorkflowExection(workflow_name="first") as we:

    je = JobExecuter.execute_job(workflow_history=we, job_name="firstjob")
    je = JobExecuter.execute_job(workflow_history=we, job_name="secon_job")



with WorkflowExection(workflow_name="second") as we:

    je = JobExecuter.execute_job(workflow_history=we, job_name="firstjob")
    je = JobExecuter.execute_job(workflow_history=we, job_name="secon_job")
