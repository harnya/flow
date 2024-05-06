
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Collection, Dict, List, Literal, Mapping,
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


class Job(BaseModel):
    """_summary_

    Args:
        BaseModel (_type_): _description_
    """
    id: int
    workflow: int = Field(alias='workflow_id')
    name: str
    created_at: datetime = datetime.now()


class JobDependancy(BaseModel):

    job: int = Field(alias="job_id")
    upstream_job: int = Field(alias="upstream_job_id")


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
        assert status_literal in Status.__args__, f"Invalid status: {
            status_literal}"  # type: ignore
        return cls(status=status_literal, **kwargs)  # type: ignore


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
        assert status_literal in Status.__args__, f"Invalid status: {
            status_literal}"  # type: ignore
        return cls(status=status_literal, **kwargs)  # type: ignore


class Xcom(BaseModel):
    """_summary_

    Args:
        BaseModel (_type_): _description_
    """
    id: int
    data: Dict[Any, Any] = {}
    job: int = Field(alias='job_id')
    workflow: int = Field(alias='workflow_id')
    Workflow_history: int = Field(alias='workflow_history_id')


workflow = [
    Workflow(id=1, name="first"),
    Workflow(id=2, name="second")
]
workflow_jobs = [
    Job(id=1, workflow_id=1, name="firstjob"),
    Job(id=2, workflow_id=1, name="secon_job"),
    Job(id=3, workflow_id=1, name="third_job"),
    Job(id=1, workflow_id=2, name="firstjob"),
    Job(id=2, workflow_id=2, name="secon_job"),
    Job(id=3, workflow_id=2, name="third_job")
]

wh: List[WorkflowHoistory] = []
jh: List[JobHistory] = []
x_con: List[Xcom] = []
jd : JobDependancy =[]

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
        wh_ = WorkflowHoistory(id=len(wh) + 1, workflow_id=w.id)
        self.workflow_history = wh_
        print(f"workflow history id: {self.workflow_history.id}")
        return wh_

    def __exit__(self, exc_type: str, exc_val: str, exc_tb: str):
        self.workflow_history.status = 'success'
        if exc_type or exc_val or exc_tb:
            print(exc_type, exc_val, exc_tb)
            self.workflow_history.status = 'failed'
        wh.append(self.workflow_history)
        print(f"{' End workflow : ':*^100}")

        return True


class JobExecutorTemplate(ABC):

    def __init__(self, workflow_history: WorkflowHoistory,
                job_name: str,
                exc_code: Callable[[], Any],
                f_args: Collection[Any] | None = None,
                f_kwargs: Mapping[str, Any] | None = None) -> None:
        self.workflow_history = workflow_history
        self.exc_code = exc_code
        self.job_name = job_name
        self.f_args = f_args or ()
        self.f_kwargs = f_kwargs or {}
        self.job_id = 0
        self.status = "success"

    def job_start(self):

        print(f"{' job start ':#^100}")

        job = next((wj for wj in workflow_jobs if wj.workflow ==
                   self.workflow_history.id and wj.name == self.job_name), None)

        job_history = JobHistory(workflow_history_id=self.workflow_history.id,
                                 job_id=job.id, id=len(jh) + 100)
        self.job_id = job_history.id
        self.job_history = job_history

    def job_end(self, status: Status = "success"):

        if not self.job_history:
            raise Exception("Job history not found")
        self.job_history.status = status
        jh.append(self.job_history)
        if status == "failed":
            raise Exception(f"job failed {self.job_history.id}")
        print(f"{' job end ':#^100}")

    def execute(self):
        status: Status = "success"
        try:
            self.job_start()
            self.execute_job()

        except Exception as e:
            print(e)
            status = "failed"
        finally:
            self.job_end(status=status)
        # return instance
 

    @abstractmethod
    def execute_job(self) -> List[JobHistory]:
        pass

    # https://python.plainenglish.io/building-microservices-with-fastapi-part-ii-a-microservice-application-with-fastapi-c190d57922ba


class JobExecuter(JobExecutorTemplate):
    def __init__(self, *args: (Any), **kwargs: Dict[str, Any]) -> None:
        super().__init__(*args, **kwargs)
        self.dependencies: List['JobExecuter'] = []

    def execute_job(self) -> List[JobHistory]:

        if not self.job_id:
            raise Exception("Job not started yet")
        r: Any = self.exc_code(*self.f_args, **self.f_kwargs)
        x_com = Xcom(id=1, data={"try": "hah"}, job_id=self.job_id,
                     workflow_id=self.workflow_history.workflow, 
                     workflow_history_id=self.workflow_history.id)
        x_con.append(x_com)
        return jh
    
    def upstream(self, *jobs: 'JobExecuter') -> None:
        if self not in self.dependencies:
            self.dependencies.append(self)
        for job in jobs:
            if job not in self.dependencies:
                self.dependencies.append(job)
    
    def run_dependencies(self):
        for job in self.dependencies:
            print(job)
            job.execute()

    def __rshift__(self,  *jobs: 'JobExecuter'):
        return self.upstream(*jobs)


def first_job(a: int, b: int) -> Any:
    """_summary_

    Args:
        a (int): _description_
        b (int): _description_

    Returns:
        Any: _description_
    """
    print(a, b)
    print(a/b)
    return "in first python"


with WorkflowExection(workflow_name="second") as we:
    je1 = JobExecuter(we, "firstjob",
                               first_job, f_kwargs={"a": 100, "b": 200})
    je2 = JobExecuter(we, "secon_job", first_job, f_kwargs={"a": 600, "b": 10})
    je3 = JobExecuter(we, "secon_job",first_job, f_kwargs={"a": 600, "b": 5})
    # je1.upstream(je1)
    # je1.upstream(je2)
    print(je1.job_id, "----------------")

    je1 >> je2
    # je2 >> je3
    je1.run_dependencies()


print(jh)
print(wh)
print(x_con)
