
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Collection, Dict, List, Literal, Mapping
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
    """_summary_

    Args:
        BaseModel (_type_): _description_
    """
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
            self.workflow_history.status = 'failed'
        wh.append(self.workflow_history)
        print(f"{' End workflow : ':*^100}")

        return True


class JobExecutorTemplate(ABC):
    """_summary_

    Args:
        ABC (_type_): _description_

    Raises:
        Exception: _description_
    """
    workflow_history: WorkflowHoistory | None = None
    job_history: JobHistory | None = None
    job_name = None
    job_id = 0
    f_args: Collection[Any] | None = None
    f_kwargs: Mapping[str, Any] | None = None
    status: Status = "success"

    def job_start(self):
        """_summary_
        """
        print(f"{' job start ':#^100}")

        job = next((wj for wj in workflow_jobs if wj.workflow ==
                   self.workflow_history.id and wj.name == self.job_name), None)

        job_history = JobHistory(workflow_history_id=self.workflow_history.id,
                                 job_id=job.id, id=len(jh) + 100)
        self.job_id = job_history.id
        self.job_history = job_history

    def job_end(self, status: Status = "success"):
        """_summary_

        Args:
            status (Status, optional): _description_. Defaults to "success".

        Raises:
            Exception: _description_
        """
        if not self.job_history:
            raise Exception("Job history not found")
        self.job_history.status = status
        print("--------> status", status)
        print(self.workflow_history)
        self.workflow_history.status = status
        print(self.workflow_history)

        jh.append(self.job_history)
        print(f"{' job end ':#^100}")

    @classmethod
    def execute(cls, workflow_history: WorkflowHoistory, job_name: str,
                exc_code: Callable[[Any], Any],
                f_args: Collection[Any] | None = None,
                f_kwargs: Mapping[str, Any] | None = None):
        """_summary_

        Args:
            workflow_history (WorkflowHoistory): _description_
            job_name (str): _description_
            exc_code (Callable[[Any], Any]): _description_
            f_args (Collection[Any] | None, optional): _description_. Defaults to None.
            f_kwargs (Mapping[str, Any] | None, optional): _description_. Defaults to None.
        """
        status: Status = "success"
        instance = cls()
        try:
            cls.workflow_history = workflow_history
            cls.job_name = job_name
            instance.job_start()
            cls.f_args = f_args or ()
            cls.f_kwargs = f_kwargs or {}
            instance.execute_job(
                workflow_history=workflow_history, exc_code=exc_code)

        except Exception as e:
            print(e)
            status = "failed"
        finally:
            instance.job_end(status=status)
        # return instance

    @abstractmethod
    def execute_job(self, workflow_history: WorkflowHoistory,
                    exc_code: Callable[[Any], Any]) -> List[JobHistory]:
        """_summary_

        Args:
            workflow_history (WorkflowHoistory): _description_
            exc_code (Callable[[Any], Any]): _description_

        Returns:
            List[JobHistory]: _description_
        """
        pass

    # https://python.plainenglish.io/building-microservices-with-fastapi-part-ii-a-microservice-application-with-fastapi-c190d57922ba


class JobExecuter(JobExecutorTemplate):
    """_summary_

    Args:
        JobExecutorTemplate (_type_): _description_

    Raises:
        Exception: _description_

    Returns:
        _type_: _description_
    """

    # type: ignore
    def execute_job(self, workflow_history: WorkflowHoistory,
                    exc_code: Callable[[], Any]) -> List[JobHistory]:
        """_summary_

        Args:
            workflow_history (WorkflowHoistory): _description_
            exc_code (Callable[[], Any]): _description_

        Raises:
            Exception: _description_

        Returns:
            List[JobHistory]: _description_
        """
        if not self.job_id:
            raise Exception("Job not started yet")
        r: Any = exc_code(*self.f_args, **self.f_kwargs)
        print(r)
        x_com = Xcom(id=1, data={"try": "hah"}, job_id=self.job_id,
                     workflow_id=workflow_history.workflow, workflow_history_id=workflow_history.id)
        x_con.append(x_com)
        return jh


def first_job(a: int, b: int) -> Any:
    """_summary_

    Args:
        a (int): _description_
        b (int): _description_

    Returns:
        Any: _description_
    """
    print(a/b)
    return "in first python"


with WorkflowExection(workflow_name="second") as we:
    je = JobExecuter().execute(workflow_history=we, job_name="firstjob",
                               exc_code=first_job, f_kwargs={"a": 100, "b": 200})
    je = JobExecuter().execute(workflow_history=we, job_name="secon_job",
                               exc_code=first_job, f_kwargs={"a": 600, "b": 0})


print(jh)
print(wh)
print(x_con)
