
from typing import Literal, List, Any, Callable, Mapping, Collection, Dict
from abc import ABC, abstractmethod
from celery import Celery

from workflows import WorkflowHoistory
from jobs import Job, JobHistory, workflow_jobs

Status = Literal['success', 'progress', 'failed']
STATUS_MAPPING = {'s': 'success', 'p': 'progress', 'f': 'failed'}

app = Celery('tasks', broker='redis://localhost:6379/0', backend='redis://localhost:6379/1')

app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Kolkata',
    enable_utc=True,
)
app.conf.task_track_started = True
app.conf.task_send_sent_event = True
app.conf.worker_send_task_events = True
app.conf.worker_hijack_root_logger = False
app.conf.worker_prefetch_multiplier = 0
app.conf.worker_enable_remote_control = True


all_talsk = []
job_history_ : List[JobHistory] = []




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
                   self.workflow_history.workflow and wj.name == self.job_name), None)
        print(job, self.job_name, self.workflow_history)
        print(workflow_jobs)
        job_history = JobHistory(workflow_history_id=self.workflow_history.id,
                                 job_id=job.id, id=len(job_history_) + 100)
        self.job_id = job_history.id
        self.job_history = job_history

    def job_end(self, status: Status = "success"):

        if not self.job_history:
            raise Exception("Job history not found")
        self.job_history.status = status
        job_history_.append(self.job_history)
        if status == "failed":
            raise Exception(f"job failed {self.job_history.id}")
        print(f"{' job end ':#^100}")

    def execute(self):
        status: Status = "success"
        try:
            self.job_start()
            task_name = f"{self.workflow_history.workflow}_{self.job_name}"

            # exc_code =  self.exc_code

            @app.task(name=task_name)
            def dynamic_job(*f_args, **f_kwargs):
                return self.exc_code(*f_args, **f_kwargs)
            task = app.tasks[task_name]
            task_shedule = {f'task_{task_name}': {
                    'task': task_name,
                    'schedule': 10,
                    'args': (self.f_args),
                    'kwargs': (self.f_kwargs)
                }}
            all_talsk.append(task_shedule)

            app.conf.beat_schedule = {key: value for task in all_talsk for key, value in task.items()}

            # print(app.conf)
            return task

        except Exception as e:
            print(e)
            status = "failed"
        finally:
            self.job_end(status=status)
 

    @abstractmethod
    def execute_job(self) -> List[JobHistory]:
        pass


class JobExecuter(JobExecutorTemplate):
    def __init__(self, *args: (Any), **kwargs: Dict[str, Any]) -> None:
        super().__init__(*args, **kwargs)
        self.dependencies: List['JobExecuter'] = []

    def execute_job(self) -> List[JobHistory]:

        if not self.job_id:
            raise Exception("Job not started yet")
        r: Any = self.exc_code(*self.f_args, **self.f_kwargs)
        return job_history_
    
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


from workflow_job import *





