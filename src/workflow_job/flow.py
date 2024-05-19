from workflows import WorkflowExection
from tasks import JobExecuter
def first_job(a: int, b: int):
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
    je2 = JobExecuter(we, "second_job", first_job, f_kwargs={"a": 600, "b": 10})
    je3 = JobExecuter(we, "second_job",first_job, f_kwargs={"a": 600, "b": 5})
    # je1.upstream(je1)
    # je1.upstream(je2)

    je1 >> je2
    je1.run_dependencies()


with WorkflowExection(workflow_name="first") as we:
    je1 = JobExecuter(we, "firstjob",
                               first_job, f_kwargs={"a": 100, "b": 200})
    je2 = JobExecuter(we, "second_job", first_job, f_kwargs={"a": 600, "b": 10})
    je3 = JobExecuter(we, "third_job",first_job, f_kwargs={"a": 600, "b": 5})
    # je1.upstream(je1)
    # je1.upstream(je2)
    je1 >> je3
    je1.run_dependencies()


with WorkflowExection(workflow_name="first") as we:
    je1 = JobExecuter(we, "firstjob",
                               first_job, f_kwargs={"a": 100, "b": 200})
    je2 = JobExecuter(we, "second_job", first_job, f_kwargs={"a": 600, "b": 10})
    je3 = JobExecuter(we, "third_job",first_job, f_kwargs={"a": 600, "b": 5})
    # je1.upstream(je1)
    # je1.upstream(je2)
    je1 >> je2
    je1.run_dependencies()