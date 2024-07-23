from pydantic import BaseModel

class Job(BaseModel):
    title: str
    workload: int