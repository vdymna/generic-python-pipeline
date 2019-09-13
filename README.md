# Generic Purpose Pipeline Class
General purpose Python pipeline class to chain various task together and their dependencies in order to construct function based data pipelines. 

```python
from pipeline.pipeline import Pipeline

pipeline = Pipeline()

@pipeline.task()
def first_task(x):
    return x + 10

@pipeline.task(depends_on=first_task)
def second_task(x):
    return x * 2

@pipeline.task(depends_on=second_task)
def third_task(x):
    return x - 5

output = pipeline.run(5)
```
