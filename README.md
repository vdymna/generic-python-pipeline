# General Purpose Pipeline Class
Pipeline class can be used to construct data pipelines by adding tasks together and defining task dependencies.   

### Example usage
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

### Results
`pipeline.run()` returns a dictionary object with the results for each task/function.
