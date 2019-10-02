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

### Examples of Pipelines
[hn_top_keywords_pipeline.py](example_pipelines/hn_top_keywords_pipeline.py) - the pipeline to ingest JSON data, clean it, analyze it and write top keywords analysis results to a CSV file.  
[csv_to_postgres_pipeline.py](example_pipelines/csv_to_postgres_pipeline.py) - the pipeline to load CSV file into a staging table, do some data transformations and load the data into a final Postgres table.
