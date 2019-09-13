import json
import io
import csv
import string
from datetime import datetime

from pipeline.pipeline import Pipeline
from pipeline.csv_helper import CsvHelper


exclude_words = ('the', 'to', 'a', 'of', 'for', 'in', 'and', 'is', '–', 
                 'on', 'hn:', 'an', 'at', 'not', 'with', 'why', 'how', 'your', 
                 'from', 'new', 'you', 'i', 'by', 'what', 'my', 'are', 'as', 
                 'that', 'we', 'it', 'be', 'now', 'using', 'has')

pipeline = Pipeline()
csv_helper = CsvHelper()


@pipeline.task()
def file_to_json():
    with open('hn_stories_2014.json', 'r') as file:
        data_dict = json.load(file)
        stories = data_dict['stories']
    return stories


@pipeline.task(depends_on=file_to_json)
def filter_stories(stories):
    def is_popular(story):
        return story['points'] > 50 and story['num_comments'] > 1 and not story['title'].startswith('Ask HN')
    
    return (
        story for story in stories if is_popular(story)
    )


@pipeline.task(depends_on=filter_stories)
def json_to_csv(stories):
    lines = []
    for story in stories:
        lines.append(
            (story['objectID'],
             datetime.strptime(story['created_at'], '%Y-%m-%dT%H:%M:%SZ'),
             story['url'],
             story['points'],
             story['title'])
        )
    return csv_helper.build_csv_file(
        lines,
        file=io.StringIO(),
        header=['objectID', 'created_at', 'url', 'points', 'title']
    )


@pipeline.task(depends_on=json_to_csv)
def extract_titles(csv_file):
    reader = csv.reader(csv_file)
    header_row = next(reader)
    title_idx = header_row.index('title')
    
    return (row[title_idx] for row in reader)


@pipeline.task(depends_on=extract_titles)
def clean_titles(titles):
    return (title.lower().strip(string.punctuation) for title in titles)


@pipeline.task(depends_on=clean_titles)
def build_keyword_dictionary(titles):
    keywords = {}
    for title in titles:
        for word in title.split():
            if word and word not in exclude_words:
                if word not in keywords:
                    keywords[word] = 0
                keywords[word] += 1
    return keywords


@pipeline.task(depends_on=build_keyword_dictionary)
def extract_top_keywords(keywords):
    top_keywords = []
    for word, count in sorted(keywords.items(), key=lambda item: item[1], reverse=True):
        top_keywords.append((word, count))
    
    return top_keywords[:100]


@pipeline.task(depends_on=extract_top_keywords)
def save_final_csv_file(keywords):  
    output_csv_file = open('top_keywords.csv', 'w', newline='')
    return csv_helper.build_csv_file(
        keywords,
        file = output_csv_file,
        header=['keyword', 'count']
    )


output = pipeline.run()