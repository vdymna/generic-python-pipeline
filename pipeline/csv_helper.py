import csv
import itertools

class CsvHelper:
    """Small helper class to work with CSV files."""


    def build_csv_file(self, rows, file, header=None):
        """Write to data to a CSV file based on a list of tuples.
           Header is optional but of provided must have proper column sequence."""
        if header:
            rows = itertools.chain([header], rows)
        writer = csv.writer(file, delimiter=',')
        writer.writerows(rows)
        file.seek(0)
        return file