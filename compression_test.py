"""
This program seeks to find the smallest .parquet compression format
The following compression types that are evaluated include:

For Snowflake:
.gzip
.brotli
.zstd
.deflate
.raw_deflate

For PySpark:
.snappy
.gzip
.lzo
.brotli
.lz4
.zstd
"""

import os
from collections import namedtuple
#from fastavro import parse_schema, writer
from dataclasses import dataclass,field
import pandas as pd
from utils import Timer
from semistructure_test import FormatTransformer

@dataclass
class ZipTester:
    trans:FormatTransformer

    def test_format(self,format:str):
        """testing out a given format."""
        print("Testing format: ",format)
        #print(self.trans.check_file_size())
        if format in ['snappy', 'gzip', 'brotli','zstd']:
            self.trans.convert_to_parquet(format)
            print(f"New file size: {self.trans.check_file_size()} Bytes")
            print(f"Difference in size: {self.trans.check_file_size() - self.trans.og_size} Bytes")
        else:
            print("Not Implemented")
            NotImplementedError()
                
def main():
    metadata_names = "dkey, meta_doi, doc_doi, doi, doc_pub_date, meta_pub_date, pub_date, doc_authors, meta_authors, author, doc_title, meta_title, title".split(", ")
    initial_name= "sample.head.300.txt"
    #data = read_csv(initial_name,metadata_names)
    ft = FormatTransformer(initial_name,metadata_names)
    zt = ZipTester(ft)
    formats_to_try = ["snappy", "gzip", "brotli","zstd","deflate","raw_deflate","lzo"]
    for format in formats_to_try:
        with Timer():
            zt.test_format(format)
    

main()
