"""This file seeks to compare the compression time and resulting file sizes of the following formats:

JSON
Avro
ORC
Parquet
XML

Note that all of these files are able to be implemented in Snowflake and PySpark
"""
import os
from collections import namedtuple
#from fastavro import parse_schema, writer
from dataclasses import dataclass,field
import pandas as pd
from utils import Timer
#import pytables
##some helper functions

def read_csv(filename:str,column_names:list[str]) -> pd.DataFrame:
    df = pd.read_csv(filename,sep="\t",header=None,skiprows=50)
    df.columns = column_names
    return df

@dataclass
class FormatTransformer:
    initial_name: str
    column_names: list[str]
    data: pd.DataFrame = field(default_factory=pd.DataFrame)
    new_name: str = ""
    og_size: int = 0

    def __post_init__(self):
        self.data = read_csv(self.initial_name,self.column_names)
        self.og_size: int = os.path.getsize(self.initial_name)


    def convert_to_json(self):
        """convert the data to json format"""
        new_name = self.initial_name.split(".")[0] + ".json" ##changing the format of the file
        self.data.to_json(new_name)
        self.new_name = new_name

    def convert_to_xml(self):
        """convert the data to xml format"""
        new_name = self.initial_name.split(".")[0] + ".xml" ##changing the format of the file
        self.data.to_xml(new_name)
        self.new_name = new_name

    def convert_to_parquet(self,compression = "gzip"):
        """convert the data to parquet format"""
        new_name = self.initial_name.split(".")[0] + ".parquet" + "." + compression ##changing the format of the file
        self.data.to_parquet(new_name,compression=compression)
        self.new_name = new_name

    def convert_to_orc(self):
        """convert the data to parquet format"""
        new_name = self.initial_name.split(".")[0] + ".orc" ##changing the format of the file
        self.data.to_orc(new_name)
        self.new_name = new_name

    ##additional file types

    def convert_to_hdf(self):
        new_name = self.initial_name.split(".")[0] + ".h5" ##changing the format of the file
        self.data.to_hdf(new_name,key="a")
        self.new_name = new_name

    def convert_to_pickle(self,compression = 'gzip'):
        new_name = self.initial_name.split(".")[0] + ".pkl" + "." + compression ##changing the format of the file
        self.data.to_pickle(new_name,compression=compression)
        self.new_name = new_name

    ##method to assess file attributes
    def check_file_size(self,display=False):
        """checking the size of the newly generated file. Returns file size in Bytes"""
        new_size = os.path.getsize(self.new_name)
        if display:
            print('Original file size:', self.og_size, 'bytes')
            print('New file Size:', new_size, 'bytes')
            print('Difference:',new_size-self.og_size,'bytes')
            print('Reduction Rate:',round(self.og_size/new_size,ndigits=2))

        return new_size

def main():
    metadata_names = "dkey, meta_doi, doc_doi, doi, doc_pub_date, meta_pub_date, pub_date, doc_authors, meta_authors, author, doc_title, meta_title, title".split(", ")
    initial_name= "sample.head.300.txt"
    #data = read_csv(initial_name,metadata_names)
    def time(method,name):
        print(f"\nTiming for {name}")
        with Timer():
            method()
    
    with Timer():
        print("Initial Overhead")
        trans = FormatTransformer(initial_name,metadata_names)
    
    time(trans.convert_to_json,"JSON")

    trans.check_file_size(display=True)

    time(trans.convert_to_xml,"XML")

    trans.check_file_size(display=True)

    time(trans.convert_to_hdf,"H5")

    trans.check_file_size(display=True)

    time(trans.convert_to_parquet,"PARQUET")

    trans.check_file_size(display=True)

    time(trans.convert_to_pickle,"PICKLE")

    trans.check_file_size(display=True)

if __name__ == "__main__":
    main()
