from fastapi import FastAPI, HTTPException, Query
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from typing import List
import json


from datetime import datetime, date

app = FastAPI()

cluster = Cluster(['cassandra'])
session = cluster.connect('ecommerce')

@app.get("/")
def read_root():
    return {"Hello": "World"}
