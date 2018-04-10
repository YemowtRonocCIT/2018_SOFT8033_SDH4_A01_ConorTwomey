#!/bin/bash

MYPATH=./my_result/
databricks fs cp -r "dbfs:/FileStore/tables/A01_my_result" "$MYPATH"
python merge_solutions.py "solution.txt" "$MYPATH"
