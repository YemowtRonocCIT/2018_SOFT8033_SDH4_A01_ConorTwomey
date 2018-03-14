set mypath=%cd%\my_result
databricks fs cp -r "dbfs:/FileStore/tables/A01_my_result" "%mypath%"
python.exe merge_solutions.py "solution.txt" "%mypath%\\"



