# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import sys
import codecs

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, o_file_dir, per_language_or_project):
    # 1. We remove the solution directory, to rewrite into it
    dbutils.fs.rm(o_file_dir, True)

	# Complete the Spark Job

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    dataset_dir = "/FileStore/tables/A01_my_dataset/"
    o_file_dir = "/FileStore/tables/A01_my_result/"

    per_language_or_project = True  # True for language and False for project

    my_main(dataset_dir, o_file_dir, per_language_or_project)
