
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

import codecs
import random
import sys
import os

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sol_name, dir):
    # 1. We create the solutionRDD
    res = []

    # 2. We collect all the files of the dir
    instances = os.listdir(dir)

    # 3. We collect the content of each file
    for name in instances:
        # 3.1. We open the file
        my_input_file = codecs.open(dir + name, "r", encoding='utf-8')

        # 3.2. We read it line by line
        for line in my_input_file:
            res.append(line)

        # 3.3. We close the input file
        my_input_file.close()

    # 4. We sort the lines
    res.sort()

    # 5. We open the file for writing
    my_output_file = codecs.open(dir + sol_name, "w", encoding='utf-8')

    # 6. We write the content to the file
    for line in res:
        my_output_file.write(line)

    # 7. We close the output file
    my_output_file.close()

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. If we run it from command line
    if len(sys.argv) > 1:
        my_main(sys.argv[1], sys.argv[2])
    # 2. If we run it via Pycharm
    else:
        sol_name = "solution.txt"
        dir = "C://Users//Ignacio.Castineiras//Desktop//Big Data Analytics//4. Assignments//A01_solved//my_result//A01 - Hint3//"
        my_main(sol_name, dir)
