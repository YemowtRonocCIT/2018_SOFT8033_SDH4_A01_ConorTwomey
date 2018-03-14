
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

# ------------------------------------------
# FUNCTION suffle_dataset_file
# ------------------------------------------
def suffle_dataset_file(file_name):
    # 1. We open the file for reading
    my_input_file = codecs.open(file_name, "r", encoding='utf-8')

    # 2. We parse the entire input file
    content = my_input_file.readlines()
    size = len(content) - 1

    # 3. We close the file
    my_input_file.close()

    # 3. We open the file again, now for writing
    my_output_file = codecs.open(file_name, "w", encoding='utf-8')

    # 4. We dump the content, now suffle, over the file
    while (size >= 0):
        # 4.1. We randomly pick one of the entries
        index = random.randint(0, size)

        # 4.2. We insert the entry into the output file
        my_output_file.write(content[index])

        # 4.3. We remove it from the list
        del content[index]

        # 4.4. We reduce the size
        size = size - 1

    # 5. We close the file
    my_output_file.close()

# ------------------------------------------
# FUNCTION split_big_file
# ------------------------------------------
def split_big_file(file_name, num_files):
    # 1. We open the file for reading
    my_input_file = codecs.open(file_name, "r", encoding='utf-8')

    # 2. We create all the files of the dataset
    dataset = []
    for i in range(num_files):
        my_output_file = codecs.open(file_name + "_" + str(i) + ".txt", "w", encoding='utf-8')
        dataset.append(my_output_file)

    # 3. We parse the input file line by line, writing it to the output files
    index = 0
    for line in my_input_file:
        # 3.1. We write the line to one of the output files
        dataset[index].write(line)

        # 3.2. We update the index, for fair balanced distribution of the big file over the dataset files
        index = index + 1
        if index == num_files:
            index = 0

    # 4. We close all files
    my_input_file.close()
    for i in range(num_files):
        dataset[i].close()

# ------------------------------------------
# FUNCTION split_big_file
# ------------------------------------------
def my_main(file_name, num_files):
    # 1. We split the big file
    split_big_file(file_name, num_files)

    # 2. We suffle each of the files
    for i in range(num_files):
        name = file_name + "_" + str(i) + ".txt"
        suffle_dataset_file(name)

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
        my_main(sys.argv[1], int(sys.argv[2]))
    # 2. If we run it via Pycharm
    else:
        file_name = "pageviews-20180219-100000"
        num_files = 70
        my_main(file_name, num_files)
