#!/usr/bin/python

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


def finish_project(project_info, output_stream, project, num_top_entries):
    sorted_projects = sorted(project_info, reverse=True)
    project_info = []

    for index, element in enumerate(sorted_projects):
        if index == num_top_entries:
            break
        output_stream.write("%s\t(%s,%s)\n" % (project, element[1], element[0]))

    return project_info

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(input_stream, num_top_entries, output_stream):
    # This needs to take each of the lines of input, and return
    # only the top 5 numbers from the result.
    # Need to keep adding to list, then finish the list with the project

    project_info = []
    current_project = ""
    for line in input_stream:
        words = line.split()
        language = words[0]
        article = words[1][1:len(words[1]) - 1]
        view_count = words[2][:len(words[2]) - 1]
        if language != current_project:
            if current_project != "":
                project_info = finish_project(project_info, output_stream, current_project, num_top_entries)
            current_project = language

        project_info.append((int(view_count), article))


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name, num_top_entries):
    # We pick the working mode:

    # Mode 1: Debug --> We pick a file to read test the program on it
    if debug == True:
        my_input_stream = codecs.open(i_file_name, "r", encoding='utf-8')
        my_output_stream = codecs.open(o_file_name, "w", encoding='utf-8')
    # Mode 2: Actual MapReduce --> We pick std.stdin and std.stdout
    else:
        my_input_stream = sys.stdin
        my_output_stream = sys.stdout

    # We launch the Map program
    my_reduce(my_input_stream, num_top_entries, my_output_stream)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. Input parameters
    debug = True

    i_file_name = "../../my_result/A01 - Hint1/sort_simulation.txt"
    o_file_name = "../../my_result/A01 - Hint1/reduce_simulation.txt"

    num_top_entries = 5

    my_main(debug, i_file_name, o_file_name, num_top_entries)
