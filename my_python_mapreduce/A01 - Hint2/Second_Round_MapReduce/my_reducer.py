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

def extract_total_view_count(input_stream, total_petitions):
    PETITION_INDEX = 0
    VIEW_INDEX = 1

    total_view_count = {}
    previous_petition = None

    for line in input_stream:
        if line:
            words = line.split()
            petition_type = words[PETITION_INDEX]
            view_count = int(words[VIEW_INDEX])

            if petition_type == previous_petition:
                total_view_count[petition_type] += view_count
            else:
                if previous_petition != None:
                    percentage = (total_view_count[previous_petition] / total_petitions) * 100
                    total_view_count[previous_petition] = (total_view_count[previous_petition], percentage)
                previous_petition = petition_type
                total_view_count[petition_type] = view_count

    percentage = (total_view_count[previous_petition] / total_petitions) * 100
    total_view_count[previous_petition] = (total_view_count[previous_petition], percentage)

    return total_view_count

def order_input_by_popularity(total_view_count):
    VIEW_INDEX = 0
    PERCENT_INDEX = 1

    ordered_items = []

    for key, value in total_view_count.items():
        views = value[VIEW_INDEX]
        percentage = value[PERCENT_INDEX]
        current_tuple = (views, percentage, key)
        ordered_items.append(current_tuple)

    ordered_items.sort(reverse=True)
    return ordered_items

def output_items(ordered_items, output_stream):
    for next_items in ordered_items:
        views = next_items[0]
        percentage = next_items[1]
        language = next_items[2]
        output_stream.write("%s\t(%s, %s%%)\n" % (language, views, percentage))


# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(input_stream, total_petitions, output_stream):
    total_view_count = extract_total_view_count(input_stream, total_petitions)
    ordered_items = order_input_by_popularity(total_view_count)
    output_items(ordered_items, output_stream)

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, total_petitions, o_file_name):
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
    my_reduce(my_input_stream, total_petitions, my_output_stream)

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

    # This variable must be computed in the first stage
    total_petitions = 21996631

    i_file_name = "../../../my_result/A01 - Hint2/Second_Round_MapReduce/sort_simulation.txt"
    o_file_name = "../../../my_result/A01 - Hint2/Second_Round_MapReduce/reduce_simulation.txt"

    my_main(debug, i_file_name, total_petitions, o_file_name)
