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


def split_by_char(string, split_char):

    if split_char in string:
        split_index = string.find(split_char)
        language = string[:split_index]
        project = string[split_index + 1:]
    else:
        language = string
        project = 'wikipedia'

    return language, project

def extract_view_count(words):
    VIEW_INDEX = 2
    word = words[VIEW_INDEX]
    views = 0

    if word.isdigit():
        views = int(word)
    else:
        list = [int(s) for s in words if s.isdigit()]
        views = list[0]

    return views

# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(input_stream, per_language_or_project, output_stream):
    SPLIT_CHAR = '.'
    using_language = per_language_or_project

    results = {}
    for line in input_stream:
        words = line.split()
        language, project = split_by_char(words[0], SPLIT_CHAR)
        view_count = extract_view_count(words)
        if using_language == True:
            if language not in results:
                results[language] = 0
            results[language] += view_count
        else:

            projects = [project]
            for project in projects:
                if SPLIT_CHAR in project:
                    first_project, second_project = split_by_char(project, SPLIT_CHAR)
                    projects = [first_project]

            for project in projects:
                if project not in results:
                    results[project] = 0
                results[project] += view_count

    for key, value in results.items():
        output_stream.write("%s\t%s\n" % (key, value))

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name, per_language_or_project):
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
    my_map(my_input_stream, per_language_or_project, my_output_stream)

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

    i_file_name = "../../../my_dataset/pageviews-20180219-100000_0.txt"

    per_language_or_project = True # True for language and False for project
    if per_language_or_project:
        o_file_name = "../../../my_result/A01 - Hint2/Second_Round_MapReduce/Per Language/mapResult.txt"
    else:

        o_file_name = "../../../my_result/A01 - Hint2/Second_Round_MapReduce/Per Project/mapResult.txt"

    # 2. Call to the function
    my_main(debug, i_file_name, o_file_name, per_language_or_project)
