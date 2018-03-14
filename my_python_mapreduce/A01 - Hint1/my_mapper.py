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



def output_mapper(language_dict, output_stream, num_top_entries):
    for language in language_dict:
        # processing_list = sorted(language_dict[language], reverse=True)
        processing_list = language_dict[language]
        for index, element in enumerate(processing_list):
            output_stream.write("%s (%s,\t%s)\n" % (language, element[1], element[0]))
            if index == num_top_entries - 1:
                break

def sort_dictionary(language_dict, num_top_entries):
    for language in language_dict:
        if len(language_dict) > num_top_entries:
            processing_list = sorted(language_dict[language], reverse=True)
            print(processing_list)
            processing_list = processing_list[:num_top_entries]
            language_dict[language] = processing_list

    return language_dict

def add_line_to_tuple(line, projects, languages, num_top_entries):
    LANGUAGE_INDEX = 0
    ARTICLE_INDEX = 1
    VIEW_INDEX = 2

    project = ""
    article = ""
    featured_language = False
    for index, word in enumerate(line.split()):

        if index == LANGUAGE_INDEX:
            for language in languages:
                if word.startswith(language):
                    featured_language = True
            # This prevents languages that aren't included
            # from being added to the job
            if not featured_language:
                break

            project = word
            if word not in projects:
                projects[word] = []

        if index == ARTICLE_INDEX:
            article = word

        if index == VIEW_INDEX:
            view_count = int(word)
            current_tuple = (view_count, article)
            projects[project].append(current_tuple)
            projects[project] = sorted(projects[project], reverse=True)
            if len(projects[project]) > num_top_entries:
                projects[project] = projects[project][:num_top_entries]

    return projects


# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(input_stream, languages, num_top_entries, output_stream):
    projects = {}

    for line in input_stream:
        # line is type 'str', each individual line
        projects = add_line_to_tuple(line, projects, languages, num_top_entries)

    output_mapper(projects, output_stream, num_top_entries)

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name, languages, num_top_entries):
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
    my_map(my_input_stream, languages, num_top_entries, my_output_stream)

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

    i_file_name = "../../my_dataset/pageviews-20180219-100000_0.txt"
    o_file_name = "../../my_result/A01 - Hint1/mapResult.txt"

    languages = ["en", "es", "fr"]
    num_top_entries = 5

    # 2. Call to the function
    my_main(debug, i_file_name, o_file_name, languages, num_top_entries)
