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


def split_identifier(language_project, per_language_or_project):
    identifier = ''

    split_index = language_project.find('.')
    if split_index != -1:
        if per_language_or_project == True:
            identifier = language_project[:split_index]
        else:
            identifier = language_project[split_index + 1:]
            if '.' in identifier:
                split_index = identifier.find('.')
                identifier = identifier[:split_index]
    else:
        if per_language_or_project != True:
            identifier = 'Wikipedia'
        else:
            identifier = language_project

    return identifier

def process_line(line, per_language_or_project):
    CORRECT_WORD_COUNT = 4
    LANGUAGE_PROJECT_INDEX = 0
    VIEW_INDEX = 2

    words = line.split()
    language_project = words[LANGUAGE_PROJECT_INDEX]

    if len(words) == CORRECT_WORD_COUNT:
        views = int(words[VIEW_INDEX])

    else:
        for word in words:
            if word.isdigit():
                if int(word) != 0:
                    views = int(word)
                    break

    result = []

    identifier = split_identifier(language_project, per_language_or_project)
    result.append((identifier, views))

    return result

def remove_blanks(count_tuple):
    IDENTIFIER_INDEX = 0
    COUNT_INDEX = 1

    valid = False

    identifier = count_tuple[IDENTIFIER_INDEX]
    count = count_tuple[COUNT_INDEX]

    if count != 0 and identifier != '':
        valid = True

    return valid

def back_to_line(count_tuple, totalCount):
    IDENTIFIER_INDEX = 0
    VIEW_COUNT_INDEX = 1
    line = ''

    identifier = count_tuple[IDENTIFIER_INDEX]
    count = count_tuple[VIEW_COUNT_INDEX]

    if count != 0:
        percent = (float(count) / float(totalCount)) * 100
        line += '(%s, %s, %s%%)' % (identifier, count, percent)

    return line

def just_numbers(count_tuple):
    VIEW_COUNT_INDEX = 1
    count = 0

    count += count_tuple[VIEW_COUNT_INDEX]

    return count

def list_to_lines(tuple_list):
    IDENTIFIER_INDEX = 0
    VIEW_COUNT_INDEX = 1
    line = ''
    for tupl in tuple_list:
        line += '(%s, %s)\t' % (tupl[IDENTIFIER_INDEX], tupl[VIEW_COUNT_INDEX])

    return line

def lines_to_tuples(lines):
    IDENTIFIER_INDEX = 0
    VIEW_COUNT_INDEX = 1
    # Remove the brackets on either end
    for line in lines.split('\t'):
        line = line[1:len(line) - 1]

        words = line.split()

        article = words[IDENTIFIER_INDEX]
        if article != '(,':
            count = int(words[VIEW_COUNT_INDEX])

            article = article[:len(article) - 1]

            return (article, count)

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, o_file_dir, per_language_or_project):
    # 1. We remove the solution directory, to rewrite into it
    dbutils.fs.rm(o_file_dir, True)

    # Complete the Spark Job
    inputRDD = sc.textFile("%s/*.txt" % dataset_dir)

    dividedRDD = inputRDD.map(lambda line: process_line(line, per_language_or_project))
    dividedRDD = dividedRDD.map(list_to_lines).map(lines_to_tuples).filter(remove_blanks)
    dividedRDD.persist()

    combinedRDD = dividedRDD.combineByKey(lambda count_value: count_value,
                                          lambda count_value, new_value: count_value + new_value,
                                          lambda first_accumulator,
                                                 second_accumulator: first_accumulator + second_accumulator)

    totalCountRDD = dividedRDD.map(just_numbers)
    totalCount = totalCountRDD.reduce(lambda x, y: x + y)

    linedRDD = combinedRDD.map(lambda tup: back_to_line(tup, totalCount))
    linedRDD.saveAsTextFile(o_file_dir)


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

    per_language_or_project = False  # True for language and False for project

    my_main(dataset_dir, o_file_dir, per_language_or_project)
