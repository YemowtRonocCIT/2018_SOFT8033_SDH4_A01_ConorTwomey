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
# FUNCTION process_line
# ------------------------------------------
def process_line(line, languages):
    # 1. We create the variable to output
    res = ()
    proj_name = ''
    page_info = ()

    # 2. We clean the line
    line = line.replace('\n', '')
    line = line.strip()
    line = line.rstrip()

    # 3. We split the line into words
    words = line.split(" ")

    # 4. We filter any non-valid entry having a wrong length
    if (len(words) == 4):
        # 4.1. If the line is a Wikipedia project for a valid language, then we keep it
        if line_is_language(line, languages):
            proj_name = words[0]

    # 5. For valid entries, we compute the rest of the info
    if (proj_name != ''):
        # 5.1. We ensure that the page name does not have some commas
        if ',' in words[1]:
            words[1] = words[1].replace(',', ':')

        # 5.2. We assign page_info to the right information
        page_info = (int(words[2]), words[1])

    # 6. We assign res properly and return res
    res = (proj_name, page_info)
    return res


def line_is_language(line, languages):
    has_language = False

    for language in languages:
        if line.startswith(language):
            has_language = True
            break

    return has_language


def initialise_list(first_tuple, num_top_entries):
    list_of_tuples = []
    sample_tuple = (0, '')

    list_of_tuples.append(first_tuple)

    while len(list_of_tuples) < num_top_entries:
        list_of_tuples.append(sample_tuple)

    return list_of_tuples


def add_new_tuple_to_list(tuple_list, new_value):
    length = len(tuple_list)

    for tup in tuple_list:
        if tup[0] < new_value[0]:
            tuple_list.append(new_value)
            break

    tuple_list = sorted(tuple_list, reverse=True)

    return tuple_list[:length]


def merging_lists(first_list, second_list):
    length = len(first_list)

    first_list.extend(second_list)
    result = sorted(first_list, reverse=True)
    result = result[:length]

    return result


def convert_to_string(tuple_list):
    language = tuple_list[0]
    list_of_articles = tuple_list[1]
    lines = ""

    for index, article_info in enumerate(list_of_articles):
        article = article_info[1]
        view_count = int(article_info[0])

        if view_count != 0:
            lines += "%s\t(%s, %s)" % (language, article, view_count)

        if index != (len(list_of_articles) - 1):
            lines += '\n'

    return lines


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, o_file_dir, languages, num_top_entries):
    # 1. We remove the solution directory, to rewrite into it
    dbutils.fs.rm(o_file_dir, True)
    inputRDD = sc.textFile("%s/*.txt" % dataset_dir)

    # Complete the Spark Job
    mappedRDD = inputRDD.map(lambda line: process_line(line, languages))
    mapped_filteredRDD = mappedRDD.filter(lambda tup: tup[0] != '')

    combinedRDD = mapped_filteredRDD.combineByKey(lambda tup: initialise_list(tup, num_top_entries),
                                                  add_new_tuple_to_list,
                                                  merging_lists)

    formattedRDD = combinedRDD.map(convert_to_string)

    formattedRDD.saveAsTextFile(o_file_dir)

    # Uncomment these lines if you want to see sample output
    values = formattedRDD.take(20)
    for item in values:
        print(item)


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

    languages = ["en", "es", "fr"]
    num_top_entries = 5

    my_main(dataset_dir, o_file_dir, languages, num_top_entries)
