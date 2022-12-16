# Vyhľadávanie informácií - Adam Kotvan, 103000

from pyspark.sql import SparkSession

import re
import time
import unittest

from prettytable import PrettyTable
from operator import attrgetter

ENERGYCONVERSION = 4.1868 # constant used for conversion from kJ to kcal

class Food:
    """Class used for creating individual food/drink instances.
    """
    def __init__(self, name, energy, protein, fat, carbs, magnesium, iron, sodium, thiamin, riboflavin, niacin):
        self.name = name
        self.energy = energy
        self.protein = protein
        self.fat = fat
        self. carbs = carbs
        self.magnesium = magnesium
        self.iron = iron
        self.sodium = sodium
        self.thiamin = thiamin
        self.riboflavin = riboflavin
        self.niacin = niacin

    def get_data(self):
        """Getting data of particular object.

        Returns:
            string: Each element of Food object ordered for table print.
        """
        return self.name, self.energy, self.protein, self.fat, self.carbs, self.magnesium, self.iron, self.sodium, self.thiamin, self.riboflavin, self.niacin


class TestResults(unittest.TestCase):
    """Class used for performing unit testing.

    Args:
        unittest (module): unittest. 
    """
    def unit_test_1(self):
        """Unit test asserting number of results to 6.

        Returns:
            string: Unittest passed message.
        """
        self.assertEqual(len(foods), 102)
        return "\nUnit test 1 successfully passed.\n"



    def unit_test_2(self):
        """Unit test asserting number of results to 104.

        Returns:
            string: Unittest passed message.
        """
        self.assertEqual(len(foods), 6)
        return "\nUnit test 2 successfully passed.\n"



def evaluation_type_choice():
    """Getting user's input for required action (analyxing results/unit testing)

    Returns:
        boolean: True if unit tests are chosen. Otherwise False.
    """
    while True:
        testing = input("\nr - results analysis, t - unit testing\n")
        if testing == 'r':
            return False
        elif testing == 't':
            return True
        else:
            print("Choose relevant action.\n")


def create_table():
    """Function for creating a new PrettyTable object.

    Returns:
        PrettyTable: New empty table with named columns.
    """
    new_table = PrettyTable()
    new_table.title = "Nutrients"

    new_table.field_names = [' ', 'Food', 'Calories(kcal)', 'Protein(g)', 'Fat(g)', 'Carbohydrates(g)',
                            'Magnesium(mg)', 'Iron(mg)', 'Sodium(mg)',
                            'Thiamine(mg)', 'Riboflavin(mg)', 'Niacin(mg)']
    return new_table


def recalculate_energy(original_value):
    """Recalculates energy value from kJ to kcal.

    Args:
        original_value (float): Energy in kiloJoules.

    Returns:
        float: Energy in kilocalories.
    """
    return original_value / ENERGYCONVERSION


def define_source_path(original_path):
    """Modifies path to the source file.

    Args:
        original_path (string): Original name of the file.

    Returns:
        string: Modified name of the file.
    """
    return './sources/' + original_path


def check_data_contents(data_list):
    """Checking if all elements of a list are None.

    Args:
        data_list (array): List of None and float values.

    Returns:
        boolean: True if exists at least one non-None value, else returns False.
    """
    for item in data_list:

        if item is not None:
            return True

    return False


def analyze_page(new_page):
    """Analyzing page of .xml document and searching for food/drink related content.

    Args:
        new_page (string): .xml page.

    Returns:
        Food/None: Newly created Food object if search was successful, else returns None.
    """

    # Regexes used for filtering filtering pages related to food/drinks
    food_reg = r'\[\[Category\:.{0,20}?(F|f)ood.{0,20}?\]\]'
    drink_reg = r'\[\[Category\:.{0,20}?(D|d)drink.{0,20}?\]\]'

    if re.search(food_reg, new_page) or re.search(drink_reg, new_page):

        # Name regex
        name_reg = r'<title>.{0,40}?<\/title>'
        name = None

        # Energy, Protein, Fat, Carbohydrates regexes:
        data_regexes = [r'(kJ|kj|kcal)\s*=\s*([^\s]+)', r'protein\s*=\s*([^\s]+).g',
                        r'fat\s*=\s*([^\s]+).g', r'(carbs|carbohydrates)\s*=\s*([^\s]+).g',
                        r'magnesium\_mg\s*=\s*.*?([0-9]+\.?[0-9]+|[0-9]+)', r'iron\_mg\s*=\s*.*?([0-9]+\.?[0-9]+|[0-9]+)',
                        r'sodium\_mg\s*=\s*.*?([0-9]+\.?[0-9]+|[0-9]+)', r'thiamin\_mg\s*=\s*.*?([0-9]+\.?[0-9]+|[0-9]+)',
                        r'riboflavin\_mg\s*=\s*.*?([0-9]+\.?[0-9]+|[0-9]+)', r'niacin\_mg\s*=\s*.*?([0-9]+\.?[0-9]+|[0-9]+)']

        # List of data filled by found nutritional values
        data = [None, None, None, None, None, None, None, None, None, None]  


        if re.search(name_reg, new_page):   # Name search
            name = str(re.search(name_reg, new_page).group(0))[7:-8]

        counter = 0 # Iterating through data_regexes for individual nutrients searching
        for reg in data_regexes:
            if re.search(reg, new_page):    # Other data search
                tmp = str(re.search(reg, new_page).group(0))

                tag, value = tmp.split('=', 1)
                value = float(re.search(r'([0-9]+\.?[0-9]+|[0-9]+)', value).group(0))    # Get only number from previous regex e.g. kcal=345.8 => 345.8

                if re.search(r'.*kj.*', tmp, re.IGNORECASE):
                    value = recalculate_energy(value)

                data[counter] = round(value, 2)

                counter += 1

        if name is not None and check_data_contents(data): # Create and return new Food instance with found nutritional values
            return Food(name=name, energy=data[0], protein=data[1], fat=data[2], carbs=data[3],
                        magnesium=data[4], iron=data[5], sodium=data[6], 
                        thiamin=data[7], riboflavin=data[8], niacin=data[9])
        else:
            return None

            


# def analyze_file(file_path, index_list, indexing = True):
def analyze_file(file_path):
    """Analyzing .xml file and creating list of Foods/Drinks with nutritional information.

    Args:
        file_path (string): Path to the analyzed file.

    Returns:
        array: List of found and created Food objects.
    """

    print("\nFile \"{}\"".format(file_path))

    with open(file_path, 'r', encoding='utf8') as f:

        results = [] # List of all created Food objects
        # c = 0

        page_cnt = 0      # Counter for number of pages
        page_flag = False # Flag signalizing if indise of <page>
        page = ''         # Buffer variable for loading page as a whole

        for line in f:
            if re.search(r'<page>', line):

                if index_dict[file_path] is not None and indexing:
                    page_cnt += 1

                    if page_cnt in index_dict[file_path]:
                        page_flag = True
                else:
                    page_flag = True
            
            if page_flag:
                page += line

            if re.search(r'<\/page>', line) and page_flag:
                page_flag = False
                result = analyze_page(page)

                if result is not None:
                    results.append(result)

                page = ''
                
    return results



# Opening index file, loading all files to analyze, and results processing
with open('index.txt', 'r', encoding='utf8') as file:

    indexing = True        # Turning indexing on/off

    if indexing:
        print("\nIndexing is turned ON.\n")
    else:
        print("\nIndexing is turned OFF.\n")
    
    path_list = []
    index_dict = {} 
    foods = []              # List of all results
    index_line = True       # Flag signalizing line of indexes in index.txt file
    start = time.time()     # Time measurement start

    for line in file:

        if line[0] != '#':

            line = line.rstrip('\n')    # Path to oncoming analyzed file

            if index_line:

                if len(line) != 0 and indexing:
                    tmp_indexes = [int(x) for x in line.split(',')] # Create array of all indexes for currently analyzed file
                else:
                    tmp_indexes = None

                index_line = False

            else:
                path = define_source_path(line) # Path to the currently analyxed file
                path_list.append(path)
                index_dict[path] = tmp_indexes
                index_line = True



    # Spark utilization
    spark = SparkSession.builder.appName('vinf_project').master('local').getOrCreate()
    rdd = spark.sparkContext.parallelize(path_list, 8)

    print("Processing stared.")

    tmp_foods_rdd = rdd.map(analyze_file)   # Array of arrays containing all results

    for arr in tmp_foods_rdd.collect():
        for food in arr:
            foods.append(food)


end = time.time()   # Time measurement start

print("\nFinished.")
print("Total elapsed time: {:.2f} s.\n".format(end - start))


unit_testing = evaluation_type_choice()


# RESULTS ANALYSIS USER INTERFACE + ACTIONS
if unit_testing:
    test = TestResults()
    print(test.unit_test_1())
    print(test.unit_test_2())
    
else:
    while True:

        table = create_table()          # Initializing empty table to be filled with results
        show_table = True               # Flag for showing or hiding table with results
        cntr = 1                        # Result counter

        print("\n* - show all results, 1 - filter by foods , 2 - sort nutrients, 3 - get calories, x - exit")
        action = input("Choose type of action:\n")

        if action != 'x':

            # Showing all results
            if action == '*':

                for food in foods:
                    table.add_row((cntr,) + food.get_data())
                    cntr += 1

            # Searching and displaying specific food/drink
            elif action == '1':
                name_search = input("Search for food/drink:\n")

                for food in foods:

                    if food.name == name_search:
                        table.add_row((cntr,) + food.get_data())
                        cntr += 1

            # Sorting nutrients by chosen column
            elif action == '2':
                sort_by = input("calories, protein, fat, carbs, magnesium, iron, sodium, thiamine, riboflavin, niacin\n")

                asc = input("a - ascending, d - descending\n")    

                if sort_by == 'calories':
                    sort_by = 'energy' 

                tmp_foods = []

                for food in foods:

                    if getattr(food, sort_by) is not None:
                        tmp_foods.append(food)

                # Ordering nutrients in ascending/descending order
                if asc == 'a':
                    tmp_foods.sort(key=attrgetter(sort_by))
                else:
                    tmp_foods.sort(key=attrgetter(sort_by), reverse=True)

                for food in tmp_foods:
                    table.add_row((cntr,) + food.get_data())
                    cntr += 1

                print("\nSorting excludes None values.\n")

            # Searching for foods' energy value
            elif action == '3':
                name_search = input("Search for food/drink:\n")
                show_table = False

                for food in foods:

                    if food.name == name_search:
                        print("Energy: {} kcal\n".format(food.energy))
                        cntr += 1


            # No results for requested action
            if cntr == 1:
                print("No results.\n")

            else:
                # Showing results table
                if show_table:
                    print('{}\n'.format(table))

                else:
                    show_table = True
        else:
            break
