# Vyhľadávanie informácií projekt

import  multiprocessing

import re
import time

from prettytable import PrettyTable

ENERGYCONVERSION = 4.1868

class Food:

    def __init__(self, name, energy, protein, fat, carbs):
        self.name = name
        self.energy = energy
        self.protein = protein
        self.fat = fat
        self. carbs = carbs

    def print_data(self):
        print("{}: {:.2f}, {:.2f}, {:.2f}, {:.2f}".format(self.name, self.energy, self.protein, self.fat, self.carbs))

    def get_data(self):
        return self.name, self.energy, self.protein, self.fat, self.carbs




def recalculate_energy(original_value):
    new_value = original_value / ENERGYCONVERSION
    print('Recalculating energy: {}kJ -> {:.2f}kcal\n'.format(original_value, new_value))
    return new_value



def define_source_path(original_path):
    return './sources/' + original_path



def check_data_contents(data_list):
    for item in data_list:
        if item is not None:
            return True
    return False



def analyze_page(new_page):

    food_reg = r'\[\[Category\:.{0,20}?(F|f)ood.{0,20}?\]\]'
    drink_reg = r'\[\[Category\:.{0,20}?(D|d)drink.{0,20}?\]\]'

    if re.search(food_reg, new_page) or re.search(drink_reg, new_page):

        # Name regex
        name_reg = r'<title>.{0,40}?<\/title>'
        name = None

        # Energy, Protein, Fat, Carbohydrates regexes:
        data_regexes = [r'(kJ|kj|kcal)\s*=\s*([^\s]+)', r'protein\s*=\s*([^\s]+).g',
                        r'fat\s*=\s*([^\s]+).g', r'(carbs|carbohydrates)\s*=\s*([^\s]+).g']
        data = [None, None, None, None]  


        if re.search(name_reg, new_page):   # Name
            name = str(re.search(name_reg, new_page).group(0))[7:-8]

        counter = 0
        for reg in data_regexes:
            if re.search(reg, new_page):    # Other data
                tmp = str(re.search(reg, new_page).group(0))
                # print(tmp)
                tag, value = tmp.split('=', 1)
                value = float(re.search(r'([0-9]+\.?[0-9]+|[0-9]+)', value).group(0))    # Get only number from previous regex e.g. kcal=345.8 => 345.8
                # if tag == 'kJ' or tag == 'kj':
                if re.search(r'.*kj.*', tmp, re.IGNORECASE):
                    value = recalculate_energy(value)
                data[counter] = round(value, 2)
                # data[counter] = '{:.2f}'.format(value)
                counter += 1

        if name is not None and check_data_contents(data):
            return Food(name=name, energy=data[0], protein=data[1], fat=data[2], carbs=data[3])
        else:
            return None

            


def analyze_file(file_path, index_list, indexing = True):

    print(file_path)

    with open(file_path, 'r', encoding='utf8') as f:

        results = []
        # c = 0

        page_cnt = 0
        page_flag = False
        page = ''

        for line in f:
            if re.search(r'<page>', line):

                if index_list is not None and indexing:
                    page_cnt += 1
                    if page_cnt in index_list:
                        page_flag = True
                else:
                    page_flag = True
            
            if page_flag:
                page += line

            if re.search(r'<\/page>', line) and page_flag:
                # c += 1
                page_flag = False
                result = analyze_page(page)
                if result is not None:
                    # print(c)
                    results.append(result)
                page = ''
                
    return results



# CTRL + K + C = multiline comment
# CTRL + K + U = multiline uncomment
# CMD + Shift + P => kill all terminals



with open('index.txt', 'r', encoding='utf8') as paths:

    foods = []
    index = True
    start = time.time()

    # pool = multiprocessing.Pool(multiprocessing.cpu_count())

    for path in paths:

        if path[0] != '#':

            path = path.rstrip('\n')

            if index:
                if len(path) != 0:
                    indexes = [int(x) for x in path.split(',')]
                    # print(indexes)
                else:
                    indexes = None

                index = False

            else:
                path = define_source_path(path)
                tmp_foods = analyze_file(path, indexes) # Add False to the function call for indexing to be disabled
                # tmp_foods = pool.map(analyze_file, path)

                for food in tmp_foods:
                    foods.append(food)

                index = True
    
    end = time.time()

    # cntr = 1
    # for food in foods:
    #     print('{}. '.format(cntr), end='')
    #     food.print_data()
    #     cntr += 1

    cntr = 1
    table = PrettyTable([' ', 'Food', 'Calories(kcal)', 'Protein(g)', 'Fat(g)', 'Carbohydrates(g)'])
    for food in foods:
        table.add_row((cntr,) + food.get_data())
        cntr += 1
    print(table)

    print("Total elapsed time: {:.2f} s".format(end - start))


    # print(foods[1].get_data2('name'))

# without indexing = 131s
# with indexing = 105s