import csv

from core.db import DB
from parser.models.organisations import Organisations


def csv_for_listmonk(name_file: str, lst_data: list):
    with open(f'../{name_file}.csv', mode='w', newline='') as cvs_file:
        writer = csv.writer(cvs_file)
        writer.writerows(lst_data)


# class GenerateCSV:
def load_data(category: str):
    db = DB()
    lst = db.category_select_with_email(category)

    return reformat(lst)


def reformat(lst: list) -> list:
    r_lst = []
    title = 'email, name, attributes'.split(', ')
    r_lst.append(title)

    for i in lst:
        if len(i.mail.split(', ')) > 1:
            for j in i.mail.split(', '):
                # r_lst.append([j, i.title, ''])
                r_lst.append(create_string(j, i))
        else:
            # r_lst.append([i.mail, i.title, ''])
            r_lst.append(create_string(i.mail, i))
    return r_lst


def create_string(mail: str, organisation: Organisations):
    return [mail, organisation.title,  '']


def main(name):
    csv_for_listmonk(name, load_data(name))


def load_all_city():
    import json

    # Открываем файл для чтения
    with open("/home/sasha/PycharmProjects/Parser_ya_maps/parser/all_cities.json", "r", encoding="utf-8") as f:
        # Загружаем данные из файла в переменную
        data = json.load(f)
        citys = []
        for i in data:
            citys.append(i['Город'])
        print(citys)

if __name__ == '__main__':
    load_all_city()
    # main('Агенство недвижимости')
