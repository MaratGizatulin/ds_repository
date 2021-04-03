from bs4 import BeautifulSoup
from pprint import pprint
import requests
import csv
import pandas as pd
import datetime
from threading import Thread
import os


def check_exception_list(page, num, slice_end, slise_enable):
    try:
        if page[9].text.split(' / ')[num] and slise_enable == 0:
            return page[9].text.split(' / ')[num]
        if page[9].text.split(' / ')[num]:
            return page[9].text.split(' / ')[num][:slice_end]
    except IndexError:
        return 'отсутствует'
    except Exception:
        return 'отсутствует'


def check_exception(page, num):
    try:
        if page[num].text:
            return page[num].text
    except IndexError:
        return 'отсутствует'
    except Exception:
        return 'отсутствует'


def get_field_value(page_body, field):
    for i in range(len(page_body)):
        if field in page_body[i].text:
            return page_body[i + 1].text
    return 'отсутствует'


def get_cars_by_brand(brand, start, end):
    car_dict_list = []

    print('start: ', datetime.datetime.now())

    try:
        for i in range(start, end):
            response = requests.get(f'https://auto.ru/moskva/cars/{brand}/used/?output_type=list&page={i}')
            print('i = ', i)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'lxml')
            links = soup.find_all('a', class_='Link ListingItemTitle-module__link')

            for link in links:
                response = requests.get(link.get('href'))
                response.encoding = 'utf-8'
                car_page = BeautifulSoup(response.text, 'html.parser')

                page_head = car_page.find_all('div', class_='CardBreadcrumbs__item')

                page_body = car_page.find_all('span', class_='CardInfoRow__cell')

                page_price = car_page.find_all('span', class_='OfferPriceCaption__price')

                car_dict_list.append({
                        'bodyType'            : check_exception(page_body, 5),
                        'brand'               : check_exception(page_head, 1)[:-1],
                        'car_url'             : link.get('href'),
                        'color'               : check_exception(page_body, 7),
                        'complectation_dict'  : 'отсутствует',
                        'description'         : 'отсутствует',
                        'engineDisplacement'  : check_exception_list(page_body, 0, -2, 1),
                        'enginePower'         : check_exception_list(page_body, 1, -5, 1), #page_body[9].text.split('/ ')[1][:-6]
                        'equipment_dict'      : 'отсутствует',
                        'fuelType'            : check_exception_list(page_body, 2, -2, 0),
                        'image'               : 'отсутствует',
                        'mileage'             : check_exception(page_body, 3),
                        'modelDate'           : 'отсутствует',
                        'model_info'          : 'отсутствует',
                        'model_name'          : check_exception(page_head, 2),
                        'name'                : check_exception(page_head, 5),
                        'numberOfDoors'       : 'отсутствует',
                        'parsing_unixtime'    : 'отсутствует',
                        'priceCurrency'       : 'RUB',
                        'productionDate'      : check_exception(page_body, 1),
                        'sell_id'             : 'отсутствует',
                        'super_gen'           : 'отсутствует',
                        'vehicleConfiguration': 'отсутствует',
                        'vehicleTransmission' : get_field_value(page_body, 'Коробка'),
                        'vendor'              : 'отсутствует',
                        'Владельцы'           : get_field_value(page_body, 'Владельцы'),
                        'Владение'            : get_field_value(page_body, 'Владение'),
                        'Привод'              : get_field_value(page_body, 'Привод'),
                        'ПТС'                 : get_field_value(page_body, 'ПТС'),
                        'Руль'                : get_field_value(page_body, 'Руль'),
                        'Состояние'           : get_field_value(page_body, 'Состояние'),
                        'Таможня'             : get_field_value(page_body, 'Таможня'),
                        'price': check_exception(page_price, 1) #check_exception_list(page_price, 1, -2, 1)#page_price[1].text[:-2]
                })

    except IndexError():
        print('end: ', datetime.datetime.now())
        df = pd.DataFrame(car_dict_list)
        with open(f'D:\\cars\\{brand}_{start}_{end}.csv', 'w') as f:
            df.to_csv(f'D:\\cars\\{brand}_{start}_{end}.csv',  ';', encoding = 'utf-8')

    print('end: ', datetime.datetime.now())
    df = pd.DataFrame(car_dict_list)
    with open(f'D:\\cars\\{brand}_{start}_{end}.csv', 'w') as f:
        df.to_csv(f'D:\\cars\\{brand}_{start}_{end}.csv', ';', encoding = 'utf-8')


brand_list = ['bmw', 'volkswagen', 'nissan', 'mercedes', 'toyota', 'audi', 'mitsubishi', 'skoda', 'volvo', 'honda',
              'infiniti', 'lexus']
# brand_list = ['lexus']
for brand in brand_list:
    print('======', brand, '===============')
    thread_1 = Thread(target=get_cars_by_brand, args=(brand, 1, 10))
    thread_2 = Thread(target=get_cars_by_brand, args=(brand, 10, 20))
    thread_3 = Thread(target=get_cars_by_brand, args=(brand, 20, 30))
    thread_4 = Thread(target=get_cars_by_brand, args=(brand, 40, 50))
    thread_5 = Thread(target=get_cars_by_brand, args=(brand, 50, 60))
    thread_6 = Thread(target=get_cars_by_brand, args=(brand, 60, 70))
    thread_7 = Thread(target=get_cars_by_brand, args=(brand, 70, 80))
    thread_8 = Thread(target=get_cars_by_brand, args=(brand, 80, 90))
    thread_9 = Thread(target=get_cars_by_brand, args=(brand, 90, 100))
    thread_10 = Thread(target=get_cars_by_brand, args=(brand, 100, 110))
    thread_11 = Thread(target=get_cars_by_brand, args=(brand, 110, 120))
    thread_12 = Thread(target=get_cars_by_brand, args=(brand, 120, 130))
    thread_13 = Thread(target=get_cars_by_brand, args=(brand, 130, 140))
    thread_14 = Thread(target=get_cars_by_brand, args=(brand, 140, 150))
    thread_15 = Thread(target=get_cars_by_brand, args=(brand, 150, 160))
    thread_16 = Thread(target=get_cars_by_brand, args=(brand, 160, 170))
    thread_17 = Thread(target=get_cars_by_brand, args=(brand, 170, 180))
    thread_18 = Thread(target=get_cars_by_brand, args=(brand, 180, 190))
    thread_19 = Thread(target=get_cars_by_brand, args=(brand, 190, 200))
    thread_20 = Thread(target=get_cars_by_brand, args=(brand, 200, 210))
    thread_21 = Thread(target=get_cars_by_brand, args=(brand, 210, 220))
    thread_22 = Thread(target=get_cars_by_brand, args=(brand, 220, 230))
    thread_23 = Thread(target=get_cars_by_brand, args=(brand, 230, 240))
    thread_24 = Thread(target=get_cars_by_brand, args=(brand, 240, 250))



    thread_1.start()
    thread_2.start()
    thread_3.start()
    thread_4.start()
    thread_5.start()
    thread_6.start()
    thread_7.start()
    thread_8.start()
    thread_9.start()
    thread_10.start()
    thread_11.start()
    thread_12.start()
    thread_13.start()
    thread_14.start()
    thread_15.start()
    thread_16.start()
    thread_17.start()
    thread_18.start()
    thread_19.start()
    thread_20.start()
    thread_21.start()
    thread_22.start()
    thread_23.start()
    thread_24.start()

    thread_1.join()
    thread_2.join()
    thread_3.join()
    thread_4.join()
    thread_5.join()
    thread_6.join()
    thread_7.join()
    thread_8.join()
    thread_9.join()
    thread_10.join()
    thread_11.join()
    thread_12.join()
    thread_13.join()
    thread_14.join()
    thread_15.join()
    thread_16.join()
    thread_17.join()
    thread_18.join()
    thread_19.join()
    thread_20.join()
    thread_21.join()
    thread_22.join()
    thread_23.join()
    thread_24.join()

files = os.listdir('D:\\cars')
print(files)
df_all = pd.read_csv(f'D:\\cars\\{files[0]}', sep=';', encoding='utf-8')

print(df_all)

for file in files:
    print(file)
    df = pd.read_csv(f'D:\\cars\\{file}', sep=';')
    # print(df)
    df_all = pd.concat([df_all, df])

with open(f'D:\\all_cars.csv', 'w') as f:
    df_all.to_csv(f'D:\\all_cars.csv', sep=';', encoding='utf-8')

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 11900)
print(pd.read_csv('D:\\all_cars.csv', sep=';', encoding='utf-8'))
