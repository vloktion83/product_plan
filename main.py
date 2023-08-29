# задача кода:
#     получить теущие заявки
#     получить текущие остатки
#     получить текущие планы производства
#     учесть огрничения в ресурсах и подобрать оптимальный план призводства

# ОГРАНИЧЕНИЯ:
# код не учитывает есть ли на складе назафасованная продукция. Возможна ситуация, когда нужно не произвести,
# а зафасовать, Но это повлечет за собой в логике программы ограничения производственных ресурсов.

# загрузка необходимых библиотек
import pickle
import numpy as np
import pandas as pd
import os as os
import math
import time
import datetime

from datetime import datetime, timedelta
import random
random.seed()
import copy
# import multiprocessing
# from ordered_set import OrderedSet

# настройка блокнота
# %config InteractiveShell.ast_node_interactivity='all'
pd.set_option('display.max_rows', 550)
pd.set_option('display.max_columns', 150)

# Устанавливаем формат отображения для чисел в numpy
np.set_printoptions(precision=5, suppress=True)

# Устанавливаем формат отображения для чисел в pandas
pd.options.display.float_format = '{:.2f}'.format
pd.options.display.float_format = '{:,.0f}'.format


# отключение уведомлений о небольших ошибках
import warnings
warnings.filterwarnings('ignore')

# задаем список дней когда планируем не работать
# dd-mm-YYYY
stop_days=[]
holidays=['01-01-2024', '02-01-2024', '03-01-2024', '04-01-2024', '05-01-2024', '06-01-2024', '07-01-2024', '08-01-2024',
         '12-01-2024', '23-02-2024', '08-03-2024']
stop_days.extend(holidays)
print(stop_days)


# Создание функции для загрузки Excel:
#
def load_excel(file_path):
    xls = pd.ExcelFile(file_path)
    sheet_names = xls.sheet_names

    dataframes = {}

    for sheet_name in sheet_names:
        dataframes[sheet_name] = xls.parse(sheet_name).fillna(0)

    return dataframes


# Создание функции для конвертации словаря в глобальные переменные
def dict_to_globals(dataframes):
    for key in dataframes.keys():
        globals()[key] = dataframes[key]
        print(key)

file_path = r'C:\Users\lvd\YandexDisk\работа\росторгуевский\планирование производства\исходные данные для планирования1.xlsx'
dfs = load_excel(file_path)
dict_to_globals(dfs)


# функция принимает на вход первую и последню дату встреченную в входных данных
def create_date_columns(start_date, last_date):
    desired_format = '%d-%m-%Y'
    date_columns = []
    new_day = start_date
    while new_day != last_date + timedelta(days=1):
        date_columns.append(new_day)
        new_day = new_day + timedelta(days=1)

    formatted_dates = [date.strftime(desired_format) for date in date_columns]

    return formatted_dates


def initialize_resource_need(resources, start_date, last_date):
    df = pd.DataFrame()
    df["Рабочий центр"] = resources

    date_cols = create_date_columns(start_date, last_date)
    for col in date_cols:
        df[col] = None

    return df

# устанавливаем формат даты для преобразований из заголовкой
date_format = '%d.%m.%Y %H:%M:%S'

# находим саммую раннюю дату
first_date_orders=datetime.strptime(dfs['orders'].columns[2], date_format)
first_date_prod=datetime.strptime(dfs['production_plan'].columns[2], date_format)
start_date=datetime.today()
start_date=min(start_date,first_date_prod,first_date_orders)


# находим последнюю дату
num_columns_orders=len(dfs['orders'].columns)
num_columns_prod=len(dfs['production_plan'].columns)

last_date_order=datetime.strptime(dfs['orders'].columns[num_columns_orders-1], date_format)+timedelta(days=1)
last_date_production_plan=datetime.strptime(dfs['production_plan'].columns[num_columns_prod-1], date_format)+timedelta(days=2)
last_date=max(last_date_order,last_date_production_plan)


resources = wc_req.iloc[:,0]

dfs['resource_need'] = initialize_resource_need(resources, start_date, last_date)


# Функция форматирования даты
def format_date(date_str):
    date_obj = pd.to_datetime(date_str, format='%d.%m.%Y %H:%M:%S')
    return date_obj.strftime('%d-%m-%Y')


# Цикл по датафреймам
for key, df in dfs.items():

    # Цикл по колонкам
    for col in df.columns:

        # Попытка отформатировать как дату
        try:
            formatted = format_date(col)
            df.rename(columns={col: formatted}, inplace=True)

        except:
            pass

        # Сохранение датафрейма
        dfs[key] = df

# заменим в стоках колонку со словами на первую дату
dfs['stock'].rename(columns={'Количество (в ед. отчетов)': start_date.strftime('%d-%m-%Y')}, inplace=True)

# Добавляем инициализацию

desired_format = '%d-%m-%Y'
date_columns = create_date_columns(start_date, last_date)
formatted_dates = date_columns


# Функция добавления недостающих дат
def add_missing_dates(df, dates):
    dates = set(dates)
    start_cols = df.columns.difference(dates)
    existing = set(set(df.columns) - set(start_cols))
    missing = dates - existing
    updated = list(existing) + list(missing)
    df = df.reindex(columns=list(start_cols) + list(formatted_dates)).fillna(0)
    return df


# Добавление дат для каждого датафрейма
for key in ['orders', 'production_plan', 'stock']:
    df = dfs[key]
    dfs[key] = add_missing_dates(df, formatted_dates)
#     dfs[key]=dfs[key]=dfs[key]
#     dfs[key].head(3)

# переименуем первый столбец в wс_req в Вид произвосдтва

dfs['wc_req'].rename(columns={wc_req.columns[0]: 'Рабочий центр'}, inplace=True)


# сортирвка датафреймов по именам рабочих центров
dfs['wc_req']=dfs['wc_req'].sort_values(by='Рабочий центр')
dfs['wc_recource']=dfs['wc_recource'].sort_values(by='Рабочий центр')

all_nomenclature = set()
# получим множество всей номенклатуры в списках с номенклатурой

for key in ['orders', 'stock', 'production_plan', 'week_history']:
    all_nomenclature.update(list(dfs[key]['Номенклатура']))

# Создаем словарь для хранения соответствия номенклатуры и вида производства
nomenclature_to_production = {}


# Заполняем словарь значениями из DataFrame production_plan и ордерс
def fill_product_dict(df, nomenclature):
    product_dict = {}

    for name in nomenclature:
        try:
            product_type = df[df['Номенклатура'] == name]['Вид производимой продукции'].iloc[0]
            product_dict[name] = product_type
        except:
            pass

    return product_dict


nomenclature_to_production = fill_product_dict(dfs['orders'], all_nomenclature)
nomenclature_to_production.update(fill_product_dict(dfs['stock'], all_nomenclature))
nomenclature_to_production.update(fill_product_dict(dfs['production_plan'], all_nomenclature))
nomenclature_to_production.update(fill_product_dict(dfs['week_history'], all_nomenclature))

len(nomenclature_to_production)

# Функция для добавления недостающих строк с номенклатурой
def add_missing_rows(df, nomenclature_list):
    existing_nomenclature = set(df['Номенклатура'].values)
    missing_nomenclature = list(set(nomenclature_list) - existing_nomenclature)
    new_rows = [{'Номенклатура': nomenclature,'Вид производимой продукции':nomenclature_to_production.get(nomenclature, 0)}
                for nomenclature in missing_nomenclature]
    df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    return df


# Цикл по датафреймам
for sheet in ['stock', 'orders', 'production_plan','week_history']:
    # Добавление недостающих строк в датафрейм
    dfs[sheet] = add_missing_rows(dfs[sheet], all_nomenclature).fillna(0)
    # Попытка удалить колонку "Вид производства"
    try:
        dfs[sheet].drop(columns='Вид производства', inplace=True)
    except:
        pass

# очищаем датафреймы от паразитного 0
all_nomenclature.discard(0)

for df in dfs:
    print(df)
    try:
        dfs[df] = dfs[df][dfs[df]['Номенклатура'] != 0]
        dfs[df].drop(columns='Вид производимой продукции',inplace=True)
    except:
        pass

all_nomenclature=pd.Series(list(all_nomenclature))
all_nomenclature.sort_values(inplace=True)

# сортировака номенклатуры

for df in dfs:

    try:
        dfs[df].sort_values(by='Номенклатура', inplace=True)
        dfs[df].reset_index(drop=True, inplace=True)
        print(f'в датафрейме {df} Номенклатура отсортирована')
    except:
        print(f'в датафрейме {df} нет колонки Номенклатура')
        pass

dates=pd.Series(formatted_dates) # все даты какие есть

# Заполняем остатки в будущих днях в сооствествии с текущими остатками, планами производства и заказами
# т.е. последний день в таблице не будем плинировать производство
# Цикл по датам, не берем в расчет последнюю дату, так как невозмодно посчитать остатки для даты на день позже

# процедура которая получает датафреймы с остатками, заказами и планом производства
# и отталкиваясь от начального остатка рассчитывает остатки для всех дат с уветом заказов и производства

def count_stock(stock, orders, production_plan, dates):
    for date_i in range(len(dates)-1):
        # Получаем текущие заказы, остатки и планы производства для данной даты
        current_orders = np.array(orders[dates[date_i]].tolist())

        current_stock = np.array(stock[dates[date_i]].tolist())
        # Проверяем, есть ли прошлый план производства для данной даты
        if date_i >= 1:
            current_plan = np.array(
                production_plan[dates[date_i-1]].tolist())

        # Если прошлого плана нет, заполняем его нулями
        else:
            current_plan = np.array([0] * len(current_orders))
        # если есть отрицательный остаток, то для расчета остатков завтрашнего дня примем его равным 0
        current_stock = np.where(current_stock < 0, 0, current_stock)
        # Вычисляем новые остатки на следующую дату
        stock_i = current_stock-current_orders+current_plan

        # Проверяем, не является ли текущая дата последней
        if dates[date_i] != dates.iloc[-1]:

            # Обновляем остатки на следующую дату
            stock[dates[date_i+1]] = stock_i
    return stock


dfs['stock']=count_stock(dfs['stock'], dfs['orders'], dfs['production_plan'], dates)

# создаем словарь с видами производства, а значение - список номенклатуры

type_to_names={}

for key,value in nomenclature_to_production.items():
    if value not in type_to_names:
        type_to_names[value]=[key]
    else:
        type_to_names[value].append(key)

prod_plan = dfs['production_plan']  # Загружаем DataFrame с планами производства
wc_req = dfs['wc_req']  # Загружаем DataFrame с требованиями к рабочим центрам
all_wc_needs = []  # Создаем пустой список для хранения всех wc_needs

# Перебираем ключи и значения словаря type_to_names
for prod_type, nomenclatures in type_to_names.items():
    if prod_type != 0:
        # Получаем коэффициенты использования рабочих центров для данного prod_type
        wc_coefficients = wc_req[prod_type]
        wc_coefficients = np.array(wc_coefficients)

        # Создаем Series для индексации DataFrame prod_plan
        nomenclature_filter = prod_plan['Номенклатура'].isin(nomenclatures)

        # Суммируем планы производства для номенклатур данного типа
        plan_sum = prod_plan[nomenclature_filter].sum(axis=0)
        plan_sum = np.array(plan_sum[1:])

        # Рассчитываем потребности в ресурсах на каждый день

        wc_needs = np.outer(plan_sum, wc_coefficients).T
        all_wc_needs.append(wc_needs)

# Суммируем все рассчитанные значения для получения итоговой потребности в ресурсах
result_array = np.zeros_like(all_wc_needs[0])
for array in all_wc_needs:
    result_array += array

# Создаем DataFrame для хранения итоговой потребности в ресурсах
raws = wc_req['Рабочий центр']
columns = prod_plan.columns[1:]
# создадим датафрейм и добавим в него массив с вычисленными потребностями
dfs['resource_need'] = pd.DataFrame(result_array, columns=columns)
dfs['resource_need'].insert(0, 'Рабочий центр', raws.values)  # вставим в начало колонку с рабочими центрами


def ABC_XYZ(week_history):
    #     определяем коэффициент запасов в зависимости от категории
    abcxyz_to_coef = {'AX': 1.2, 'BX': 1.5, 'CX': 1.5, 'AY': 1.3, 'BY': 1.5, 'CY': 2, 'AZ': 0.5, 'BZ': 0.6, 'CZ': 0}

    col_num = week_history.iloc[:, 2:].count().count()
    # Создание новой колонки с суммой продаж
    week_history['Total_Sales'] = week_history.iloc[:, 2:].sum(axis=1)

    # Сортировка датафрейма по сумме продаж
    week_history_sorted = week_history.sort_values(
        by='Total_Sales', ascending=False)

    # Добавление колонки с нарастающим итогом
    week_history_sorted['Cumulative_Sales'] = week_history_sorted['Total_Sales'].cumsum()

    # Создание словаря для хранения XYZ групп
    xyz_groups = {}
    name_to_mean = {}

    # Проход по каждой строке (номенклатуре) в датафрейме week_history
    for index, row in week_history_sorted.iterrows():
        nomenclature = row["Номенклатура"]
        weekly_sales = row[2:col_num]  # Продажи за каждую из 10 недель
        #     print(weekly_sales)

        # Находим коэффициент вариации
        mean_sales = np.mean(weekly_sales)
        std_deviation = np.std(weekly_sales)
        coefficient_of_variation = (std_deviation / mean_sales) * 100
        #     print(mean_sales,std_deviation,coefficient_of_variation)
        # Определяем XYZ группу
        if coefficient_of_variation < 60:
            xyz_group = "X"
        elif coefficient_of_variation < 100:
            xyz_group = "Y"
        else:
            xyz_group = "Z"

        xyz_groups[nomenclature] = xyz_group
        name_to_mean[nomenclature] = mean_sales
    #     if coefficient_of_variation!=100:
    #         print(row)
    #         print(mean_sales,std_deviation,coefficient_of_variation)

    # Определение ABC группы
    total_sales_sum = week_history_sorted['Total_Sales'].sum()
    total_sales_threshold_A = total_sales_sum * 0.8
    total_sales_threshold_B = total_sales_sum * 0.95

    abc_groups = {}

    for index, row in week_history_sorted.iterrows():
        cumulative_sales = row['Cumulative_Sales']
        nomenclature = row['Номенклатура']

        if cumulative_sales < total_sales_threshold_A:
            abc_group = 'A'
        elif cumulative_sales < total_sales_threshold_B:
            abc_group = 'B'
        else:
            abc_group = 'C'

        abc_groups[nomenclature] = abc_group
    abcxyz = {}
    for name in abc_groups:
        abcxyz[name] = abc_groups[name] + xyz_groups[name]

    week_history_sorted['XYZ Группа'] = week_history_sorted['Номенклатура'].map(xyz_groups)
    week_history_sorted['ABC Группа'] = week_history_sorted['Номенклатура'].map(abc_groups)

    week_history_sorted['mean_sell'] = week_history_sorted['Номенклатура'].map(name_to_mean)
    week_history_sorted['ABC_XYZ'] = week_history_sorted['Номенклатура'].map(abcxyz)
    week_history_sorted['stock-coef'] = week_history_sorted['ABC_XYZ'].map(abcxyz_to_coef)
    week_history_sorted['transit_stock'] = week_history_sorted['stock-coef'] * week_history_sorted['mean_sell']

    #     display (week_history_sorted)
    # Печать/Вывод DataFrame в CSV файл с индексом
    week_history_sorted.to_excel('DataFrame_index1.xlsx', index=False)  # Сохранение в формате XLSX

    #     week_history_sorted.to_excel('DataFrame_index.xls')
    return week_history_sorted


# # Добавление XYZ и ABC групп в датафрейм
# week_history_sorted['XYZ Группа'] = week_history_sorted['Номенклатура'].map(xyz_groups)
# week_history_sorted['ABC Группа'] = week_history_sorted['Номенклатура'].map(abc_groups)

# Вывод результата
# display(week_history_sorted)
transit_stock = ABC_XYZ(week_history)
transit_stock = transit_stock[['Номенклатура', 'transit_stock']]

transit_stock=add_missing_rows(transit_stock, all_nomenclature).drop('Вид производимой продукции',axis=1)
transit_stock.sort_values(by='Номенклатура',inplace=True)
# transit_stock['Вид производимой продукции']=transit_stock['Номенклатура'].apply(nomenclature_to_production)
transit_stock.fillna(0,inplace=True)
# nomenclature_to_production['Блины с картофелем и грибами 0,35 кг']


dfs['transit_stock']=transit_stock

orders=dfs['orders']
stock=dfs['stock']
production_plan=dfs['production_plan']
wc_recource=dfs['wc_recource']
wc_req=dfs['wc_req']
transit_stock=dfs['transit_stock']
resource_need=dfs['resource_need']


# функция, которая будет возвращать тип дня (рабочий или выходной) на основе переданной даты

def get_day_type(date_str):
    date_obj = datetime.strptime(date_str, '%d-%m-%Y')
    day_of_week = date_obj.weekday()  # Возвращает день недели (0 - понедельник, 6 - воскресенье)

    return day_of_week


def calculate_fitness_equipment_overloads(wc_resource, resource_need):
    total_penalty = 0

    capacity = wc_resource['Мощность'].values
    resource_need_values = resource_need.iloc[:, 1:].values

    overload = (resource_need_values - capacity.reshape((-1, 1)))
    overload = np.where(overload < 0, 0, overload)
    overload_percent = ((resource_need_values - capacity.reshape((-1, 1))) / capacity.reshape((-1, 1))) * 100
    overload_percent = np.where(overload_percent < 0, 0, overload_percent)
    total_penalty = np.sum(overload * overload_percent)
    return total_penalty


print(calculate_fitness_equipment_overloads(wc_recource, resource_need))
# calculate_fitness_equipment_overloads(wc_recource, best.resource_need)
# print(pd.merge(resource_need,wc_recource))
# wc_recource)
def calculate_fitness_storage_cost(stock):
    over_pen = []
    stor_pen = []
    overage_penalty_per_three_tons = 3000
    storage_penalty_per_kilogram = 0.1
    total_penalty = 0
    stock_limit=30000

    stock_a_day = stock.sum(axis=0)[1:]
    prev_date = formatted_dates[0]

    for i, date in enumerate(formatted_dates[1:], start=1):
        current_stock = stock_a_day[date]
        previous_stock = stock_a_day[prev_date]

        if previous_stock > stock_limit and current_stock > previous_stock:
            overage = current_stock - previous_stock
            overage_in_three_tons = math.floor(overage / 3000)
            total_penalty += overage_in_three_tons * overage_penalty_per_three_tons
            over_pen.append([date, current_stock, previous_stock,
                            overage_in_three_tons, overage_in_three_tons * overage_penalty_per_three_tons])
        if current_stock > 0:
            total_penalty += current_stock * storage_penalty_per_kilogram
            stor_pen.append([date, current_stock, current_stock *
                            storage_penalty_per_kilogram, total_penalty])

        prev_date = date
    stor_df = pd.DataFrame(
        stor_pen, columns=['date', 'остаток', 'стоимость хранения', 'общий штраф'])
    over_df = pd.DataFrame(over_pen, columns=[
                           'date', 'остаток сегоня', 'остаток вчера', 'превышение 3тонны', 'штраф'])
#     display(stor_df)
#     display(over_df)
    return total_penalty
calculate_fitness_storage_cost(stock)


def calculate_fitness_order_completion(stock):
    penalty_per_kilogram = 60
    total_penalty = 0

    stock = np.array(stock[dates])
    total_penalty = abs(np.sum(stock[stock < 0] * penalty_per_kilogram))
    #     total_penalty=abs(np.sum(stock[stock<0]*penalty_per_kilogram))
    #     negative_sum = abs(stock[dates][stock[dates] < 0].sum().sum())
    #     total_penalty += negative_sum * penalty_per_kilogram
    #     penalty_df = pd.DataFrame(columns=['Дата', 'Штраф'])

    #     for date in dates:
    #         daily_penalty = abs(stock[date][stock[date] < 0].sum()) * penalty_per_kilogram
    #         penalty_df = penalty_df.append({'Дата': date, 'Штраф': daily_penalty}, ignore_index=True)
    # #     print(f'невыполнение заказов {negative_sum}')
    # #     print(f'Штраф {total_penalty}')
    #     display(penalty_df)
    return total_penalty


calculate_fitness_order_completion(dfs['stock'])


# Предполагаем, что у вас есть переменные stock, transit и dates

def calculate_fitness_minstock_completion(stock, transit):
    penalty_per_kilogram = 0.3

    ostatok_array = transit['transit_stock'].values
    stock_array = np.array(stock[dates])
    transit_stock_array = np.subtract(stock_array, ostatok_array[:, np.newaxis])

    # Вычисляем штрафы за каждый день
    daily_penalties = abs(np.where(transit_stock_array > 0, 0, transit_stock_array) * penalty_per_kilogram)

    # Суммируем штрафы за все дни
    total_penalty = np.sum(daily_penalties)

    return total_penalty


calculate_fitness_minstock_completion(dfs['stock'], dfs['transit_stock'])


def calculate_fitness_minimize_product_variety(production_plan):
    exponent = 2.4  # Показатель степени
    production_plan = np.array(production_plan[dates])
    # Считаем количество видов продукции в каждом дне
    product_variety_per_day = np.where(production_plan > 0, 1, 0)
    product_variety_per_day = np.sum(product_variety_per_day, axis=0)
    penalty_per_day = exponent ** product_variety_per_day
    penalty = np.sum(penalty_per_day)
    #     display(penalty_per_day)

    return penalty


calculate_fitness_minimize_product_variety(dfs['production_plan'])


def total_production_cost(production_plan, stop_days):
    regular_work_cost = 30000  # Стоимость работы в обычный рабочий день
    weekend_work_cost = 100000  # Стоимость работы в выходной день
    stop_day_cost = 1000000
    pen = []
    total_cost = 0
    for date in dates:
        production_volume = production_plan[date].sum()  # Объем выпуска продукции в этот день
        day = get_day_type(date)

        if day > 5:
            total_cost += (production_volume > 0) * weekend_work_cost
        else:
            total_cost += (production_volume > 0) * regular_work_cost

        if date in stop_days:
            total_cost += stop_day_cost

    return total_cost


total_production_cost(production_plan, stop_days)


def random_change_prod(production_plan, change=100, chance=0.5):
    prod = production_plan.copy()
    prod.iloc[:, 1:] = prod.iloc[:, 1:].applymap(
        lambda num: random_change(num, change, chance))
    return prod


# Функция для случайного изменения числа


def random_change(num, change, chance):
    if num == 0:
        if random.randint(0, 100) / 100 < chance / 2:
            num = random.randint(0, change)

    if random.randint(0, 100) / 100 < chance:
        num += random.randint(0, min(int(num), int(change)))

    return num


def random_generate(num, change=4000, chance=1 / 13):
    if random.randint(0, 100) / 100 < chance:
        num = random.randint(0, change)
    else:
        num = 0
    return num


def product_plan_generate(production_plan, change=100, chance=0.1):
    prod = production_plan.copy()
    prod[prod.columns[1:]] = prod.iloc[:, 1:].applymap(lambda num: random_generate(num))
    return prod


new_plan = product_plan_generate(dfs['production_plan'])

# product_plan_generate(production_plan)


production_plan_suspect = random_change_prod(
    production_plan, change=1000, chance=0.1)


def calculate_fitness_production_changes(production_plan, production_plan_suspect):
    penalty_multiplier = 0.5
    current_day_penalty = 100
    tomorrow_penalty = 30

    # Преобразование датафреймов в массивы numpy
    plan_array = production_plan.iloc[:, 1:].to_numpy()
    suspect_array = production_plan_suspect.iloc[:, 1:].to_numpy()

    # Вычисление модуля разности массивов
    difference_array = np.abs(suspect_array - plan_array)
    #     штрафы за изменения в 1-2 дни
    extra_penalty = (difference_array[:, 0] * current_day_penalty)  # штраф за изменения в текущем дне
    extra_penalty += (difference_array[:, 1] * tomorrow_penalty)  # штраф за изменения в текущем дне
    # Умножение на коэффициент штрафа
    penalty_array = difference_array * penalty_multiplier
    # Вычисление общего штрафа

    total_penalty = np.sum(penalty_array) + np.sum(extra_penalty)
    #     print(f'Штраф за изменения в плане производства: {total_penalty}')
    #     display(pd.DataFrame(difference_array))
    return total_penalty


# Пример использования
penalty=calculate_fitness_production_changes(production_plan, production_plan_suspect)
print(f'Штраф за изменения в плане производства: {penalty}')


# начнем оформлять фитнессфункции

def calculate_total_fitness(stock, orders, transit_stock, initial_production_plan, production_plan_suspect, wc_recource,
                            resource_need):
    fitness_order_completion = calculate_fitness_order_completion(stock)
    fitness_minimal_stock = calculate_fitness_minstock_completion(
        stock, transit_stock)
    fitness_storage_cost = calculate_fitness_storage_cost(stock)
    fitness_minimize_product_variety = calculate_fitness_minimize_product_variety(
        production_plan_suspect)
    fitness_minimize_equipment_overloads = calculate_fitness_equipment_overloads(
        wc_recource, resource_need)
    fitness_total_production_cost = total_production_cost(production_plan_suspect, stop_days)
    fitness_production_plan_change = calculate_fitness_production_changes(
        initial_production_plan, production_plan_suspect)
    # Дополните код для остальных критериев
    total_fitness = []
    total_fitness.append(fitness_order_completion)
    total_fitness.append(fitness_minimal_stock)
    total_fitness.append(fitness_storage_cost)
    total_fitness.append(fitness_minimize_product_variety)
    total_fitness.append(fitness_minimize_equipment_overloads)
    total_fitness.append(fitness_total_production_cost)
    total_fitness.append(fitness_production_plan_change)

    # Дополните код для остальных критериев
    total_fitness_sum = pd.Series(total_fitness).sum()
    total_fitness.append(total_fitness_sum)

    return total_fitness


# Расчет фитнеса
total_fitness = calculate_total_fitness(
    stock, orders, transit_stock, production_plan, production_plan_suspect, wc_recource, resource_need)
print("Total Fitness:", *[f"{x:,.0f}  " for x in total_fitness])
print(calculate_total_fitness(
    stock, orders, transit_stock, production_plan, production_plan, wc_recource, resource_need))

# type_to_raw - словарьв котором ключ - тип продукции, а возвращает списко индексов
# Сначала сортируем серию, чтобы ее индексы были как в датафреймах
all_nomenclature.reset_index(drop=True, inplace=True)

# Создаем словарь, в котором ключ - тип, значение - список индексов
type_to_raw = {}

for typ in type_to_names.keys():
    # Получаем индексы номенклатур данного типа
    list_index = list(all_nomenclature.loc[all_nomenclature.isin(type_to_names[typ])].index)
    # Сохраняем список индексов в словаре
    type_to_raw[typ] = list_index
# type_to_raw - словарьв котором ключ - тип продукции, а возвращает списко индексов

# Функция генерирует случайный план производства с заданными параметрами

def generate_random_prod_plan(all_nomenclature, dates, min=0, max=5000, step=100, chance=1/14):
    # Создаем пустой DataFrame с колонкой "Номенклатура" и колонками дат
    df = pd.DataFrame()
    df['Номенклатура'] = all_nomenclature
    df[dates] = 0
    # Создаем массив случайных значений с заданными параметрами
    size_x = len(all_nomenclature)
    size_y = len(dates)
    chance_num = 1 / chance
#     маска значний с вероятностью *chance*
    i_shape = np.random.randint(chance_num, size=(
        size_x, size_y)) // (chance_num - 1)

    individual = np.random.randint(max, size=(size_x, size_y))
    individual = individual // step * step
    individual = individual * i_shape

    # Заполняем значения в DataFrame
    df.iloc[:, 1:] = individual

    return df


# Генерируем случайные данные с заданными параметрами
generate_random_prod_plan(all_nomenclature, dates, step=20, min=0, max=5000)


# мы создадит класс индивид
# в нем будут методы обращения к плану производства и формированию стоков и потребности в ресурсах.
# создадим класс особь - Individual

class Individual:
    def __init__(self, production_plan, stock, orders):
        self.production_plan = production_plan
        self.stock = stock.copy()
        self.fill_stock(orders)
        #         self.resource_need = [123]
        self.fill_resource_need()
        self.fitness = None
        self.count_fitness()

    def recalc(self):
        #         self.production_plan = production_plan
        self.fill_stock(orders)
        self.fill_resource_need()
        self.count_fitness()

    #  нужно векторизировать
    def fill_stock(self, orders):

        for date_i in range(0, len(dates) - 1):
            # Получаем текущие заказы, остатки и планы производства для данной даты
            current_orders = np.array(orders[dates[date_i]].tolist())
            current_stock = np.array(self.stock[dates[date_i]].tolist())
            # Проверяем, есть ли прошлый план производства для данной даты
            if date_i >= 1:
                current_plan = np.array(
                    self.production_plan[dates[date_i - 1]].tolist())
            # Если прошлого плана нет, заполняем его нулями
            else:
                current_plan = np.array([0] * len(current_orders))

            current_stock = np.where(current_stock < 0, 0, current_stock)

            # Вычисляем новые остатки на следующую дату
            stock_i = current_stock - current_orders + current_plan

            # Обновляем остатки на следующую дату
            self.stock[dates[date_i + 1]] = stock_i

    def fill_resource_need(self):
        prod_plan = self.production_plan  # Загружаем DataFrame с планами производства
        # Загружаем DataFrame с требованиями к рабочим центрам
        wc_req = dfs['wc_req']
        all_wc_needs = []  # Создаем пустой список для хранения всех wc_needs
        # Перебираем ключи и значения словаря type_to_names
        for prod_type, nomenclatures in type_to_names.items():
            if prod_type != 0:
                # Получаем коэффициенты использования рабочих центров для данного prod_type
                wc_coefficients = wc_req[prod_type]
                wc_coefficients = np.array(wc_coefficients)

                # Создаем Series для индексации DataFrame prod_plan
                nomenclature_filter = prod_plan['Номенклатура'].isin(
                    nomenclatures)

                # Суммируем планы производства для номенклатур данного типа
                plan_sum = prod_plan[nomenclature_filter].sum(axis=0)
                plan_sum = np.array(plan_sum[1:])

                # Рассчитываем потребности в ресурсах на каждый день
                wc_needs = np.outer(plan_sum, wc_coefficients).T
                all_wc_needs.append(wc_needs)

        # Суммируем все рассчитанные значения для получения итоговой потребности в ресурсах
        result_array = np.zeros_like(all_wc_needs[0])
        for array in all_wc_needs:
            result_array += array

        # Создаем DataFrame для хранения итоговой потребности в ресурсах
        raws = wc_req['Рабочий центр']
        columns = prod_plan.columns[1:]
        # Создаем атрибут resource_need внутри self и сохраняем туда DataFrame с вычисленными потребностями

        self.resource_need = pd.DataFrame(result_array, columns=columns)
        # вставим в начало колонку с рабочими центрами
        self.resource_need.insert(0, 'Рабочий центр', raws.values)

    def count_fitness(self):  # возвращает кортеж с фитнесами, [7] - итоговый
        self.fitness = calculate_total_fitness(
            self.stock, orders, transit_stock, dfs['production_plan'], self.production_plan, wc_recource,
            self.resource_need)


# объявляем переменные для генетического алгоритма массивы данных
stock=dfs['stock']
orders=dfs['orders']
production_plan=dfs['production_plan']
# production_plan_suspect=generate_random_prod_plan(all_nomenclature, dates, step=20, min=0, max=5000)
transit_stock=dfs['transit_stock']
wc_recource=dfs['wc_recource']
population=[]


def try_round(x):
    try:
        x=x//1
        return x
    except:
        return x


# сортируем особи
def pop_sort(osobs, key):
    osobs.sort(key=lambda x: x.fitness[key], reverse=False)


def roulette_selection(population, num_parents=2):
    #     создадим вероятность выбора каждой в зависимости от фитнесса
    #      чем ниже фитнесс тем веорятней выбор
    fitness_values = 1 / (np.array([item.fitness[7] for item in population])) * 1000000000
    std = np.std(fitness_values)
    fitness_values = fitness_values / std
    total_fitness = np.sum(fitness_values)
    probabilities = (fitness_values) / total_fitness

    selected_indices = np.random.choice(
        len(population), size=num_parents, p=probabilities, replace=False)
    selected_parents = [copy.deepcopy(population[idx]) for idx in selected_indices]
    if len(selected_parents) != num_parents:
        selected_parents = roulette_selection(population, num_parents)
    #     здесь нужно использовать функцию вместо equal которая исключит разницу изза округлений
    if selected_parents[0].production_plan.equals(selected_parents[1].production_plan):
        selected_parents = roulette_selection(population, num_parents)

    return selected_parents


# написали код для рулетной выборки
# теперь нужен код для кроссовера

def create_boolean_matrix(rows, cols, true_count):
    if true_count > rows * cols:
        raise ValueError("Number of 'true' values cannot exceed matrix size.")

    matrix = np.full((rows, cols), False)
    flat_indices = np.random.choice(rows * cols, true_count, replace=False)
    row_indices, col_indices = np.unravel_index(flat_indices, (rows, cols))
    matrix[row_indices, col_indices] = True
    return matrix


# убедится что нет рисков ссылок которые меняют оба объекта
def crossover(parent1, parent2, crossover_rate=0.2):
    rows = len(all_nomenclature)
    columns = len(dates)
    #     crossover_rate =

    df1 = pd.DataFrame(parent1.production_plan)
    df2 = pd.DataFrame(parent2.production_plan)

    # Создаем булевую маску и заменяем значения в столбце 0 на True

    mask = create_boolean_matrix(
        rows, columns + 1, round(crossover_rate * rows * columns))
    mask[:, 0] = True

    # Выбираем значения из df1 и df2 в соответствии с маской
    df1_transit = copy.copy(df1)
    df1_transit_values = df1_transit.values

    df1_values = df1.values
    df1_values[mask] = df2.values[mask]

    df2_values = df2.values
    df2_values[mask] = df1_transit.values[mask]

    # Создаем новые DataFrame с обновленными значениями
    new_df1 = pd.DataFrame(df1_values, columns=df1.columns)
    new_df2 = pd.DataFrame(df2_values, columns=df2.columns)

    # Создаем новые экземпляры Individual с обновленными DataFrame
    child1 = Individual(new_df1, stock, orders)
    child2 = Individual(new_df2, stock, orders)

    return child1, child2


def mutation(osob):
    new_osob = copy.deepcopy(osob)  # Создаем копию особи
    df = new_osob.production_plan
    df_array1 = df.iloc[:, 1:]
    df_array = df_array1.applymap(lambda x: random_change_item(x))
    #     print(df_array1.sum())
    #     print(df_array.sum())

    df_array.fillna(0, inplace=True)
    df.iloc[:, 1:] = df_array
    new_osob.production_plan = df
    new_osob.recalc()
    return new_osob


# меняет поданное значение на mutation_rate % d большую или меньшую сторону
def random_change_item(item):
    #     print(item)
    if pd.isna(item):
        return 0
    else:
        item = round(item, 10)

    change = random.randint(0, mutation_rate * 100) / 100

    if item > 0:
        if random.randint(0, 100) / 100 <= mutation_chance:
            #             print(f'увеличим на {change}, было {item} станет {item * (1+change)//1}')
            return (item * (1 + change)) // 1
        elif random.randint(0, 100) <= mutation_chance / 4:
            #             print(f'изменим на 0, было {item} станет 0')
            return 0
        elif random.randint(0, 100) / 100 <= mutation_chance:
            #             print(f'уменьшим на {change}, было {item} станет {(item*(1-change))//1}')
            return (item * (1 - change)) // 1
        else:
            #             print(f'не меняем {item} ')
            return item

    elif item == 0:
        if random.randint(0, 100) < mutation_chance / 4:
            #             print(f'изменим 0 на  {(change*100*3)//1}')
            return (change * 100 * 3) // 1
    elif item < 0:
        print('*' * 30)
        #         print(f'удивительно, но было меньше 0, вернем 0')
        return 0
    elif pd.isna(item):
        print('*' * 30)
        #         print(f'удивительно, но было NaN, вернем 0')
        return 0
    else:
        #         print('ничегон не меняем')
        return 0


# Вычисляем средние и стандартные отклонения


def generate_value(median, std):
    value = round(np.random.normal(loc=median, scale=std))

    if (random.randint(0, 100) < mutation_chance / 2) or (value <= 0):
        value = round(np.random.normal(loc=median, scale=std))
    else:
        value = 0

    return max(value, 0)


# Генерация плана


def generate_random_prod_plan(nomenclature, dates):
    df = pd.DataFrame()
    df['Номенклатура'] = nomenclature
    #     medians = np.median(
    #         [ind.production_plan.iloc[:, 1:].values for ind in population[:len(population)]], axis=0)
    #     medians = np.mean(medians, axis=1)
    #     stds = np.std(
    #         [ind.production_plan.iloc[:, 1:].values for ind in population[:len(population)]], axis=0)
    #     stds = np.mean(stds, axis=1)
    medians = transit_stock['transit_stock']
    stds = orders.iloc[:,1:].max(axis=1)
    for row in range(len(nomenclature)):
        median = medians[row]
        std = stds[row]
        for col in dates:
            value = generate_value(median, std)
            df.at[row, col] = value

    return pd.DataFrame(df)


def tournament_selection(population, tournament_size=7):
    selected_parents = []
    fitness_values = get_fitness_values(population)
    max_pop = 2
    while len(selected_parents) < max_pop:
        tournament_indices = random.sample(
            range(len(population)), tournament_size)
        winner_index = min(tournament_indices, key=lambda i: fitness_values[i])
        selected_parents.append(population[winner_index])
    return selected_parents


def get_fitness_values(population):
    fitness_values = pd.Series(population)
    fitness_values = fitness_values.apply(lambda x: x.fitness[7])

    return fitness_values


def population_info(population, key=7):
    fitness = [individual.fitness[key] for individual in population]
    fitness = pd.Series(fitness)
    #     fitness = pd.Series(population)
    #     fitness = fitness.apply(lambda x: x.fitness[7])
    mean, count, min = fitness.describe()[['mean', 'count', 'min']]
    print(f'размер популяции = {count:,} особей, среднее - {mean:,.0f}, лучшее - {min:,.0f}')


def drop_duplicates(population):
    #     population=list(set(population))
    set_osob = []
    set_osob_fitness = []
    for name in population:
        if name.fitness not in set_osob_fitness:
            set_osob.append(name)
            set_osob_fitness.append(name.fitness)
        else:
            pass
    return set_osob


def is_list(population):
    for x in population:
        if type(x) == 'list':
            print('вот он кто делает лист')

fd=dates[0]
ld=dates[-1:].values[0]
# fd
# ld
# filename=
population_file_name=f'plan for  {fd} - {ld}.pkl'
population_file_name
# Сохранение population в файл
def save_population(population, filename):
    with open(filename, 'wb') as f:
        pickle.dump(population, f)

# Загрузка population из файла
def load_population(filename):
    with open(filename, 'rb') as f:
        population = pickle.load(f)
    return population

# Начальные параметры генетического алгоритма
# ТОЛЬКО ДЛЯ ИНИЦИАЛИЗАЦИИ ПОПУЛЯЦИИ
population_size = 100
max_population_size=200

max_generations = 100

mutation_chance = 0.15
mutation_rate=0.15

crossover_rate=0.1

# population=population[:30]

# Инициализация популяции
population = []

original_osob = Individual(dfs['production_plan'].copy(), stock, orders)
population.append(original_osob)

# повысим интенсивность мутаций для стартовой популяции до предела.
# создадим максимальное многообразие стартовых точек

mutation_chance = 1
mutation_rate = 1

for _ in range(500):
    mutated_osob = mutation(copy.deepcopy(original_osob))
    population.append(mutated_osob)

mutation_chance = 0.15
mutation_rate = 0.15

# for _ in range(population_size):
#     suspect_plan=generate_random_prod_plan(all_nomenclature, dates, step=50, min=0, max=5000)
#     osob = Individual(suspect_plan, stock, orders)  # Здесь нужно написать функцию для генерации случайного индивида
#     population.append(osob)

population_info(population, 7)

# Вариант начала с заполнением случайными особями
import warnings
warnings.filterwarnings('ignore')
key=7
# population=[]
population_size=1
start_time = time.time()


def calculate_population_size(current_generation):
    #     if current_generation == 0:
    #         return 300

    new_individuals = math.ceil(100 * (1.02) ** (-0.6 * current_generation))
    return new_individuals // 1


add_num = []
for x in [0, 10, 50, 100, 300, 500, 900]:
    add_num.append(calculate_population_size(x))
print(add_num)

fitness_history=[]

import time

# Основной цикл эволюции
previous_time = time.time()

# Инициализация
max_stagnant_generations = 10
stagnant_count = 0
prev_best_fitness = float('inf')  # Бесконечность в начале


population_size=100
max_population_size=200
max_generations=150

# Начальные параметры генетического алгоритма


mutation_chance = 0.13
mutation_rate=0.15

crossover_rate=0.1

mutation_num=40
crossover_num=70

columns = ['выполнение', 'миностаток', 'хранение','разнообразие', 'перегруз', 'смены', 'изменения', 'итого']


def local_search(population, mutation_rate=0.03, mutation_chance=0.07, mutation_num=30):
    # Применение локального посика
    new_mutants = []
    for individual in population:
        for _ in range(mutation_num):
            new_mutant = mutation(individual)
            new_mutants.append(new_mutant)
    #     population.extend(new_mutants)

    return new_mutants


def evolution_v1(population):
    global max_generations
    population_size = 1
    population = population[:]
    global mutation_num
    global crossover_num
    non_productive = 10
    global fitness_history
    start_time = time.time()
    print(f'Эволюция началась. Ожидается {max_generations} поколений')
    prev_best_fitness = float('inf')
    flag_co = False
    flag_m = False
    for generation in range(max_generations):
        mutation_rate = random.randint(1, 30) / 100
        mutation_chance = random.randint(5, 20) / 100
        crossover_rate = random.randint(1, 40) / 100
        mutation_num = 20 + math.floor(2.7 ** (0.6 * random.randint(1, 11)))
        crossover_num = 20 + math.floor(2.7 ** (0.6 * random.randint(1, 11)))


        evolution_start = time.time()
        previous_time = time.time()
        population_size = calculate_population_size(generation)

        print('add random individuals= ', population_size)
        print('mutation_num= ', mutation_num)
        print('crossover_num= ', crossover_num)
        print(f'mutation_chance is {mutation_chance}, mutation_rate is {mutation_rate}')

        print(f'{generation} виток эволюции начался.')
        population_info(population, key)

        print('случайных особей добавим')
        for _ in range(population_size):
            suspect_plan = generate_random_prod_plan(all_nomenclature, dates)
            osob = Individual(suspect_plan, stock,
                              orders)  # Здесь нужно написать функцию для генерации случайного индивида
            population.append(osob)
        pop_sort(population, key)
        population_info(population, key)

        print('удалим дубликаты')
        population = drop_duplicates(population)
        population_info(population, key)

        print('мутация')

        # Применение мутаций
        pop_sort(population, key)
        # принудительно мутируем особи из топа

        print('форсированная мутация первых 10')
        new_mutants = []
        m_n = mutation_num
        mutation_num = min(mutation_num, len(population))

        for _ in range(5):
            for individual in population[:10]:
                new_mutant = mutation(individual)
                population.append(new_mutant)

        population_info(population, key)

        print('мутация топа')
        pop_sort(population, key)
        new_mutants = []

        mutant_candidate = roulette_selection(population, num_parents=int(mutation_num + 3))

        new_mutants = []
        for _ in range(5):
            new_mutants = []
            for individual in mutant_candidate:
                new_mutant = mutation(individual)
                new_mutants.append(new_mutant)

        population.extend(new_mutants)
        population_info(population, key)

        print('применим локальный поиск')
        pop_sort(population, key)
        population.extend(local_search(population[:5]))
        population_info(population, key)
        pop_sort(population, key)

        print('crossover_tournament')
        pop_sort(population, key)

        #         кроссовер
        for x in range(int(crossover_num)):
            #             parent1, parent2 = roulette_selection(population, num_parents=2)
            parent1, parent2 = tournament_selection(population, tournament_size=10)
            # Здесь нужно написать функцию для скрещивания
            child1, child2 = crossover(
                parent1, parent2, crossover_rate=crossover_rate)
            population.extend([child1, child2])
        population_info(population, key)

        print('crossover_roulette_selection')
        pop_sort(population, key)

        #         кроссовер
        for x in range(int(crossover_num)):
            parent1, parent2 = roulette_selection(population, num_parents=2)
            #             parent1, parent2 =tournament_selection(population, tournament_size=10)
            # Здесь нужно написать функцию для скрещивания
            child1, child2 = crossover(
                parent1, parent2, crossover_rate=crossover_rate)
            population.extend([child1, child2])
        population_info(population, key)

        print('удалим дубликаты')
        population = drop_duplicates(population)
        population_info(population, key)
        pop_sort(population, key)

        print('обрезка инвалидов')
        if len(population) > max_population_size:
            last = len(population) // 10
            population = population[:last]

        fitness_history.append(population[0].fitness)
        population_info(population, key)

        print(f'Эволюция длилась {time.strftime("%H:%M:%S", time.gmtime(time.time() - evolution_start))}')
        print(f'Эволюция {generation} сделал шаг?')

        print('*' * 30)

        best_fitness = population[0].fitness[7]

        if prev_best_fitness > best_fitness:
            prev_best_fitness = best_fitness
            stop_count = 0
        else:
            stop_count += 1
        if stop_count > non_productive:
            print('#' * 50)
            print(f'нет изменений уже {stop_count} поколений')
            print('может пора остановится???')
            print('#' * 50)

        pop_sort(population, key)
        mutation_num = m_n
        save_population(population, population_file_name)
        print(f'population saved in {population_file_name}')
    #         best_fitness = population[0].fitness[7]

    #     # loaded_population = load_population('population.pkl')  # Загрузка из файла

    print(f'Эволюция для {key} свершилась?')
    print('*' * 30)
    population_info(population)
    print(datetime.now())
    return population

import warnings
warnings.filterwarnings('ignore')
import time
# import matplotlib.pyplot as plt
# from IPython.display import clear_output

# import plotly.graph_objs as go
# from IPython.display import clear_output, display
# from plotly.subplots import make_subplots
initial_population=copy.deepcopy(population)
len(initial_population)

# ОСНОВНАЯ ЭВОЛЮЦИЯ
# разбирайся с кроссовером.
# почему все удалилось????
# population=new_population(10)
key = 7

# crossover_num=1000
mutation_chance = 0.3
mutation_rate = 0.3
crossover_rate = 0.2

populations = []
# mutation_nums=[20,20,20,20,20]
# crossover_nums=[50,50,50,50,50]

evo_muts = [50, 60, 70, 80, 100]
evo_cros = [100, 90, 80, 70, 60]
# initial_population=copy.copy(population)
max_generations = 7

import multiprocessing


def evolve(x, mutation_num, crossover_num, max_population_size, initial_population):
    print(f'{x} линия эволюции')
    print('x' * 50)
    population = evolution_v1(initial_population, mutation_num, crossover_num, max_population_size)
    return population


if __name__ == '__main__':
    evo_muts = [random.randint(1, 100) for _ in range(5)]
    evo_cros = [random.randint(1, 100) for _ in range(5)]
    max_population_size = 200

    with multiprocessing.Pool() as pool:
        populations = pool.starmap(
            evolve,
            [(x, evo_muts[x], evo_cros[x], max_population_size, initial_population) for x in range(5)]
        )

    hyper_pop = []
    for pop in populations:
        hyper_pop.extend(pop)

    population = hyper_pop

mutation_chance = 0.15
mutation_rate = 0.15
crossover_rate = 0.1
max_generations = 550
mutation_num = 70
crossover_num = 70
max_population_size = 200

final_population = evolution_v1(population)





