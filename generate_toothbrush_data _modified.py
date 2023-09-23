import pandas as pd
import numpy as np
import datetime as dt
import warnings
import os


def main():
    warnings.filterwarnings('ignore')
    # set path or use working directory
    path = os.getcwd()
    # setting the size of the data
    n = np.random.choice(range(5000, 10000))
    # set if doing a full dump
    full_dump = False  #tracey: toggle to False after first run
    if full_dump:
        # setting the size of the data
        n = np.random.choice(range(5000, 10000))
        start_date = pd.to_datetime('2021-01-01')
        end_date = pd.to_datetime(dt.date.today())
        max_id = 0
        df = generate_order_number(max_id, max_id + n, [])
        df = add_columns(df, start_date, end_date, n, path)
        df = add_delivery_columns(df, n)
    else:
        n = np.random.choice(range(500, 1000))
        start_date = pd.to_datetime(dt.date.today() - dt.timedelta(days=1))
        end_date = pd.to_datetime(dt.date.today())
        # reading in the previous data generated that wasn't delivered
        null_df, max_id = read_existing_data(path)
        # updating the delivery columns
        null_df = update_delivery_columns(null_df)

        # adding order numbers to a list that already have data
        null_list = list(null_df['Order Number'].str[3:].astype(int))

        # generating new data
        df = generate_order_number(max_id + 1, max_id + n + 1, null_list) # tracey: changed max_id to max_id + 1 and max_id + n to max_id + n + 1
        df = add_columns(df, start_date, end_date, n, path)  # tracey: changed n-1 to n
        df = add_delivery_columns(df, n) # tracey: changed n-1 to n
        # adding the old data with new
        df = pd.concat([df, null_df], ignore_index=True)

    null_df = df[df['Delivery Date'].isnull()]
    # saving data to flat files
    file_name = f'order_data_{dt.datetime.today().strftime("%Y%m%d_%H%M")}.csv'
    df.to_csv(f'{path}/{file_name}', index=False)
    print(f'Saved file {file_name} to {path}')
    null_df.to_csv(f'{path}/null_order_data.csv', index=False)
    print(f"Saved file 'null_order_data.csv' to {path}")


def read_existing_data(path):
    max_id = 0
    for file in os.listdir(path):
        if file.endswith(".csv") and file.startswith("null"):
            null_df = pd.read_csv(f"{path}/{file}")  # tracey: changed path + file to f"{path}/{file}"
            null_df['Order Date'] = pd.to_datetime(null_df['Order Date'], errors='coerce')
        elif file.endswith(".csv") and file.startswith("order_data_"):
            df = pd.read_csv(f"{path}/{file}") # tracey: changed path + file to f"{path}/{file}"
            # tracey: changed while loop to if statement
            if max_id <= int(df['Order Number'].str[3:].max()):
                max_id = int(df['Order Number'].str[3:].max())
    return null_df, max_id


def random_dates(start, end, n):
    start_u = start.value // 10 ** 9
    end_u = end.value // 10 ** 9
    return pd.to_datetime(np.random.randint(start_u, end_u, n), unit='s')


def generate_order_number(l, n, null_list):
    lst = []
    start = l
    for i in range(l, n):
        if start in null_list:
            start += 1
        else:
            lst.append(''.join(['BRU{0:08}'.format(start)]))
            start += 1
    df = pd.DataFrame({'Order Number': list(set(lst))})

    return df


def add_columns(df, start_date, end_date, n, path):
    # add two types of toothbrushes
    toothbrush_type = ['Toothbrush 2000', 'Toothbrush 4000']
    df['Toothbrush Type'] = np.random.choice(toothbrush_type, size=n)

    tooth_1 = (df['Toothbrush Type'] == 'Toothbrush 2000')
    tooth_2 = (df['Toothbrush Type'] == 'Toothbrush 4000')

    len_tooth_1 = df[tooth_1].shape[0]
    len_tooth_2 = df[tooth_2].shape[0]

    # add random dates
    df['Order Date'] = random_dates(start_date, end_date, n)
    df['Order Date'] = pd.to_datetime(df['Order Date'])

    # adding in insight re: time of order and toothbrush type
    time_1 = np.random.normal(11, 3.4, n)
    time_2 = np.random.normal(18, 4.5, n)

    df.loc[tooth_1, 'Order Date'] = pd.to_datetime(df['Order Date'] + pd.to_timedelta(time_1, unit='h'))
    df.loc[tooth_2, 'Order Date'] = pd.to_datetime(df['Order Date'] + pd.to_timedelta(time_2, unit='h'))

    # adding in insight: re age of orderer and toothbrush type
    age_1 = np.random.normal(75, 11, len_tooth_1)
    age_2 = np.random.normal(26, 9, len_tooth_2)

    df.loc[tooth_1, 'Customer Age'] = age_1
    df.loc[tooth_2, 'Customer Age'] = age_2

    df['Customer Age'] = df['Customer Age'].astype(int)

    # adding quantity
    df['Order Quantity'] = np.random.choice(range(1, 10), n)

    # reading in postcode data
    postcodes = pd.read_csv(f"{path}/data/open_postcode_geo.csv", header=None, usecols=[0, 1],
                            names=['postcode', 'status'])
    postcodes = postcodes[(postcodes['status'] == 'live')]

    # randomly choosing postcodes
    df['Delivery Postcode'] = list(postcodes['postcode'].sample(n))
    # setting the billing postcode as the delivery postcode
    df['Billing Postcode'] = df['Delivery Postcode']

    # randomly picking the number of records where the billing and delivery postcode are different
    postcode_split = np.random.choice(range(1, int(n / 2)), 1)[0]
    # randomly picking a different billing postcode
    df.loc[:postcode_split - 1, 'Billing Postcode'] = list(postcodes['postcode'].sample(postcode_split))

    # dirty the postcode data
    lower = np.random.choice(range(1, int(n / 3)), 1)[0]
    upper = np.random.choice(range(int(n / 3), n), 1)[0]
    df.loc[lower:upper, 'Delivery Postcode'] = df['Delivery Postcode'].str.replace(' ', '').str.lower()
    df.loc[lower:upper, 'Billing Postcode'] = df['Billing Postcode'].str.replace(' ', '').str.lower()
    df.loc[:lower, 'Delivery Postcode'] = df.loc[:lower, 'Delivery Postcode'].str.replace(' ', '%20')
    df.loc[upper:, 'Billing Postcode'] = df.loc[upper:, 'Billing Postcode'].str.replace(' ', '   ')

    df.loc[:, 'is_first'] = 1
    return df


def add_delivery_columns(df, n):
    days_ago = dt.date.today() - dt.timedelta(days=3)

    # add dispatch status
    dispatch_status = ['Order Received', 'Order Confirmed', 'Dispatched']
    df['Dispatch Status'] = np.random.choice(dispatch_status, size=n)

    # all orders have been dispatched for first run
    df.loc[(df['Order Date'].dt.date < days_ago), 'Dispatch Status'] = 'Dispatched'

    # generate time intervals
    order_received = np.random.normal(0.2, 0.01, n)
    order_confirmed = np.random.normal(0.9, 0.2, n)
    order_dispatched = np.random.normal(6, 0.5, n)

    # generate dispatch time
    df.loc[df['Dispatch Status'] == 'Order Received', 'Dispatched Date'] = pd.to_datetime(
        df['Order Date'] + pd.to_timedelta(order_received, unit='h'))
    df.loc[df['Dispatch Status'] == 'Order Confirmed', 'Dispatched Date'] = pd.to_datetime(
        df['Order Date'] + pd.to_timedelta(order_received + order_confirmed, unit='h'))
    df.loc[df['Dispatch Status'] == 'Dispatched', 'Dispatched Date'] = pd.to_datetime(
        df['Order Date'] + pd.to_timedelta(order_received + order_confirmed + order_dispatched, unit='h'))

    # add delivery status to generate insight re: unsuccessful deliveries before 4am
    delivery_status = ['In Transit', 'Delivered', 'Unsuccessful']

    dispatch_mask_1 = (df['Dispatch Status'] == 'Dispatched') & (df['Dispatched Date'].dt.hour <= 4)
    df.loc[dispatch_mask_1, 'Delivery Status'] = np.random.choice(delivery_status, p=[0.4, 0.2, 0.4])

    dispatch_mask_2 = (df['Dispatch Status'] == 'Dispatched') & (df['Dispatched Date'].dt.hour > 4)
    df.loc[dispatch_mask_2, 'Delivery Status'] = np.random.choice(delivery_status, p=[0.3, 0.69, 0.01])

    # forcing all old orders to have some delivery data
    delivery_status = ['Delivered', 'Unsuccessful']
    dispatch_mask_1 = (df['Order Date'].dt.date < days_ago) & (df['Dispatched Date'].dt.hour <= 4)
    df.loc[dispatch_mask_1, 'Delivery Status'] = np.random.choice(delivery_status, p=[0.8, 0.2])

    dispatch_mask_2 = (df['Order Date'].dt.date < days_ago) & (df['Dispatched Date'].dt.hour > 4)
    df.loc[dispatch_mask_2, 'Delivery Status'] = np.random.choice(delivery_status, p=[0.99, 0.01])

    # generate time intervals
    in_transit = np.random.normal(1, 0.2, n)
    delivered = np.random.normal(26, 4, n)
    unsuccessful = np.random.normal(26, 8, n)

    # generate delivery time
    df.loc[df['Delivery Status'] == 'In Transit', 'Delivery Date'] = pd.to_datetime(
        df['Dispatched Date'] + pd.to_timedelta(in_transit, unit='h'))
    df.loc[df['Delivery Status'] == 'Delivered', 'Delivery Date'] = pd.to_datetime(
        df['Dispatched Date'] + pd.to_timedelta(in_transit + delivered, unit='h'))
    df.loc[df['Delivery Status'] == 'Unsuccessful', 'Delivery Date'] = pd.to_datetime(
        df['Dispatched Date'] + pd.to_timedelta(in_transit + unsuccessful, unit='h'))

    return df


def update_delivery_columns(df):
    # orders that weren't dispatched in the first generation, are updated to dispatch
    df.loc[(df['Dispatch Status'] != 'Dispatched'), 'Dispatch Status'] = 'Dispatched'

    n = df.shape[0]
    # generate time intervals
    order_received = np.random.normal(0.2, 0.01, n)
    order_confirmed = np.random.normal(0.9, 0.2, n)
    order_dispatched = np.random.normal(6, 0.5, n)

    # add dispatch time
    df.loc[df['Dispatch Status'] == 'Dispatched', 'Dispatched Date'] = pd.to_datetime(
        df['Order Date'] + pd.to_timedelta(order_received + order_confirmed + order_dispatched, unit='h'))

    delivery_status_transit = ['Delivered', 'Unsuccessful']

    # update delivery status for old data
    null_dispatch_mask_1 = (df['Delivery Status'] == 'In Transit') & (df['Dispatched Date'].dt.hour <= 4)
    df.loc[null_dispatch_mask_1, 'Delivery Status'] = np.random.choice(delivery_status_transit, p=[0.8, 0.2])
    null_dispatch_mask_2 = (df['Delivery Status'] == 'In Transit') & (df['Dispatched Date'].dt.hour > 4)
    df.loc[null_dispatch_mask_2, 'Delivery Status'] = np.random.choice(delivery_status_transit, p=[0.99, 0.01])

    # add delivery status to generate insight re: unsuccessful deliveries before 4am
    delivery_status = ['In Transit', 'Delivered', 'Unsuccessful']

    dispatch_mask_1 = (df['Dispatch Status'] == 'Dispatched') & (df['Dispatched Date'].dt.hour <= 4)
    df.loc[dispatch_mask_1, 'Delivery Status'] = np.random.choice(delivery_status, p=[0.4, 0.2, 0.4])

    dispatch_mask_2 = (df['Dispatch Status'] == 'Dispatched') & (df['Dispatched Date'].dt.hour > 4)
    df.loc[dispatch_mask_2, 'Delivery Status'] = np.random.choice(delivery_status, p=[0.3, 0.69, 0.01])

    # generate time intervals
    in_transit = np.random.normal(1, 0.2, n)
    delivered = np.random.normal(26, 4, n)
    unsuccessful = np.random.normal(26, 8, n)

    # generate delivery time
    df.loc[df['Delivery Status'] == 'In Transit', 'Delivery Date'] = pd.to_datetime(
        df['Dispatched Date'] + pd.to_timedelta(in_transit, unit='h'))
    df.loc[df['Delivery Status'] == 'Delivered', 'Delivery Date'] = pd.to_datetime(
        df['Dispatched Date'] + pd.to_timedelta(in_transit + delivered, unit='h'))
    df.loc[df['Delivery Status'] == 'Unsuccessful', 'Delivery Date'] = pd.to_datetime(
        df['Dispatched Date'] + pd.to_timedelta(in_transit + unsuccessful, unit='h'))
    return df


if __name__ == '__main__':
    main()

"""
def job():
    if __name__ == '__main__':
        main()

schedule.every().day.at("11:05").do(job)

while True:
    schedule.run_pending()
    time.sleep(60)
"""

