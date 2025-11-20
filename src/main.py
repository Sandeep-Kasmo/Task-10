import pandas as pd #type:ignore
from extract import *
from transform import *
from pymongo.errors import PyMongoError #type:ignore


def main():
    # connect to mongodb
    connect_mongodb()

    # extract collections
    customer_df = get_data("PythonLearningDB", "customer")
    print(customer_df)
    orders_df = get_data("PythonLearningDB", "orders")
    print(orders_df)

    #perform transformations and print data
    try:
        customer_transform_df=customer_transform(customer_df)
        print(customer_transform_df)
    except PyMongoError as e:
        print(f'Error:{e}')
    try:
        orders_transform_df=orders_transform(orders_df)
        print(orders_transform_df)
    except PyMongoError as e:
        print(f"Error:{e}")
    
    # merge the data and print it
    try:
        merged_df=merge_data(customer_transform_df,orders_transform_df,'customer_id','left')
        print(merged_df)
    except PyMongoError as e:
        print(f"Error:{e}")
    

    # close the connection
    close_mongodb()

if __name__ == "__main__":
    main()
