from pyspark.sql.functions import *

def filter_order_status(order_df,status):
 return order_df.filter('order_status="{}"'.format(status))
def joined_order_customer_df(order_df,customer_df):
 return order_df.join(customer_df,on='customer_id',how='inner')
def count_order_status(joined_df):
 return joined_df.groupBy('state').count()
