import sys
import Lib
print(Lib.__file__)
from Lib.Utils import get_spark_session
from Lib.DataReader import get_orders,get_customers
from Lib.DataManipulation import filter_order_status,joined_order_customer_df,count_order_status
if __name__ == '__main__':
  if len(sys.argv) < 2:
      print("Please specify the environment")
      sys.exit(-1)
job_run_env = sys.argv[1]
print('creating spark session')
spark=get_spark_session(job_run_env)
order_df=get_orders(spark,job_run_env)
      #filter the closed status
order_status_df=filter_order_status(order_df,'CLOSED')
# Join the dataframes
customer_df=get_customers(spark,job_run_env)
joined_df=joined_order_customer_df(order_df,customer_df)
# Count the orders
count_orders=count_order_status(joined_df)

joined_df=joined_order_customer_df(order_df,customer_df)
      #count the orders
count_orders=count_order_status(joined_df).collect()
print(count_orders)
print('End of Environment')