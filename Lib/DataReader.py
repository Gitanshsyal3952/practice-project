from Lib import ConfigReader
def order_schema():
    return "order_id int,order_date timestamp,customer_id int,order_status string"

def get_orders(spark,env):
  conf = ConfigReader.get_app_config(env)
  orders_file_path  =conf['orders.file.path']
  return spark.read \
        .format('csv') \
        .schema(order_schema()) \
        .option('header',True) \
        .load(orders_file_path)

def customers_schema():
    return '''customer_id int,customer_fname string,customer_lname string,username string,password string,address string,city string,
    state string,pincode string'''

def get_customers(spark,env):
    conf = ConfigReader.get_app_config(env)
    customers_file_path=conf['customers.file.path']
    return spark.read \
           .format('csv') \
           .schema(customers_schema()) \
           .option('header',True)  \
           .load(customers_file_path)
