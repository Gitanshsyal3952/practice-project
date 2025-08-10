import configparser
from pyspark import SparkConf

import os

def get_app_config(env):
    config_path = "config/application.conf"
    print("Looking for config file at:", os.path.abspath(config_path))
    config = configparser.ConfigParser()
    files_read = config.read(config_path)
    print("Files read:", files_read)                  # Should be ['config/application.conf']
    print("Sections found:", config.sections())       # Should include 'LOCAL'
    if env not in config.sections():
        raise Exception(f"Section '{env}' not found in application.conf")
    app_conf = {}
    for key, val in config.items(env):
        app_conf[key] = val
    return app_conf


def get_pyspark_config(env):
    import os
    print(f"Reading pyspark.conf for env: {env}")
    print("Full path:", os.path.abspath("config/pyspark.conf"))
    config = configparser.ConfigParser()
    files_read = config.read("config/pyspark.conf")
    print("Files read:", files_read)
    print("Available sections:", config.sections())
    if env not in config.sections():
        raise Exception(f"Section '{env}' not found in config file")
    pyspark_conf = SparkConf()
    for (key, val) in config.items(env):
        pyspark_conf.set(key, val)
    return pyspark_conf

