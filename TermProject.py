#!/usr/bin/python3

from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
import subprocess
from pyspark.sql.functions import col, max as max_
import sys

class TermProject():
    def __init__(self):
        self.conf = SparkConf().setAppName('TermProject App')
        self.sc=SparkContext(conf=self.conf)
        
    def load_year(self, year_file):
        spark = SparkSession.builder.appName("TermProject-app").config("spark.config.option", "value").getOrCreate()
        self.df = spark.read.option("header", "true").csv(year_file)
        rdd=self.df.rdd
        return self.df
    
  
    def Delay(self, out_dir):

        #carriers = self.df.groupBy('UniqueCarrier').count().orderBy(desc('count'))
#1 2
        departure_delay_origins = self.df.withColumn('DepDelay', col('DepDelay').cast('integer')).groupBy('Origin').sum('DepDelay').orderBy('sum(DepDelay)')
        
        #least_arr_delay = departure_delay_origins.first()
        
        most_dep_delay_airport = departure_delay_origins.orderBy(desc('sum(DepDelay)')).first()
        print('\n\nAirport {} has the most Departure Delay of total {} minutes'.format(most_dep_delay_airport['Origin'], most_dep_delay_airport['sum(DepDelay)']))
        print('Airport {} has the least Departure Delay of total {} minutes'.format(departure_delay_origins.first()['Origin'], departure_delay_origins.first()['sum(DepDelay)']))

        #self.sc.parallelize(departure_delay_origins.collect()).saveAsTextFile(out_dir)
        
        

#3 4
        arr_delay_origins = self.df.withColumn('ArrDelay', col('ArrDelay').cast('integer')).groupBy('Origin').sum('ArrDelay').orderBy('sum(ArrDelay)')
        
        #least_arr_delay = arr_delay_origins.first()
        
        most_arr_delay_airport = arr_delay_origins.orderBy(desc('sum(ArrDelay)')).first()
        print('\nAirport {} has the most Arrival Delay of total {} minutes'.format(most_arr_delay_airport['Origin'], most_arr_delay_airport['sum(ArrDelay)']))
        print('Airport {} has the least Arrival Delay of total {} minutes'.format(arr_delay_origins.first()['Origin'], arr_delay_origins.first()['sum(ArrDelay)']))
        
        #self.sc.parallelize(arr_delay_origins.collect()).saveAsTextFile(out_dir)
        

#5 6 
        arr_delay_flights = self.df.withColumn('ArrDelay', col('ArrDelay').cast('integer')).groupBy('UniqueCarrier').sum('ArrDelay').orderBy('sum(ArrDelay)')
        
        #least_arr_delay = arr_delay_flights.first()
        
        most_arr_delay_flights= arr_delay_flights.orderBy(desc('sum(ArrDelay)')).first()
        print('\nAirline {} has the most Arrival Delay at of total {} minutes'.format(most_arr_delay_flights['UniqueCarrier'], most_arr_delay_flights['sum(ArrDelay)']))
        print('Airline {} has the least Arrival Delay at of total {} minutes'.format(arr_delay_flights.first()['UniqueCarrier'], arr_delay_flights.first()['sum(ArrDelay)']))
        
        #self.sc.parallelize(arr_delay_flights.collect()).saveAsTextFile(out_dir)
        
        

#7 8
        departure_delay_flights = self.df.withColumn('DepDelay', col('DepDelay').cast('integer')).groupBy('UniqueCarrier').sum('DepDelay').orderBy('sum(DepDelay)')
        
        #least_arr_delay = departure_delay_flights.first()
        
        most_dep_delay_flights = departure_delay_flights.orderBy(desc('sum(DepDelay)')).first()
        print('\nAirline {} has the most Departure Delay at of total {} minutes'.format(most_dep_delay_flights['UniqueCarrier'], most_dep_delay_flights['sum(DepDelay)']))
        print('Airline {} has the least Departure Delay at of total {} minutes'.format(departure_delay_flights.first()['UniqueCarrier'], departure_delay_flights.first()['sum(DepDelay)']))

        #self.sc.parallelize(departure_delay_flights.collect()).saveAsTextFile(out_dir)
        
        

#9 10

        Average_Delay = self.df.withColumn('ArrDelay', col('ArrDelay').cast('integer'))\
                        .withColumn('DepDelay', col('DepDelay').cast('integer'))\
                        .groupBy('UniqueCarrier').avg('ArrDelay','DepDelay').sort('UniqueCarrier')
        print('\n\nThe following table shows the average Arrival and average Departure delay per carrier ')
        Average_Delay.show()


        return  

def delete_out_dir(out_dir):
    subprocess.call(["hdfs", "dfs", "-rm", "-R", out_dir])           


def main(argv):
    delete_out_dir(argv[1])
    perf = TermProject()
    perf.load_year(argv[0])
    perf.Delay(argv[1])
  
if __name__ == '__main__':
    main(sys.argv[1:])
