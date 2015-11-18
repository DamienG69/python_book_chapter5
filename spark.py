#imports

from pyspark import SparkContext

from pyspark.sql import HiveContext

#only needed if you want to use spark in batch mode

#2 import the hive context to be able to interact with hive

sc = SparkContext()

sqlContext = HiveContext(sc)

data = sc.textFile("/chapter5/LoanStats3d.csv")

#2 split every line at a comma

parts = data.map(lambda l:l.split(','))


#print the first line, this should contain the headers

#in most applications, the header is stored separately from the data

#1 read the first line of the csv file

firstline =  parts.first()

#2 retain all the lines that are not equal to the first line

datalines = parts.filter(lambda x:x != firstline)

def cleans(l):

        l[7] = str(float(l[7][:-1])/100)

        return [s.encode('utf8').replace(r"_"," ").lower() for s in l]

#2 call the helper function for every line

datalines = datalines.map(lambda x: cleans(x))

#1 imports

from pyspark.sql.types import *

#2 create metadata

fields = [StructField(field_name,StringType(),True) for field_name in firstline]

schema = StructType(fields)

#3 create a dataframe

schemaLoans = sqlContext.createDataFrame(datalines, schema)

#4 register it as a table called loans

schemaLoans.registerTempTable("loans")

#1 drop table, summarize and store in hive

sqlContext.sql("drop table if exists LoansByTitle")

sql = '''create table LoansByTitle stored as parquet as select title, count(1) as number from loans group by title order by number desc'''

sqlContext.sql(sql)
sqlContext.sql('drop table if exists raw')

sql = '''create table raw stored as parquet as select title, emp_title,grade,home_ownership,int_rate,recoveries,collection_recovery_fee,loan_amnt,term from loans'''

sqlContext.sql(sql)

