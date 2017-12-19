__author__ = 'divu'

from pyspark import SparkConf, SparkContext

def parseline(line):
    words = line.split(',')
    id = int(words[0])
    cost = float(words[2])
    return (id, cost)

conf = SparkConf().setMaster("local").setAppName("CustomerInvoice")
sc = SparkContext(conf=conf)

input = sc.textFile("customer-orders.csv")
orders = input.map(parseline)
amount_spent = orders.reduceByKey(lambda x,y : x+y).sortByKey()
results = amount_spent.collect()

for result in results:
    print result[0], " : ", (result[1])
