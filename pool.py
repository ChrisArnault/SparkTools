import csv
import numpy as np
import random
import sys
import pickle
from scipy import stats
from pyspark import SparkConf, SparkContext

def hash(z0, galType, magnitude):
    return z0 + 60 * galType + 3 * 60 * magnitude

def main(sc, N=100):
    print "start main..."
    d1 = sc.textFile('./pzdist.txt')
    d2 = d1.filter(lambda u: u[0] != '#')
    d3 = d2.map(lambda u: u.split())
    d4 = d3.map(lambda u : (hash(int(u[0]), int(u[1]), int(u[2])), [float(z) for z in u[3:]]) )
    data = d4.filter(lambda u : sum(u[1]) > 0)

    # print "data", data.take(3)

    dv = data.collect()
    d = {k: v for k, v in dv}
    # print d.keys()

    r1 = sc.parallelize(xrange(N))
    r2 = r1.map(lambda u: hash(random.randint(0, 60), random.randint(0, 2), random.randint(0, 121)))
    print r2.take(10)

    r3 = r2.filter(lambda u : u in d)

    print "r3 count=", r3.count()

    print "end main..."


if __name__ == "__main__":
    # Configure Spark
    print sys.argv

    N = 1000
    if len(sys.argv) >= 2:
        N = int(sys.argv[1])


    conf = SparkConf().setAppName("POOL").set("spark.kryoserializer.buffer.max", "100m") # .set("spark.executor.cores", 12).set("spark.executor.memory", "16g")
    sc = SparkContext(conf=conf)

    main(sc, N=N)

    print 'end...............'


