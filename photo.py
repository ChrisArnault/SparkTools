 
import csv
import numpy as np
import random
import sys
import pickle
#from pyspark import SparkConf
from scipy import stats
from pyspark import SparkConf, SparkContext



ZSPEC = np.linspace(0.05, 3, 60)
MAG = np.linspace(-24, -12, 121)
GALTYPE = {0: "Ell", 1: "S", 2: "SB"}
DZ = np.linspace(-4.5, 4.5, 901)

def gal_id(z, gal_type, magnitude,dz):
    return '%d,%d,%d,%d' % (z, gal_type, magnitude,dz)

class Simulation:
    """
    Hold a simulation context with
    - a base Z
    - a magnitude
    - a galaxy type
    - a PDF to be added to the base Z

    the values are encoded in the simulation file as follows:

      zspec=np.linspace(0.05, 3, 60)
      mag=np.linspace(-24, -12, 121)
      galtype={0:"Ell", 1:"S", 2:"SB"}
      dz=np.linspace(-4.5,4.5,901) #zphot-zsdef
    """

    def __init__(self, zspec, gal_type, mag, dz):
        self.gal_type = gal_type
        self.mag = mag
        self.zspec = zspec
        self.dz = np.array(dz)
        self.my_generator = None

    def get_zspec(self):
        # get the real value for the Z base
        return ZSPEC[self.zspec]

    def get_mag(self):
        # get the real value for the magnitude
        return MAG[self.mag]

    def generator(self):
        # construct a generator using the PDF
        if self.my_generator is None:

            N = len(self.dz)

            xk = np.arange(N)
            pk = self.dz
            pk = pk / pk.sum()

            self.my_generator = stats.rv_discrete(name='zdist', values=(xk, pk))

        return self.my_generator

Simulations = dict()

def f(u, v):
    test = np.array(u) < np.array(v)
    return np.all(test)

def main(sc, N=100):
    d1 = sc.textFile('hdfs:///user/christian.arnault/pzdist.txt')  # get physics model
    d2 = d1.filter(lambda u: u[0] != '#')  # remove comments
    d3 = d2.map(lambda u: u.split())  # get fields
    d4 = d3.map(lambda u: (int(u[0]), int(u[1]), int(u[2]), [float(z) for z in u[3:]]))  # format as (K, V=PDF)
    d5 = d4.filter(lambda u: np.sum(np.array(u[3])) > 0)  # remove empty PDF

    data = d5.map(lambda u: ((u[0], u[1], u[2]), u[3]))

    dv = data.collect()
    d = {k: v for k, v in dv}

    sc.broadcast(d)

    #N = 1000000000
    rdd = sc.parallelize(xrange(N)).map(lambda u: (random.randint(0, 60), random.randint(0, 2), random.randint(0, 121))).filter(lambda u: u in d)
    rdd2= rdd.count()
    print(rdd2)


def generate(u):

    #z=u[0][0]
    #m=u[0][1]
    #t=u[0][2]
    #dz=u[1][0]
    #id = gal_id(z,m,t,dz)





    x0 = []
    x1 = []
    x2 = []
    y0 = []
    y1 = []
    y2 = []
    key = u[0]
    val = u[1]



    #result = 'new sim %s %d' % (repr(key), len(val[1]))

    zspec = int(key[0])
    gal_type = int(key[1])
    mag = int(key[2])

    dz = [float(z) for z in val[1]]

    sim = Simulation(zspec, gal_type, mag, dz)

    generator = sim.generator()

    zspec = sim.get_zspec()

    n_tries = 1

    y = generator.rvs(size=1)
    tries = (y * 9.0 / 901.0) - 4.5 + zspec

    gal_type = sim.gal_type
    mag = sim.get_mag()

    #if gal_type == 0:
        #x0.append(mag)
        #y0.append(tries[0])
    #elif gal_type == 1:
        #x1.append(mag)
        #y1.append(tries[0])
    #elif gal_type == 2:
        #x2.append(mag)
        #y2.append(tries[0])


    #return ((x0,y0),(x1,y1),(x2,y2))

    return (mag,tries[0])


if __name__ == "__main__":
    # Configure Spark
    print sys.argv

    N = 100
    if len(sys.argv) >= 2:
        N = int(sys.argv[1])
    conf = SparkConf().setMaster("spark://134.158.75.222:20000")
    conf = conf.setAppName("PHOTO3")
    sc = SparkContext(conf=conf)

    main(sc, N=N)

    print 'end...............'
