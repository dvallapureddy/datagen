import sys
from pyspark.sql.types import *
from pyspark import SQLContext as sqlContext
from pyspark.context import SparkContext
from pyspark.sql import Row
from pyspark.sql import SparkSession
from faker import Faker
## @params: [JOB_NAME]
#args = getResolvedOptions(sys.argv, ['JOB_NAME'])
import random,string
from faker.providers import BaseProvider
sc = SparkContext("yarn","datagen")

# number of records
#num_records = int(1e7)
num_records = int(5000)
# faker settings
#fake = Faker('nl_NL')
fake = Faker()
class genderProvider(BaseProvider):
    def gender(self):
        gender = ["male", "female"]
        return random.choice(gender)
class shirtSizeProvider(BaseProvider):
    def shirtSize(self):
        shirtsize = ["S", "M", "L", "XL", "XXL", "XXXL"]
        return random.choice(shirtsize)
class carMakerProvider(BaseProvider):
    def carMaker(self):
        carmaker = ["VW", "NISSAN", "GM", "TATA", "JEEP", "LAND ROVER", "HYUNDAI", "KIA"]
        return random.choice(carmaker)
class moiveGeneric(BaseProvider):
    def movieGen(self):
        generic = ["Thiller", "truestory", "comdey", "action", "horror", "documenty", "forign", "KIA"]
        return random.choice(generic)
class getRegion(BaseProvider):
    def getRegion(self):
        region = ["apac", "na", "euro", "oceanic", "sa", "emea"]
        return random.choice(region)
fake.add_provider(genderProvider)
fake.add_provider(shirtSizeProvider)
fake.add_provider(carMakerProvider)
fake.add_provider(moiveGeneric)
#fake_line = lambda x: Row(fake.sha256(), fake.name(), fake.street_name(), fake.province(), fake.country(), fake.phone_number(), fake.email(), fake.iban())
def gendata():
    fake_line = lambda x: Row(fake.uuid4(),fake.numerify('#' * 25),fake.first_name(),fake.last_name(),fake.email(), fake.gender(),fake.ipv4(),fake.city(),fake.country(),fake.latitude(),fake.longitude(),fake.phone_number(), fake.postcode(),fake.state(),fake.street_address(),fake.street_address(),fake.street_name(), fake.random_number(),fake.street_suffix(),fake.date_time(),fake.currency()[0],fake.currency_code(), fake.bs(), fake.date_time(),fake.company(), fake.bs(),fake.random_number(), fake.company(), fake.date_time(), fake.currency_code(),fake.random_letter(),fake.numerify('#######'),fake.file_path(),fake.job(),fake.random_letter(), fake.numerify('###'),fake.numerify('################'),fake.shirtSize(),fake.pyfloat(left_digits=None, right_digits=None, positive=False),fake.pystr(min_chars=None, max_chars=40),fake.pystr(min_chars=40,max_chars=80),fake.city(),fake.carMaker(), fake.pystr(min_chars=None, max_chars=20),fake.movieGen(),fake.pystr(min_chars=None, max_chars=60),fake.pystr(min_chars=20, max_chars=60),fake.pystr(min_chars=None, max_chars=50),fake.numerify('###'),fake.numerify('################'),fake.numerify('####'),fake.numerify('#######'),fake.numerify('#######'),fake.pybool(),fake.pyfloat(left_digits=None, right_digits=None, positive=False),fake.pyfloat(left_digits=None, right_digits=None, positive=False),fake.pyfloat(left_digits=None,right_digits=None, positive=False),fake.word(),fake.word(), fake.numerify('#######'),fake.ean(length=13),fake.word(),fake.safe_hex_color(),fake.address().replace("\n", " "),random.randint(0, 1000000),random.randint(0, 1000000),random.choice(string.ascii_uppercase),random.randint(0, 1000000),random.randint(0, 1000000),random.choice(string.ascii_uppercase),random.randint(0, 1000000),random.randint(0, 1000000),random.choice(string.ascii_uppercase),random.randint(0, 1000000),random.randint(0, 1000000),random.choice(string.ascii_uppercase),random.randint(0, 1000000),random.randint(0, 1000000),random.choice(string.ascii_uppercase),random.randint(0, 1000000),random.randint(0, 1000000),random.choice(string.ascii_uppercase),random.randint(0, 1000000),random.randint(0, 1000000),random.choice(string.ascii_uppercase),random.randint(0, 1000000),random.randint(0, 1000000),random.choice(string.ascii_uppercase),random.randint(0, 1000000),random.randint(0, 1000000),random.choice(string.ascii_uppercase),random.randint(0, 1000000),random.randint(0, 1000000),random.choice(string.ascii_uppercase),random.randint(0, 1000000),random.randint(0, 1000000),random.choice(string.ascii_uppercase),random.randint(0, 1000000),random.randint(0, 1000000),random.choice(string.ascii_uppercase),random.randint(0, 1000000))
#df_header = ['sha256', 'name', 'streetname', 'province', 'country', 'phonenumber', 'email', 'iban']
    df_header=["id","c_id","first_name","last_name", "mail","gender","ip_address_v4","residentcity","Countrycode","latitude","longitude","contactphone","statepostalcode","state","streetaddressnewlabel","streetaddress","streetname","streetnumber","carpart","datetitmecreated","currency","currencycode","Retaildepartmentname","Rdate", "stockindustryname","stockmarketname","stockmarketcap", "nameofthestock","stockaddeddate","stocksymbol","sequenceofchar","digitsequence","filepath","Deptnew","personalavatar","etin","dunsnumber","shirtsize","uadnumber","Selected","universitystudied","district","CarMake","CarMakeModel","MovieGeneres","MovieTitile","MovieTitile","Movieproducts","3Digitcode","SerialID","SeriesID","IntValue","Promocode","Booleanvalue","Floatvalue2","Floatvalue1","Floatvalue","carsummary", "Smipleword","colorcode","barcode", "Word","colorcode1","address","random0","random1","random2","random3","random4","random5","random6","random7","random8","random9","random10","random11","random12","random13","random14","random15","random16","random17","random18","random19","random20","random21","random22","random23","random24","random25","random26","random27","random28","random29","random30","random31","random32","random33","random34","random35","random36","random37"]
# create 
#dff = sc.parallelize(range(0, num_records)).map(fake_line).mapPartitions(10)#.toDF(schema = df_header)
    dff = sc.parallelize(range(0, num_records),10).map(fake_line)
    spark = SparkSession(sc)
    hasattr(dff, "toDF")
#print dff.count()
#dff.toDF().show()
    df=dff.toDF()#(schema = df_header)
#df=sqlContext.createDataFrame(dff).collect()
#df.repartition(100)
#df.withColumn("currency",stringify("ArrayOfString")).write.csv('file:///tmp/mycsv')
#f.coalesce(1).write.csv('file:///home/amrutha/tagdatagen/datagen/mycsv')
    df.coalesce(1).write.csv('mycsv')
#df.write.csv('file:///tmp/mycsv')

#dff.saveAsTextFile("mycsv")
