from pyspark.sql import SparkSession
from pyspark import SparkConf

## Creating Spark Object
def get_spark_object(appName):
    
    conf = SparkConf() \
    .setAppName(appName) \
    .setMaster("local") \
    .set("spark.executor.memory", '8g')\
    .set('spark.executor.cores', '3')\
    .set('spark.cores.max', '3')\
    .set("spark.driver.memory",'8g')\
    
    # Create a SparkSession using the SparkConf object
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    return spark

spark = get_spark_object('test')

        
    # spark = SparkSession \
    #           .builder \
    #           .master('local') \
    #           .appName(appName) \
    #           .set("spark.executor.memory", '8g')\
    #           .set('spark.executor.cores', '3')\
    #           .set('spark.cores.max', '3')\
    #           .set("spark.driver.memory",'8g')\
              # .getOrCreate()


# .set("spark.executor.memory", '8g')
# .set('spark.executor.cores', '3')
# .set('spark.cores.max', '3')
# .set("spark.driver.memory",'8g')


# conf = SparkConf() \
#     .setAppName("MySparkApp") \
#     .setMaster("local[2]") \
#     .set("spark.executor.memory", "2g")



# style
th_props = [
  ('font-size', '14px'),
  ('text-align', 'center'),
  ('font-weight', 'bold'),
  ('color', 'red'),
  ('background-color', 'white'),
  ('border', '1px solid black')
  ]
                               
td_props = [
  ('font-size', '12px'),
  ('border', '1px solid black'),
  ('text-align', 'center')

  ]

table_props = [
      ('width','100%'),
      ('border-collapse', 'collapse')

]
                                 
styles = [
  dict(selector="th", props=th_props),
  dict(selector="td", props=td_props),
  dict(selector="table", props=table_props)

  ]

                  