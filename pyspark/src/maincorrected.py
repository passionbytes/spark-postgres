#!/usr/bin/python
import pyspark

if __name__ == '__main__':
    sc = pyspark.SparkContext(appName='CoinToss')

    try:
        session = pyspark.sql.SparkSession(sc)

        df = session.read.format("jdbc").option("driver","org.postgresql.Driver").option("url", "jdbc:postgresql://postgres/postgres").option("query","select * from public.coin_toss").option("numPartitions",5).option("fetchsize", 2000).option("user", "postgres").option("password", "postgres").load()
        samples = df.count()
        heads_freq = df.filter("outcome == 'heads'").count() / samples
        tails_freq = df.filter("outcome == 'tails'").count() / samples

        stats = df.groupBy('outcome').count()

        for row in stats.rdd.collect():
            print("{} {}%".format(row['outcome'],
                                  row['count'] / samples * 100))
    finally:
        sc.stop()
