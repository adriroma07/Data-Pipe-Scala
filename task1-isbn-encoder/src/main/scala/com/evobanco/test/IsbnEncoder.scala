package com.evobanco.test

import org.apache.spark.sql.{DataFrame, SparkSession}

object IsbnEncoder {
  implicit def dmcEncoder(df: DataFrame) = new IsbnEncoderImplicit(df)
}

class IsbnEncoderImplicit(df: DataFrame) extends Serializable {

  /**
    * Creates a new row for each element of the ISBN code
    *
    * @return a data frame with new rows for each element of the ISBN code
    */
  def explodeIsbn(): DataFrame = {

    val sparkInit =SparkSession.builder().appName("IsbnEncoder").master("local").getOrCreate()

    val dataFr=df.filter(df("isbn") rlike """ISBN: \d{3}-\d{10}""")

    if(dataFr.count()!= 0) {
      val collection = dataFr.collect()

      val mapISBN=collection.flatMap(x=>List((
         x.get(0).toString,x.get(1).toString.toInt,"ISBN-EAN: "+x.get(2).toString.substring(6,9)),
        (x.get(0).toString,x.get(1).toString.toInt,"ISBN-GROUP: "+x.get(2).toString.substring(10,12)),
        (x.get(0).toString,x.get(1).toString.toInt,"ISBN-PUBLISHER: "+x.get(2).toString.substring(12,16)),
        (x.get(0).toString,x.get(1).toString.toInt,"ISBN-TITLE: "+x.get(2).toString.substring(16,19))

      ))


      return df.union(sparkInit.createDataFrame(mapISBN).toDF())
    }

    return df

  }
}
