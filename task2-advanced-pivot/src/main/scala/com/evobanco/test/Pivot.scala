package com.evobanco.test

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object Pivot {
  val TypeMeasurement = "measurement"
  val TypeTest = "test"

  val TestPassed = "passed"
  val TestFailed = "failed"

  val AggFirst = "first"
  val AggLast = "last"

  implicit def pivot(df: DataFrame) = new PivotImplicit(df)
}

class PivotImplicit(df: DataFrame) extends Serializable {

  /**
   * Pivots machine data
   *
   * @return machine data pivoted
   */
  def getTests(): DataFrame = {

    val sparkInit =SparkSession.builder().appName("parts").master("local").getOrCreate()

    import sparkInit.implicits._
    df.createOrReplaceTempView("viewtemp")


    val firstMed=sparkInit.sql("""Select part,'first' as Aggregation,unixTimestamp as time,concat('(',name,',',value,')') as Features from viewtemp""")
    val lastMed=sparkInit.sql("""Select part,'last' as Aggregation,unixTimestamp as time,concat('(',name,',',value,')') as Features from viewtemp""")

    val firstMedfilter=firstMed.filter($"mType".contains("measurement")).groupBy($"part",$"Aggregation").agg(expr("first(Features) as Features"))
    val lastMedfilter=lastMed.filter($"mType".contains("measurement")).groupBy($"part",$"Aggregation").agg(expr("first(Features) as Features"))

    val resultUnionMed=firstMedfilter.union(lastMedfilter)
    val result=resultUnionMed.as("x1").join(resultUnionMed.as("x2"),$"x1.part"===$"x2.part" && $"x1.Features".=!=($"x2.Features")  && $"x1.Aggregation".=!=($"x2.Aggregation") ).filter($"x1.Features".substr(0,2)=!=$"x2.Features".substr(0,2)).withColumn("Features1",expr("concat('[',x1.Features,',',x2.Features,']')")).select($"x1.part",$"x1.Aggregation",$"Features1")

    val agruparMed=result.select($"part",$"Aggregation",$"Features1")
    val resultCons=resultUnionMed.select("part").intersect(agruparMed.select("part"))


    val auxMedJoin=resultUnionMed.join(resultCons,Seq("part"),"leftanti").withColumn("Features",expr("concat('[',Features,']')"))

    val FinalMed=auxMedJoin.union(agruparMed)


    val firstTestConsult=sparkInit.sql("""Select part,location,'first' as Aggregation,name,testResult from viewtemp""")
    val LastTestConsult=sparkInit.sql("""Select part,location,'last' as Aggregation,name,testResult from viewtemp""")

    val firstTestFilter= firstTestConsult.filter($"mType".contains("test")).groupBy($"part",$"location",$"Aggregation",$"name").agg(first($"testResult").as("testResult"),expr("first(name) as Name1"))
    val lastTestFilter= LastTestConsult.filter($"mType".like("test")).groupBy($"part",$"location",$"Aggregation",$"name").agg(last($"testResult").as("testResult"),expr("last(name) as Name2"))

    val resultTest=firstTestFilter.select($"part",$"location",$"Aggregation",$"Name1",$"testResult").union(lastTestFilter.select($"part",$"location",$"Aggregation",$"Name2",$"testResult"))
    val resultFin= resultTest.as("x").join(FinalMed.as("y"),$"x.part"===$"y.part" && $"x.Aggregation"===$"y.aggregation").select($"x.part".as("Part"),$"x.location".as("Location"),$"x.Name1".as("Test"),$"testResult",$"x.Aggregation",$"y.Features")

    resultFin

  }

}