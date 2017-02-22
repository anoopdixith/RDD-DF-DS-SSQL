// AverAge ;-)
println("\nRDDs")
val averageAgeFromRDD:(Double, Double) = citizensRDD.map((a) => (a.age.toDouble, 1.0))
                            .reduce((a,b) => ((a._1 + b._1).toDouble, (a._2 + b._2).toDouble))
println("AverAge is " + (averageAgeFromRDD._1 / averageAgeFromRDD._2).toDouble)

println("\nDataFrames")
val averageAgeFromDF = citizensDF.agg(avg($"age")).show

println("\nDataSets")
//Almost same as DF
val averageAgeFromDS = citizensDS.agg(avg("age")).show

println("\nSparkSQL")
val averageAgeQ = "SELECT avg(age) from citizens"
val averageAgeFromSS = sqlContext.sql(averageAgeQ).show
