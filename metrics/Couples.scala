// Couples
println("\nRDDs")
println("\nAll couples from RDDs\n=============")
val couplesFromRDD = citizensRDD.map(a => (a.uniqueId, a.name))
                            .join(citizensRDD.map(a => (a.spouseId, a.name)))
                            .collect
                            .foreach(row => println(row._2._1 + " and " + row._2._2))

println("\nLong distance couples from RDDs\n=============")
val longDistanceCouplesFromRDD = citizensRDD.map(a => (a.uniqueId, (a.name, a.gender)))
                                            .join(citizensRDD.map(a => (a.spouseId, (a.name, a.gender))))
                                            .filter(row => (row._2._1._2 == row._2._2._2))
                                            .collect
                                            .foreach(row => println(row._2._1._1 + " and " + row._2._2._1))

println("\nGay couples from RDDs\n=============")
val gayCouplesFromRDD = citizensRDD.map(a => (a.uniqueId, (a.name, a.city)))
                                   .join(citizensRDD.map(a => (a.spouseId, (a.name, a.city))))
                                   .filter(row => (row._2._1._2 != row._2._2._2))
                                   .collect
                                   .foreach(row => println(row._2._1._1 + " and " + row._2._2._1))


println("\nDataFrames")
println("\nAll couples from DataFrames\n=============")
val couplesFromDF = citizensDF.as("A").join(citizensDF.as("B"), $"A.uniqueId" === $"B.spouseId").select("A.name", "B.name").show
println("\nLong distance couples from DF\n================")
val longDistanceCouplesFromDF = citizensDF.as("A")
                                          .join(citizensDF.as("B"), $"A.uniqueId" === $"B.spouseId")
                                          .filter("A.city != B.city")
                                          .select("A.name", "B.name")
                                          .show

println("\nGay couples from DF\n================")
val gayCouplesFromDF = citizensDF.as("A")
                                 .join(citizensDF.as("B"), $"A.uniqueId" === $"B.spouseId")
                                 .filter("A.gender == B.gender")
                                 .select("A.name", "B.name")
                                 .show

println("\nDataSets")
// Almost same as DataFrames
println("\nAll couples from DataSets\n=============")
val couplesFromDS = citizensDS.as("A").joinWith(citizensDS.as("B"), $"A.uniqueId" === $"B.spouseId").show
val couplesFromDSAlt1 = citizensDS.as("A").join(citizensDS.as("B"), $"A.uniqueId" === $"B.spouseId").select("A.name", "B.name").show

println("\nLong distance couples from DS\n================")
val longDistanceCouplesFromDS = citizensDS.as("A")
                                          .join(citizensDS.as("B"), $"A.uniqueId" === $"B.spouseId")
                                          .filter("A.city != B.city")
                                          .select("A.name", "B.name")
                                          .show

println("\nGay couples from DS\n================")
val gayCouplesFromDS = citizensDS.as("A")
                                 .join(citizensDS.as("B"), $"A.uniqueId" === $"B.spouseId")
                                 .filter("A.gender == B.gender")
                                 .select("A.name", "B.name")
                                 .show

println("\nSparkSQL")
println("\nAll couples from SparkSQL\n=============")
//Wow, so elegant!
val allCouplesQ = "select a.name, b.name from citizens a, citizens b where a.uniqueId = b.spouseId"
val allCouplesFromSS = sqlContext.sql(allCouplesQ).show

println("\nAll gay couples from SparkSQL\n=============")
val allGayCouplesQ = "select a.name, b.name from citizens a, citizens b where a.uniqueId = b.spouseId and a.gender = b.gender"
val allGCouplesFromSS = sqlContext.sql(allGayCouplesQ).show

println("\nAll long distance couples from SparkSQL\n=============")
val allLongDistanceCouplesQ = "select a.name, b.name from citizens a, citizens b where a.uniqueId = b.spouseId and a.city NOT LIKE b.city"
val allLongDistanceCouplesFromSS = sqlContext.sql(allLongDistanceCouplesQ).show
