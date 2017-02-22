// Match the citizens to their employments
val onboardMatchRDD = employeesRDD.map(a => (a.citizenId, a))
                        .join(citizensRDD.map(a => (a.uniqueId, a)))
                        .map(a => (a._2._1, a._2._2))
val onboardAllRDD = employeesRDD.map(a => (a.citizenId, a))
                        .rightOuterJoin(citizensRDD.map(a => (a.uniqueId, a)))
                        .map(a => (a._2._1, a._2._2))
println("\nRDDs")
onboardMatchRDD.collect.foreach(println)
println("=================")
//onboardAllRDD.collect.foreach(println)
println("\nDataFrames")
val onboardMatchDF = citizensDF.join(employeesDF, $"uniqueId" === $"citizenId")
onboardMatchDF.show
val onboardAllDF = citizensDF.join(employeesDF, $"uniqueId" === $"citizenId", "left_outer")
//onboardAllDF.show

