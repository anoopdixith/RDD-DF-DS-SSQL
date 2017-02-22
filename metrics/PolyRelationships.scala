// Poly marriages
println("\nRDDs")

val polyMarriagesFromRDD = citizensRDD.map(a => (a.uniqueId, a.name))
                                      .join(citizensRDD.map(a => (a.spouseId, a.name)))
                                      .map(a => a._2)
                                      .groupBy(a => a._1)
                                      .filter(a => a._2.toList.length > 1)
                                      .map(a => a._2.toList)
                                      .collect
                                      .foreach(a => println(a))
                                      


println("\nDataFrames and DataSets")
val polyMarriagesFromDF = citizensDF.as("A").join(citizensDF.as("B"), $"A.uniqueId" === $"B.spouseId")
                              .groupByKey(row => row.getString(1))
                              .mapGroups((k,v) => (v.mkString(" AND ")))
                              .toDF("PolyRelationships")
                              .as("polySpouses")
                              .filter("polySpouses.PolyRelationships like '%AND%'")
                              .show(false)


println("\nSparkSQL")
val polyMarriagesQ = "select * from (select collect_list(c2.name) spouse_of, first(c1.name) is from citizens c1 join citizens c2 on c1.uniqueId = c2.spouseId group by c2.spouseId) where count(c2.name) > 1"
val polyMarriagesFromSS = sqlContext.sql(polyMarriagesQ).show(false)

