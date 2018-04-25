//spark-shell --master yarn-client --executor-memory 2G --executor-cores 6 --num-executors 6
//val List_Friends=sc.textFile("/Users/Saumya/Documents/BDandML/people.txt")


spark-shell --executor-memory 8G --driver-memory 2G

val file=sc.textFile("/user/sg5290/soc-LiveJournal1Adj.txt")

val allfriends = file.map(line=>line.split("\\s+")).filter(line => (line.size == 2)).map(line=>(line(0),line(1).split(","))).flatMap(x=>x._2.flatMap(z=>Array((x._1.toInt,z.toInt))))

val friendsjoin = allfriends.join(allfriends)

val onlyfriends = friendsjoin.map(elem => elem._2).filter(elem => elem._1 != elem._2)

val mutualfriends = onlyfriends.subtract(allfriends)

val recommendfriends = mutualfriends.map(value => (value, 1)).reduceByKey((a, b) => a + b)

def Sort(friends: List[(Int, Int)]) : List[Int]   = {
friends.sortBy(allPair => (-allPair._2, allPair._1)).map(allPair => allPair._1)
}

val Finalrecommendation = recommendfriends.map(elem => (elem._1._1, (elem._1._2, elem._2))).groupByKey().map(triplet => (triplet._1, Sort(triplet._2.toList))).map(triplet => triplet._1.toString + "\t" + triplet._2.map(x=>x.toString).toArray.mkString(","))

//.collect.mkString(",")
Finalrecommendation.saveAsTextFile("outputFriends.txt")
hadoop fs -getmerge /user/sg5290/outputFriends.txt /home/sg5290/outputFriends.txt