import scala.collection.mutable
import math.log


def main(args: Array[String]){
val file = sc.textFile("s3n://s15-p42-part1-easy/data/")
//val file = sc.textFile("s3n://madpigaaron/project4part2/1liner")
//val tf = file.flatMap(line => line.split("\t")(3).replaceAll("\\<.*?\\>"," ").replace("\\n"," ").toLowerCase().replaceAll("[^A-Za-z\\s]"," ").split("\\s+").map(wd => ((wd,line.split("\t")(1)),1))).reduceByKey(_+_).persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
val tf = file.map(_.split("\t")).map(line => (line(1),line(3).replaceAll("\\<.*?\\>"," ").replace("\\n"," ").toLowerCase().replaceAll("[^A-Za-z\\s]"," ").trim())).flatMap(line => line._2.split("\\s+").map(word => ((word,line._1),1))).reduceByKey(_+_).persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)



val totalDocCount = tf.map{case ((word,title),ct) => title}.distinct.count()
val wordDocCount = tf.map{case ((word,title),ct) => (word,1)}.reduceByKey((x,y)=>x+y).persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
val tfidf = wordDocCount.join(tf.map {
  case ((word, title), ct) => (word, (title, ct))
}).map {
  case (word, (wdct, (title, ct))) => ((word, title),ct*log(totalDocCount*1.0/wdct))
}.filter(x => x._1._1 == "cloud").collect()
//.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)

//tf.unpersist()
//wordDocCount.unpersist()
val top100 = tfidf.sortBy(x => (x._2,x._1._2))(Ordering.Tuple2(Ordering.Double.reverse, Ordering.String)).take(100)
//val filtered = tfidf.filter(x => x._1._1 == "yes").collect()
//val top100 = filtered.sortBy(x => (x._2,x._1._2))(Ordering.Tuple2(Ordering.Double.reverse, Ordering.String)).take(100)
top100.foreach(line=>println(line))
}

main(new Array[String](1))