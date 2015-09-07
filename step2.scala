import math.log
import org.apache.spark._

//def main(args: Array[String]) {
//val totalCount = sc.textFile("s3n://s15-p42-part2/wikipedia_arcs").count()
val file = sc.textFile("s3n://madpigaaron/project4part2/tiny_arcs")
val totalCount = file.distinct().count()
val links = file.map( s => {
  val parts = s.split("\t")
  (parts(0),parts(1))}).distinct().groupByKey().persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)

val nodes = scala.collection.mutable.ArrayBuffer.empty ++ links.keys.collect()
val newNodes = scala.collection.mutable.ArrayBuffer[String]()
for {s <- links.values.collect()  
     k <- s if (!nodes.contains(k))} {  
    nodes += k
    newNodes += k
}
val linkList = links ++ sc.parallelize(for (i <- newNodes) yield (i, List.empty))
val nodeSize = linkList.count()


var ranks = links.mapValues(v=>1.0)
//var danglingCount = totalCount - parts.count()

for (i <- 1 to 10) {
  val dangling = sc.accumulator(0.0)
  val contribs = links.join(ranks).values.flatMap {
    case (urls, rank) =>  {
      val size = urls.size
      if (size == 0){
        dangling += rank
        List()
      }else{
        urls.map(dest => (dest,rank/size))
      }
    }
  }
  contribs.count()
  val danglingValue = dangling.value
  ranks = contribs.reduceByKey(_+_).mapValues(score => 0.15+0.85*(score+(danglingValue/nodeSize))).cache()
}


//}

//main(new Array[String](1))