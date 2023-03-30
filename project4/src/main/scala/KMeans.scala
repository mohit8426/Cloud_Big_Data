import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object KMeans {
  type Point = (Double, Double)

  var centroids: Array[Point] = Array[Point]()

  def distance(x: Point, y: Point): Double =
    /* return the Euclidean distance between x and y */
    Math.sqrt(Math.pow(x._1 - y._1, 2) + Math.pow(x._2 - y._2, 2))

  def closest_centroid(p: Point, cs: Broadcast[Array[Point]]): Point =
    /* return a point c from cs that has the minimum distance(p,c) */ {
    cs.value.minBy(distance(p, _))
  }

  def main(args: Array[String]) {
    val config = new SparkConf().setAppName("KMeans_Project4")
    val sprk_context = new SparkContext(config)
    // spliting the delimitter and reading line by lines for RDD point 
    val points_line: RDD[Point] = sprk_context.textFile(args(0)).map(_.split(",")).map(x => (x(0).toDouble, x(1).toDouble))
    // splitting the delimitter and reading line for array point 
    val initial_centroids: Array[Point] = sprk_context.textFile(args(1)).map(_.split(",")).map(x => (x(0).toDouble, x(1).toDouble)).collect()
    centroids = initial_centroids
   

    for (i <- 1 to 5) {
      /* broadcast centroids to all workers */
      
      val broad_centroids = sprk_context.broadcast(centroids)

      /* find new centroids using KMeans */
      centroids = points_line.map { p =>
        val cs = broad_centroids.value
        (closest_centroid(p, broad_centroids), p)
      }
        .groupByKey()
        .map { case (centroid, cluster) =>
          val (x, y) = cluster.reduce((a, b) => (a._1 + b._1, a._2 + b._2))
          (x / cluster.size, y / cluster.size)
        }
        .collect()
    }

    centroids.foreach { case (x, y) => println(f"\t$x%2.2f\t$y%2.2f") }
    sprk_context.stop()
  }
}
