import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.clustering.{KMeans, StreamingKMeans}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Cluster {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Streaming").setMaster("local")
    val ssc = new StreamingContext(conf, Seconds(1))

   

  }

}
