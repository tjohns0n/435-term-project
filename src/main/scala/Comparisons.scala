import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

object Comparisons {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("TermProject")
    val sc = new SparkContext(conf)

    val file: RDD[Array[String]] = sc.textFile(args(0)).map(_.split(","))
    val enrollmentToRD: RDD[(String, String)] = file.map(line => {
      (line(0) + line(1), (line(4).toDouble / line(5).toDouble).toString)
    })
    val capacityToAttendance: RDD[(String, String)] = file.map(line => {
      (line(0) + line(1), (line(3).toDouble / line(6).toDouble).toString)
    })
    val donationsToRD: RDD[(String, String)] = file.map(line => {
      (line(0) + line(1), (line(8).toDouble / line(5).toDouble).toString)
    })
    // donations / 1000
    val enrollmentToDonation: RDD[(String, String)] = file.map(line => {
      (line(0) + line(1), (line(4).toDouble / (line(8).toDouble / 1000)).toString)
    })

    val bySchool: RDD[(String, Iterable[Array[String]])] = file.groupBy(line => line(0))
    val oldToNewCapacity: RDD[(String, String)] = bySchool.map(school => {
      val oldVsNew = (school._2.last(3).toDouble / school._2.head(3).toDouble).toString
      school._2.map(s => (school._1 + s(1), oldVsNew))
    }).flatMap(x => x)

    val wlToDonations: RDD[(String, String)] = bySchool.flatMap(school => {
      val t: Iterator[(String, String)] = for (s <- school._2.iterator.sliding(2)) yield {
        var thisYear = s.head(9).toDouble
        thisYear = if (thisYear > 0) thisYear else 0.0001
        var lastYear = s.last(9).toDouble
        lastYear = if (lastYear > 0) lastYear else 0.0001
        (s.head(0) + s.head(1), ((thisYear / lastYear) / (s.head(8).toDouble / 1000000)).toString)
      }
      t.toList.union(List((school._1 + school._2.last(1), "")))
    })

    val wlToAttendance: RDD[(String, String)] = file.map(s => {
      var wl = s(9).toDouble
      wl = if (wl > 0) wl else 0.0001
      (s(0) + s(1), (wl / s(6).toDouble).toString)
    })

    println(wlToAttendance.collect().mkString("\n"))

    val flatten = (s: (String, (List[String], String))) => {(s._1, s._2._1 :+ s._2._2)}

    val all: RDD[String] = enrollmentToDonation.join(capacityToAttendance).map(s => (s._1, List(s._2._1, s._2._2)))
      .join(oldToNewCapacity).map(flatten)
      .join(donationsToRD).map(flatten)
      .join(enrollmentToRD).map(flatten)
      .join(wlToDonations).map(flatten)
      .join(wlToAttendance).map(flatten)
      .sortByKey()
      .map(s => s._1 + "," + s._2.mkString(","))

    println(all.collect.mkString("\n"))
    all.saveAsTextFile("temporaryOut/")

    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path("temporaryOut/"), hdfs, new Path(args(1)), false, hadoopConfig, null)
  }

}
