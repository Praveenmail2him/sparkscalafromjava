

import java.io.File

import scala.collection.Parallelizable

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import opennlp.tools.cmdline.PerformanceMonitor
import opennlp.tools.cmdline.postag.POSModelLoader
import opennlp.tools.postag.POSModel
import opennlp.tools.postag.POSSample
import opennlp.tools.postag.POSTaggerME

object PosTagger  {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("NLP Spark");
    val context = new SparkContext(conf);
    val modelpath = "D:\\HDFS\\en-pos-maxent.bin"
    val modelfile = new File(modelpath)
    val model = new POSModelLoader().load(modelfile)
    val monitor = new PerformanceMonitor(System.err, "Sent")
    val tagger = new POSTaggerME(model)
    monitor.start();
    val mydata = context.textFile("D:\\HDFS\\small.txt", 4).cache()
    val words = mydata.flatMap(line => line.split("\\s+")).map(word => word)
    val tags = words.map(word => tagger.tag(word))
    
    //val posSamplle = new POSSample(words.toArray, tags)
    //val results = context.
    //tags.saveAsTextFile("hdfs://IMPETUS-DSRV02.IMPETUS.CO.IN:9000/spark/postag")
  }
}
