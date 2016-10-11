import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object DemoSpark {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
             .setMaster("local")
             .setAppName("DemoSparkSession")
    val sc=new SparkContext(conf);
    val lines=sc.textFile("D://HDFS//ReadMeSample.txt");
    val counts = lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_);
    counts.toDebugString
    val shw= counts.count()
    println(">>>>>>>word count>>>>>>>"+shw)
    //Saving in OutputPos Directory In IDE Project Path
    counts.saveAsTextFile("outputpos");
   }
}