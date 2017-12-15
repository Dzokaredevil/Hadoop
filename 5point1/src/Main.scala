import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import SparkContest._

object Main{
    def main (args: Array[String]){
        val appName = "SparkWordCount"
        val jars = List(SparkContext.jarOfObject(this).get)
        println(jars)
        val conf = new SparkConf().setAppName(appName).setJars(jars)
        val sc = new SparkContext(conf)
        if (args.length < 2){
            println(args.mkString(","))
            println("ERROR. Please, specify input and output directories.")
        } else {
            val inputDir = args(0)
            val outputDir = args(1)
            println("Input directory: " + inputDir)
            println("Output directory: " + outputDir)
            run(sc, inputDir, outputDir)
        }
    }
    def run(sc: SparkContext, inputDir: String, outputDir: String){
        val maxEl = sc
        .textFile(inputDir + "/*")
        .flatMap(line => line.split("\\W+"))
        .filter(word => !word.isEmpty())
        .zipWithIndex()
        .map(_.swap)
        .flatMap(pair => Array(pair, (pair._1 + 1, pair._2)))
        .groupByKey()
        .filter(it => (it._2size == 2))
        .map(_.swap)
        .map(pair => (pair._1, 1))
        .reduceByKey((a,b) => a+b)
        .sortBy(_._2, false)
        .first()
        // ((a, b), 2)
        val rdd = sc
        .parallelize(Array(maxEl._1))
        // (a, b)
        .map(it => it.mkString(" "))
        .saveAsTextFile(outputDir)
    }
}
