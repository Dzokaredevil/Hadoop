import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import SparkContext._
import java.io.File
import java.net.URI
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Scanner;

object Main {
	def main(args: Array[String]){
		val appName = "SparkSpec"
		val jars = List(SparkContext.jarOfObject(this).get)
		println(jars)
		val conf = new SparkConf().setAppName(appName).setJars(jars)
		val sc = new SparkContext(conf)
		if(args.length < 2){
			println(args.mkString(","))
			println("ERROR. Please, specify input and output directories.")
		}else{
			val inputDir = args(0)
			val outputDir = args(1)
			println("Input directory: " + inputDir)
			println("Output directory: " + outputDir)
			run(sc, inputDir, outputDir)
		}
	}
	val GLUSTERFS_MOUNT_PATH = "/mnt/root"
	val FILENAME_PATTERN = "([0-9]{5})([dijkw])([0-9]{4})\\.txt\\.gz".r
	val LINE_PATTERN = "([0-9]{4} [0-9]{2} [0-9]{2} [0-9]{2} [0-9]{2})(.*)".r
	val VARIABLE_NAMES = Array("d", "i", "j", "k", "w")
	def toGlusterfsPath(path: String): String = path.replace(GLUSTERFS_MOUNT_PATH, "")
	def toGlusterfsPath(file: File): String = toGlusterfsPath(file.getAbsolutePath())
	def fmtArr(x: Array[Float]) = "[" + x.mkString(", ") + "]"
	def makeLines( fileNum: String, s: String, array: Array[String] ) = {
		var result: Array[(String, String, String)] = new Array[(String, String, String)](array.length);
		for (i <- 0 to array.length-1){
			result(i) = (fileNum, s, array(i) );
		}
		result
	}
	def fmtStr(s: String) = {
		var scanner : Scanner = new Scanner(s);
		var string : String = new String("[");
		if (scanner.hasNextFloat()){
			string += scanner.nextFloat();
		}
		while(scanner.hasNextFloat()){
			string += ", " + scanner.nextFloat();
		}
		string += "]"
		string
	}
	def run(sc: SparkContext, inputDir: String, outputDir: String) {
		sc
		.wholeTextFiles(inputDir + "/*.gz")
		.map( pair => ((new File(new File(pair._1).toURI().getPath()).getName()), pair._2) )
		.map( pair => (FILENAME_PATTERN.pattern.matcher(pair._1), pair) )
		.filter(pair => pair._1.matches())
		.map( pair => ((pair._1.group(1) , pair._1.group(2)), pair._2._2) )
		.map( pair => (pair._1, pair._2.split("\\n")) )
		.flatMap(pair => makeLines(pair._1._1, pair._1._2, pair._2) )
		.map( pair => (LINE_PATTERN.pattern.matcher(pair._3), pair) )
		.filter(pair => pair._1.matches())
		.map( pair => (( pair._2._1,  pair._1.group(1), pair._2._2 ),( pair._1.group(2))) )
		.groupByKey()
		.map( pair => ((pair._1._1, pair._1._2), ( pair._1._3, pair._2.iterator.next())) )
		.groupByKey()
		.filter(pair => pair._2.size == 5)
		.map( pair => (pair._1._2, pair._2.toMap) )
		.sortByKey()
		.map( pair => (pair._1, pair._2.get("i"), pair._2.get("j"), pair._2.get("k"), pair._2.get("w"), pair._2.get("d")) )
		.map( set => (set._1, set._2.getOrElse("[]"), set._3.getOrElse("[]"), set._4.getOrElse("[]"), set._5.getOrElse("[]"),set._6.getOrElse("[]")) )
		.map( set => (set._1, fmtStr(set._2), fmtStr(set._3), fmtStr(set._4), fmtStr(set._5), fmtStr(set._6)) )
		.map( set => set._1+"\t["+"i="+set._2+"," + "j="+set._3  +"," + "k=" + set._4 + "," + "w=" + set._5 + ","  + "d=" + set._6+ "]" )
		.saveAsTextFile(outputDir)
	}
}
