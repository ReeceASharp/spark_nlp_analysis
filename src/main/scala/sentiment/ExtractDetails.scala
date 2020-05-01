package sentiment

import com.johnsnowlabs.nlp.base._
import com.johnsnowlabs.nlp.annotator._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

object ExtractDetails {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.getOrCreate()
    val sc = SparkContext.getOrCreate()

    val inputPath = "hdfs://indianapolis:62120/project/data/"
    val inputFile = "Answers.csv"
    val inputBlob = inputPath.concat(inputFile)

    val outputPath = "hdfs://indianapolis:62120/project/results/extractDetails_Answers"

    import spark.implicits._
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("multiline", "true")
      .option("escape" , "\"")
      .option("inferSchema", "true")
      .load(inputBlob)

    //val df_repartition = df.repartition(3 * 28)

    //df_partition.show()

    val extract = df.select($"Id", $"OwnerUserId", $"Score")

    //extract.show()

    extract.write.format("com.databricks.spark.csv").option("header", "true").save(outputPath)
  }
}