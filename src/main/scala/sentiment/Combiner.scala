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


object Combiner {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.getOrCreate()
    val sc = SparkContext.getOrCreate()

    val inputPath = "hdfs://indianapolis:62120/project/results/"
    val inputFile_1 = "sentiment_analysis_Answers_out/data_out.csv"
    val inputFile_2 = "extractDetails_Answers/data_out.csv"

    val inputBlob_1 = inputPath.concat(inputFile_1)
    val inputBlob_2 = inputPath.concat(inputFile_2)

    val outputPath = "hdfs://indianapolis:62120/project/results/Combine_answers"

    import spark.implicits._
    val df_1 = spark.read.format("csv")
      .option("header", "false")
      .option("multiline", "true")
      .option("escape" , "\"")
      .option("inferSchema", "true")
      .load(inputBlob_1)

    //df_1.show()

    import spark.implicits._
    val df_2 = spark.read.format("csv")
      .option("header", "true")
      .option("multiline", "true")
      .option("escape" , "\"")
      .option("inferSchema", "true")
      .load(inputBlob_2)

    //df_2.show()

    val temp_1 = df_1.withColumn("temp", monotonically_increasing_id())
    val temp_2 = df_2.withColumn("temp", monotonically_increasing_id())

    val combined = temp_1.join(temp_2, "temp").drop("temp")

    combined.write.format("com.databricks.spark.csv").option("header", "true").save(outputPath)

  }
}
