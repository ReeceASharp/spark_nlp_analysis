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

object SparkNLP {
    def main(args: Array[String]) {
        //print("STARTING PROGRAM ")
        val spark = SparkSession.builder.getOrCreate()
        val sc = SparkContext.getOrCreate()

        // Read in data, save as dataset (Will work with either Answers.csv, or Questions.csv)
        import spark.implicits._ 
        val df = spark.read.format("csv")
            .option("header", "true")
            .option("multiline", "true")
            .option("escape" , "\"")
            .option("inferSchema", "true")
            .load("hdfs://indianapolis:62120/project/data/Question.csv")

        //break up the dataset, 28 cores, 3 partitions created per core to leverage Spark
        val df_partition = df.repartition(3 * 28)

        val ds = df_partition.select($"Body").map(line => { val body = line.getAs[String]("Body").toLowerCase()
            body.replaceAll("\\.|<[^>]*>|,|\\?", " ")
        })
        //println("DS OUTPUT")

        // set REGEX for Normalizer
        val reg_str = Array("<[^>]*>", "[^A-Za-z]")

        // raw data -> document -> token -> normalized tokens -> sentiments
        //############### First Pipeline

        //annotates the dataset, allowing other ML functions to come in and manipulate it
        val documentAssembler = new DocumentAssembler()
            .setInputCol("value")
            .setOutputCol("document")
            .setCleanupMode("shrink")
        
        //tokenizes the dataset
        val tokenizer = new Tokenizer()
            .setInputCols("document")
            .setOutputCol("token")
            .setContextChars(Array("(", ")", "?", "!", "."))
            .setSplitChars(Array("-"))

        //normalizes the dataset, also performs a regex expression
        val normalizer = new Normalizer()
            .setInputCols("token")
            .setOutputCol("normalized")
            .setCleanupPatterns(reg_str)

        //Repackages the dataset into something that can be used by the second pipeline
        val finisher = new Finisher()
            .setInputCols("document")
            .setOutputCols("text_array")

        //println("TRANSFORMING")
        //val pipeline = new Pipeline().setStages(Array(tokenizer, normalizer, finisher))
        val pipeline = new Pipeline()
            .setStages(Array(documentAssembler, tokenizer, normalizer, finisher))

        //val result = pipeline.fit(Seq.empty[String].toDS.toDF("finish")).transform(testing)

        val data_normalized = pipeline.fit(ds).transform(ds)
        //println("DATA_NORMALIZED OUTPUT")
        //data_normalized.show(false)

        //###############

        //grab data from normalized data, and convert back to a string for use in the next pipeline
        val data_to_analyze = data_normalized.select($"text_array")

        //creates a DF column of strs called "text"
        val normalize_str = data_to_analyze.withColumn("text_upper", concat_ws(" ",col("text_array")))
        //data.withColumn("friends", concat_ws("",col("friends")))


        val lowercase_str = normalize_str.withColumn("text", lower(col("text_upper")))

        //################ SECOND PIPELINE
        //Load it from the HDFS instead of fetching a download each time
        val sentiment_model = PipelineModel.load("hdfs://indianapolis:62120/sentiment/analyze_sentiment_en_2.4.0_2.4_1580483464667")

        //Run the data through, needs a DF column of strs called "text"
        val data_analyzed = sentiment_model.transform(lowercase_str)
        //println("DATA_ANALYZED OUTPUT")
        //data_analyzed.show(false)

        //grab the results from the sentiment analysis
        val sentiment_results = data_analyzed.select("Sentiment.result")

        val results_str = sentiment_results.withColumn("Sentiment", concat_ws(", ",col("result")(0)))

        //sentimentModel = PretrainedPipeline.load("analyze_sentiment", lang="en")

        val final_result = results_str.select($"Sentiment")

        final_result.write.csv("hdfs://indianapolis:62120/project/results/sentiment_analysis_Question_out_repartition")
    }


    def merge(srcPath: String, dstPath: String): Unit =  {
        val hadoopConfig = new Configuration()
        val hdfs = FileSystem.get(hadoopConfig)
        FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), true, hadoopConfig, null) 
        // the "true" setting deletes the source files once they are merged into the new output
    }
}

