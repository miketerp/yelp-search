import org.apache.spark.sql.SparkSession

object Wrangle {
  def main(args: Array[String]) {
    /*
    if (args.length < 2) {
      System.err.println("");
      System.exit(1);
    }
    */

    val spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.json("/Users/Kim/Desktop/github/yelp-search/datasets/yelp_academic_dataset_business.json")
    val newDF = df.select("name", "address", "city", "state", "postal_code", "latitude", "longitude", "stars", "review_count", "is_open", "categories", "hours")
    newDF.write.json("/Users/Kim/Desktop/github/yelp-search/datasets/modded-df")

    spark.stop()  
  }
}
