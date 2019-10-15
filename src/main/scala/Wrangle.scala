import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.length

object Wrangle {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Please check arguments!");
      System.exit(1);
    }

    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.json(args(0))
    val newDF = df.select("business_id", "name", "address", "city", "state", "postal_code", "latitude", "longitude", "stars", "review_count", "is_open", "categories", "hours")
    
    val canDF = newDF.filter(length($"postal_code") > 5)
    val usDF = newDF.filter(length($"postal_code") === 5)
    
    // this may contain nulls, half filled postal_codes and etc...
    val notsureDf = newDF.filter(length($"postal_code") < 5)
    
    canDF.write.format("json").mode("overwrite").save(args(1) + "can-df")
    usDF.write.format("json").mode("overwrite").save(args(1) + "us-df")

    val storeDF = canDF.select("address", "business_id", "categories", "city", "latitude", "longitude", "name", "postal_code", "review_count", "stars", "state")
    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "??????")
    
    // SHOW VARIABLES WHERE Variable_name = 'port'
    val url = "jdbc:mysql://localhost:3306/elastic?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val table = "yelpCanada"
    
    // .mode("overwrite") prevents same data being index into db
    // but be very careful of this overwriting can be dangerous
    storeDF.write.mode("append").jdbc(url, table, prop)

    spark.stop()  
  }
}
