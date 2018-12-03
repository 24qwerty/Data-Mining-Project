import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object testing {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KMeans").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sc.setLogLevel("ERROR")

    val df_mexican = sc.textFile("/Users/viralithakkar/Desktop/INF553/Project/Yelp_Updated_Data/mexican_businesses.csv")
    val testHeaderAndRows_mexican = df_mexican.map(line => line.split("\t").map(_.trim)).map(x => x(1))
    val testHeader_mexican = testHeaderAndRows_mexican.first
    val data_mexican = testHeaderAndRows_mexican.filter(_ != testHeader_mexican).toLocalIterator.toList

    val df_business = sqlContext.read.json("/Users/viralithakkar/Desktop/INF553/Project/Yelp_Updated_Data/business.json")
    val df_business_new = df_business.filter(df_business("business_id") isin(data_mexican:_*)).rdd.map(x => (x(2).toString)).toLocalIterator.toList

    val df = sqlContext.read.json("/Users/viralithakkar/Desktop/INF553/Project/Yelp_Updated_Data/review.json")
    val df_review_new = df.filter(df("date") >= "2017-01-01").filter(df("business_id") isin(df_business_new:_*)).rdd.map(x => (x(8).toString)).toLocalIterator.toList
    val df_reviews = df.filter(df("date") >= "2017-01-01").filter(df("business_id") isin(df_business_new:_*)).rdd.map(x => (x(8).toString, (x(0).toString, x(5).toString)))//.sample(false, 0.001, 1L)
    println(df_reviews.count())
    val test_business_user_pairs = df_reviews.map{ case(user_id:String, (business_id:String, stars:String))=>((business_id, user_id),stars)}.collectAsMap()


    val df_test = sc.textFile("/Users/viralithakkar/Desktop/INF553/Project/Yelp_Updated_Data/yelp_result 2.csv")
    val testHeaderAndRows = df_test.map(line => line.split(",").map(_.trim)).map(x => (x(1), x(2))).collectAsMap()
//    val testHeader = testHeaderAndRows.first
//    val testData = testHeaderAndRows.filter(_._1 != testHeader._1).collectAsMap()
    println(testHeaderAndRows.size)

    val total_count = test_business_user_pairs.size
    var count = 0

    testHeaderAndRows.foreach{ key =>
      if(test_business_user_pairs.contains(key))
        count = count + 1
    }
    println(count)
    println(total_count)
  }
}
