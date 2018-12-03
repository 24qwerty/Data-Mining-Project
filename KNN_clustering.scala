import java.io.PrintWriter
import java.io.File
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import scala.collection.mutable

object KNN_clustering {
  def main(args: Array[String]): Unit = {

    val start_time = System.nanoTime

    val conf = new SparkConf().setAppName("KMeans").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sc.setLogLevel("ERROR")

    //Test data
    val df_test = sc.textFile("/Users/viralithakkar/Desktop/INF553/Project/Yelp_Updated_Data/subset_cross.csv")
    val testHeaderAndRows = df_test.map(line => line.split(",").map(_.trim)).map(x => (x(1), x(2)))
    val testHeader = testHeaderAndRows.first
    val testData = testHeaderAndRows.filter(_._1 != testHeader._1).collectAsMap()

    //Overall business users average
    val overall_avg = 3.87

    //business stars
    val df_business = sqlContext.read.json("/Users/viralithakkar/Desktop/INF553/Project/Yelp_Updated_Data/business.json")
    val df_business_new = df_business.filter(df_business("categories").contains("Restaurants")).rdd.map(x => (x(2).toString)).toLocalIterator.toList
    val df_reviews_business = df_business.rdd.map(x => (x(2).toString, x(13).toString)).collectAsMap()
    println("filtered")
    println(df_reviews_business.size)

    //reviews
    val df = sqlContext.read.json("/Users/viralithakkar/Desktop/INF553/Project/Yelp_Updated_Data/review.json")
//    df.filter(df_reviews_business.contains(df("business_id").toString()).equals(true))
    val df_reviews = df.filter(df("date") < "2017-01-01").filter(df("business_id") isin(df_business_new:_*)).rdd.map(x => (x(8).toString, (x(0).toString, x(5).toString))).sample(false, 0.01, 1L)
    println(df_reviews.count())

    //user stars
    val df_users = sqlContext.read.json("/Users/viralithakkar/Desktop/INF553/Project/Yelp_Updated_Data/user.json")
    val df_reviews_users = df_users.rdd.map(x => (x(20).toString, x(0).toString)).collectAsMap()

    //Predicted Result
    val user_business_predicted = new mutable.HashMap[(String, String), Double]()

    val user_business = df_reviews.groupByKey()
    val user_business_map = user_business.collectAsMap()
    println(user_business.count())

    val similar_users = user_business_map.foreach { key =>
      //      println("hey")
      val users_sim = user_business.map { case (user_id: String, x) => (user_id, x ++ key._2) }.flatMap { case (user_id: String, x) => x.toList.map { case (business_id: String, stars: String) => ((user_id, business_id), stars.toDouble) } }.groupByKey().map {
        case ((user_id: String, business_id: String), x) =>
          val x_list = x.toList
          ((user_id, business_id), if (x_list.size > 1) Math.pow(x_list(0) - x_list(1), 2) else Math.pow(x_list(0), 2))
      }.map { case ((user_id: String, business_id: String), stars: Double) => (user_id, (stars, 1)) }.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map {
        case (user_id: String, (stars: Double, count: Int)) => (user_id, Math.sqrt(stars) / count)
      }.map { case (user_id: String, stars: Double) => ((key._1, user_id), stars) }

      val top_sim_users = users_sim.map { case ((user1: String, user: String), stars: Double) => (user1, (user, stars)) }.groupByKey().map { case (user1: String, user_star_list) => (user1, user_star_list.toList.sortBy(_._2).take(10)) }.collectAsMap()
      val user_business_star_map = user_business.map { case (user_id: String, x) => (user_id, x.toMap) }.collectAsMap()

      val final_ratings = df_reviews_business.foreach { key1 =>
        val sim_users = top_sim_users.get(key._1)
        var count = 0
        var summ = 0.0
        sim_users.toList.foreach { sim_user_stars =>
          sim_user_stars.foreach { similar_user =>

            val business_stars = user_business_star_map.get(similar_user._1)
            var aa = 0.0

            if(business_stars.toList(0).contains(key1._1)) {
              aa = business_stars.get(key1._1).toDouble
              count = count + 1
            }
            summ = summ + aa
          }
        }

        var val_ans = 0.0
        if (summ == 0.0 ) {
          val_ans = overall_avg + (key1._2.toDouble - overall_avg) + (df_reviews_users.get(key._1).toList(0).toDouble - overall_avg)
        }
        else
          val_ans = (summ / count)

        val ans_key = (key1._1, key._1)
        user_business_predicted.put(ans_key, val_ans)
      }

    }
    //    testData.foreach(println)

    //println(df_reviews_users.get("KQ8lQEjsSTKcG8phLJZm5g").toList(0).toDouble)
    testData.foreach{ key =>
      if(!user_business_predicted.contains((key._1, key._2))){
        if(df_reviews_users.contains(key._2))
          user_business_predicted.put((key._1, key._2), overall_avg + (df_reviews_business.get(key._1).toList(0).toDouble - overall_avg)+ (df_reviews_users.get(key._2).toList(0).toDouble - overall_avg))
      }
    }

    val out = new PrintWriter(new File("yelp_result.csv"))
    out.write("business_id,user_id,stars")
    out.write("\n")
    user_business_predicted.foreach{ key =>
      out.write(key._1._1 + "," + key._1._2 + "," + key._2)
      out.write("\n")
    }
    //    out.write(user_business_predicted.mkString("\n"))
    out.close()
    //      user_business_predicted.foreach(println)
    //      println(user_business_predicted.size)

    val end_time = System.nanoTime()
    val time = (end_time - start_time) / 1000000000
    println("Time: " + time + " sec")
  }
}
