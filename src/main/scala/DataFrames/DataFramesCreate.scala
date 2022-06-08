package DataFrames

import org.apache.spark.sql.{SaveMode, SparkSession}
//import org.apache.spark.sql.types.{StructField, StructType}

object DataFramesCreate extends  App {

  val path = "C:\\Rakesh\\SparkLearning\\spark\\SparkEssentials-UdemyCourseRJVM\\src\\main\\resources\\data"
  val spark = SparkSession.builder()
    .appName("CreateDataFrames")
    .config("spark.master", "local")
    .getOrCreate()
  val carsDf = spark.read
    .format("json").
    option("inferSchema", "true")
    .load("C:\\Rakesh\\SparkLearning\\spark\\SparkEssentials-UdemyCourseRJVM\\src\\main\\resources\\data\\cars.json")
  carsDf.show()
  carsDf.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("C:\\Rakesh\\SparkLearning\\spark\\SparkEssentials-UdemyCourseRJVM\\src\\main\\resources\\data\\cars_dup.json")
  //reading from remote db
  val employeeDf = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()
  employeeDf.show()
  val movieData = spark.read
    .option("inferSchema", "true")
    .json(path + "/movies.json")
  movieData.show()
  movieData.write
    .format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .mode(SaveMode.Overwrite)
    .option("path", path + "/movies.csv")
    .save()
  movieData.write
    .mode(SaveMode.Overwrite)
    .parquet(path + "/movies.parquet")
  movieData.write
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .save()

}
