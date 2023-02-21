package Lab1

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.nio.file.{Paths, Files}
import java.sql.Timestamp

object Lab1 {
  case class Breweries(
      city: String,
      breweries: Integer
  )

  def main(args: Array[String]) {
    val schema = StructType(
      Array(
        StructField("city", StringType),
        StructField("breweries", IntegerType)
      )
    )
    
    // Default values
    var inputFile : String = "./project/data/zuid-holland.orc"
    var outputFile : String = "breweries"
    var partitions : Int = 8

    for (value <- 0 to args.length-1){
      if (args(value).startsWith("-i")){
        if (value + 2 > args.length){
          println("Enter a valid input value")
          println("Using default value: " + inputFile)
        }
        if (Files.exists(Paths.get(args(value + 1 )))){
          inputFile = args(value + 1 )
        }
        else {
          println("Enter a valid input file")
          println("Using default value: " + inputFile)
        }
      }
      if (args(value).startsWith("-o")){
        if (value + 2 > args.length){
          println("Enter a valid output value")
          println("Using default value: " + outputFile)
        }
        outputFile = args(value + 1 )
      }
      if (args(value).startsWith("-p")){
        if (value + 2 > args.length){
          println("Enter a valid number of partitions")
          println("Using default value: " + partitions)
        }
        partitions = args(value + 1 ).toInt
      }
    }

    // Inputs print
    println("Input file path: " + inputFile)
    println("Output file name: " +outputFile)
    println("Number of partitions: " +partitions)

    val spark = SparkSession.builder
      .appName("Lab 1")
      .getOrCreate()
    val sc = spark.sparkContext // If you need SparkContext object

    import spark.implicits._

    // Read input data
    val df = spark.read
      .format("orc")
      .load(inputFile)

    // Filter the data for the breweries
    val brewery = df
      .select("id", "tags", "lat", "lon")
      .filter(
        ($"tags".getItem("craft").equalTo("brewery")) ||
          ($"tags".getItem("microbrewery").equalTo("yes")) ||
          ($"tags".getItem("name").contains("brouwerij"))
      )
      .withColumn("city", $"tags".getItem("addr:city"))

    brewery.cache
    brewery.createOrReplaceTempView("cities")

    // Obtaining a list of ALL the cities and its coordinates
    df.withColumn("city", $"tags".getItem("addr:city"))
      .createOrReplaceTempView("citiesCoord")

    // Obtaining all the filtered values that has not any city
    val noCities = spark
      .sql(
        "SELECT city, lat, lon FROM cities where city is null"
      )
      .select(
        $"city" as "city",
        $"lat".cast("Double"),
        $"lon".cast("Double")
      )

    // Obtaining the average center of each city
    val citiesCoord = spark
      .sql(
        "SELECT city, avg(lat) as lat, avg(lon) as lon FROM citiesCoord where city is not null group by city"
      )
      .select(
        $"city" as "cityC",
        $"lat".cast("Double") as "latC",
        $"lon".cast("Double") as "lonC"
      )
    
    citiesCoord.cache

    // joining and filtering just for the cities that are actually near the given coordinates
    val df3 = noCities
      .repartition(partitions)
      .join(
        citiesCoord.repartition(partitions),
        abs(noCities("lat") - citiesCoord("latC")) < 0.1 && abs(
          noCities("lon") - citiesCoord("lonC")
        ) < 0.1,
        "left"
      )
    df3.createOrReplaceTempView("citiesDist")

    // Define an udf to calculate distance
    val distance = (latC: Double, lonC: Double, latN: Double, lonN: Double) => {
      Math.sqrt(Math.pow((lonC - lonN), 2) + Math.pow((latC - latN), 2))
    }

    // The function is registered
    spark.udf.register("distance", distance)

    // Calculating the distance to each of the possible cities
    val citiesDist = spark.sql(
      "select cityC, city,lat,lon, distance(lat,lon,latC,lonC) as result from citiesDist"
    )
    citiesDist.createOrReplaceTempView("reduce")

    // Selecting the nearest city given it's coordinates
    val ans =
      spark.sql("select cityC, lat as latC, lon as lonC, result from reduce")
    val minDist = spark.sql(
      "select lat, lon, min(result) as result from reduce group by lat, lon"
    )
    val newCities = minDist
      .repartition(partitions)
      .join(ans.repartition(partitions), ans("result") === minDist("result"), "left")
      .select($"cityC".as("city"), $"lat", $"lon")

    // Obtaining the total list of city nodes with breweries in it
    val total = brewery.select($"city").union(newCities.select($"city"))
    total.createOrReplaceTempView("cities")

    // Getting the final count
    val finalCount = spark
      .sql(
        "SELECT city, count(*) as breweries_number FROM cities where city is not null GROUP BY city ORDER BY breweries_number desc"
      )
      .select(
        $"city" as "city",
        $"breweries_number".as("breweries").cast("Int")
      )
      .as[Breweries]

    // Writing the answer
    finalCount.explain(true)
    finalCount.write.orc(outputFile)

    spark.stop
  }
}
