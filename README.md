# Lab 1 Report
## Usage
The spark application is compiled and packaged inside sbt. 

The app has the following default parameters:
1. Input file path: "./project/data/zuid-holland.orc"
2. Output file name: "breweries"
3. Number of partitions: 8

But they can be modified with the following input parameters:

- '-p' <-- Number of partitions
- '-i' <-- Input file path
- '-o' <-- Output folder name


However, we are unable to run it inside sbt, we have to use the docker command to run the application. Screenshots are given below:

![usage image 1](https://github.com/abs-tudelft-sbd20/lab-1-group-29/blob/Geolocate-city/usage%20image%201.png)

The compiled application was run over a dockerized spark instance as shown below. This allows us to run it on any platform with docker/spark installed on it.
```scala
sudo docker run -it --rm -v "`pwd`":/io -v "`pwd`"/spark-events:/spark-events spark-submit target/scala-2.12/lab-1_2.12-1.0.jar
```

![usage image 2](https://github.com/abs-tudelft-sbd20/lab-1-group-29/blob/Geolocate-city/usage%20image%202.png)

The application generates a **breweries** folder with the output orc files and a _SUCCESS_ file.

![usage image 3](https://github.com/abs-tudelft-sbd20/lab-1-group-29/blob/Geolocate-city/usage%20image%203.png)

## Functional overview

Our program can be divided into 4 steps, each explained in detail in their own subsections.
1. Prepare the data.
2. Get the list of breweries for cities that have a name.
3. Geo-location for cities that do not have a name (NULL).
4. Assign a city name to the ones that are NULL and write the final output.
### Step 1: Prepare the data
As noted in the lab assignment guide, the Zuid-Holland (i.e., South Holland) input file was in a .osm.pbf format (osm - OpenStreetMap). We converted this to a .orc file using the method outlined in the introduction.
We converted the input file before running the code.
### Step 2: Get the list of breweries for cities that have a name 
We define the main function and the output schema provided in the lab guide:
```scala
def main(args: Array[String]) {
    val schema = StructType(
      Array(
        StructField("city", StringType),
        StructField("breweries", IntegerType)
      )
    )
```

Then we create a spark session named "Lab 1" and a sparkContext object:
```scala
val spark = SparkSession.builder
      .appName("Lab 1")
      .getOrCreate()
    val sc = spark.sparkContext
```

Then we read the input file and filter the data to include only breweries:
- First we declare a variable "df" in the orc format on line 3 in the code block below. We then load the South Holland data-set (converted to .orc in step 1) on line 4. 

- The next step is to filter the data so that only cities with breweries are retained. We are also fetching the latitude and longitude on line 9. The reason for this is Geo-location, which is explained in Step 3. This filtering is where we spent quite a while trying to improve the accuracy of the output to the best of our abilities, given the limited time we had. 

- Initially we only had the tag "craft" filtered by "brewery" in our SQL. This method fetched a very limited output, only around 11 or 12 entries in total. Upon scrutinising the input data from OpenStreetMap, we realised that adding the terms "industrial" and "microbrewery" to the spark SQL would result in a much better output.

- The issue with our initial attempt was that it fetched more than just breweries. The query fetched bakeries and even cemeteries! Also, as you can see, the original SQL was quite complicated compared to the modified version which we ended up using. Needless to say, we had to be very careful with the parentheses and ran into more than one compilation error due to missing or extra parentheses.
```scala
// This was our initial attempt at filtering to get the data we need
val breweries = df.select("id","tags")
                  .filter(($"tags".getItem("craft").geq("brewery") && $"tags".getItem("craft").leq("brewery")) 
                          ($"tags".getItem("industrial").geq("brewery") && $"tags".getItem("industrial").leq("brewery")) 
                          ($"tags".getItem("micro_brewery").geq("yes") && $"tags".getItem("micro_brewery").leq("yes")))
```
```scala
// Read input data
    val df = spark.read
      .format("orc")
      .load("./project/data/zuid-holland.orc")

// This is the version that we ended up using
    // Filter the data for the breweries
    val brewery = df
      .select("id", "tags", "lat", "lon")
      .filter(
        ($"tags".getItem("craft").equalTo("brewery")) ||
          ($"tags".getItem("microbrewery").equalTo("yes")) ||
          ($"tags".getItem("name").contains("brouwerij"))
      )
      .withColumn("city", $"tags".getItem("addr:city"))
```
```scala
brewery.cache
brewery.createOrReplaceTempView("cities")
```
The purpose of "brewery.cache" is explained in the performance section below. 
"The commandcreateOrReplaceTempView creates (or replaces if the name already exists) a lazily evaluated "view" that can then be used like a hive table in Spark SQL" [source](https://stackoverflow.com/questions/44011846/how-does-createorreplacetempview-work-in-spark).  Here we create a view named **"cities"**. This view is used for further querying as we will see in subsequent sections.
### Step 3: Geo-location for cities that do not have a name (NULL)
First we fetched a list of ALL cities and their coordinates and create a new view named "citiesCoord" that we will query later on.
```scala
// Obtaining a list of ALL the cities and its coordinates
    df.withColumn("city", $"tags".getItem("addr:city"))
      .createOrReplaceTempView("citiesCoord")
```

Next up, we fetch only the cities that have no name (i.e., "NULL") using the "cities" view that we created previously in Step 2.  The "cast" keyword on lines 8 and 9 ensure that latitude and longitude are stored as Double values. 
```scala
// Obtaining all the filtered values that have city name as "NULL"
    val noCities = spark
      .sql(
        "SELECT city, lat, lon FROM cities where city is null"
      )
      .select(
        $"city" as "city",
        $"lat".cast("Double"),
        $"lon".cast("Double")
      )
```

### Step 4: Assign a city name to the ones that are NULL and write the final output.

- Obtain the (average) city center of each city that is NOT NULL from the "citiesCoord" view created above.
```scala
 // Obtaining the average center of each city that is NOT NULL
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
```

- Join and filter only for the cities that are actually near the given coordinates. The absolute value of latitude and longitude of cities that are NULL **minus** the latitude and longitude of cities that are NOT NULL should be less than 0.1. Write this to a new view named **"citiesDist"**.
```scala
// joining and filtering just for the cities that are actually near the given coordinates
    val df3 = noCities
      .repartition(8)
      .join(
        citiesCoord.repartition(8),
        abs(noCities("lat") - citiesCoord("latC")) < 0.1 && abs(
          noCities("lon") - citiesCoord("lonC")
        ) < 0.1,
        "left"
      )
    df3.createOrReplaceTempView("citiesDist")
```

- UDF (user defined function) to calculate distance and register the UDF. 
```scala
// Define an udf to calculate distance
    val distance = (latC: Double, lonC: Double, latN: Double, lonN: Double) => {
      Math.sqrt(Math.pow((lonC - lonN), 2) + Math.pow((latC - latN), 2))
    }

    // The function is registered
    spark.udf.register("distance", distance)
```

- Calculate the distance to each of the possible cities from the view **"citiesDist"** created in the previous step. Write this data to a view named **"reduce"**.
```scala
// Calculating the distance to each of the possible cities
    val citiesDist = spark.sql(
      "select cityC, city,lat,lon, distance(lat,lon,latC,lonC) as result from citiesDist"
    )
    citiesDist.createOrReplaceTempView("reduce")
```

- Select the nearest possible city given it's coordinates from the view **"reduce"**. This is done by performing a left join between the **"ans"** and **"minDist"** variables declared on lines 2 and 4 respectively. "ans" holds the city name, latitude, and longitude of cities that are NOT NULL (from the "reduce"  view) and "minDist" contains the same information for cities that are NULL.
```scala
// Selecting the nearest city given it's coordinates
    val ans =
      spark.sql("select cityC, lat as latC, lon as lonC, result from reduce")
    val minDist = spark.sql(
      "select lat, lon, min(result) as result from reduce group by lat, lon"
    )
    val newCities = minDist
      .repartition(8)
      .join(ans.repartition(8), ans("result") === minDist("result"), "left")
      .select($"cityC".as("city"), $"lat", $"lon")
```

- Obtain the total list of city nodes with breweries in it.
```scala
  // Obtaining the total list of city nodes with breweries in it
    val total = brewery.select($"city").union(newCities.select($"city"))
    total.createOrReplaceTempView("cities")
```
- Get the final count and write it to an orc file in the predefined schema outlined in the lab guide.
```scala
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
    finalCount.write.orc("breweries")
```

## Result

![result image 1](https://github.com/abs-tudelft-sbd20/lab-1-group-29/blob/Geolocate-city/result%20image%201.png)
 
Name of the city and count of breweries:
- [Rotterdam,5] 
- ['s-Gravenhage,5]
- [Delft,3] 
- [Hillegom,1] 
- [Den Hoorn,1] 
- [Leiden,1] 
- [Bodegraven,1]

### Improvements
We could include some more Dutch terms for "breweries" or "brewery" or anything related to the booze jargon in the Netherlands, but we would actually have to be native Dutchs to do that, which neither of us is! 

## Scalability

First of all, the input file is parametrizable, so the input can end up being any dataset in .orc format from openstreetmap.

Before optimizing our code, the app generated 1600 tasks/jobs, which took a long time to process. So we used partitioning and caching (explained in performance section below) to reduce the processing time.

![scale image 1](https://github.com/abs-tudelft-sbd20/lab-1-group-29/blob/Geolocate-city/scale%20image%201.png)

![scale image 2](https://github.com/abs-tudelft-sbd20/lab-1-group-29/blob/Geolocate-city/scale%20image%202.png)



Before the joins, there is just a filter and no additional steps. This filtering is performed for each of the default partitions spark creates when we do a join. In the image below, we see that for each individual partition, it is generating a partition filter which increases processing time overall.

![scale image 3](https://github.com/abs-tudelft-sbd20/lab-1-group-29/blob/Geolocate-city/scale%20image%203.png)

After performing the partition step, an aditional process is added to the SQL data flow in order to shuffle the data, as showed below:

![scale image 3](https://github.com/abs-tudelft-sbd20/lab-1-group-29/blob/Geolocate-city/scale%20image%204.png)

We chose 8 partitions as default because the machine we tested on has 8 threads on the CPU. In terms of scalability, if we want to run this app on the cloud, the configuration and number of machines we have will affect the number of partitions we choose to create. For instance, if the number of machines is less than the number of partitions, that would negatively affect performance. For instance, the partition number was added as a parameter and a scalability test was performed in our local machine using the script 'partitions_test.sh'. In it we increased systematically the unmber of partitions from 1 to 128 in 8 different runs. The result can be seen in the following image. 

![scale image 3](https://github.com/abs-tudelft-sbd20/lab-1-group-29/blob/Geolocate-city/scale%20image%20scalability%20test.png)

Test performed with 1,2,4,8,16,32,64 and 128 partitions from bottom to top

Example of test line in 'partitions_test.sh'
```scala
sudo docker run -it --rm -v "`pwd`":/io -v "`pwd`"/spark-events:/spark-events spark-submit target/scala-2.12/lab-1_2.12-1.0.jar -p 8 -o brew-4
```

In it we can see from botton to top, that the time it takes is proportional to the number of partitions for a local system.

## Performance

The application took 1 minute to run locally. The specifications of the test bench are given below:
- Intel Core i7-8565U - 4 cores, 8 threads, 1.8 GHz base clock.
- 16 GB of RAM
- 490 GB External SSD

We tested the run time of our code under 4 conditions:
- No optimizations
- Only caching
- Only partitioning
- Both caching and partitioning

When we ran our code with no optimizations, it took more than 31 minutes to obtain the results:

![perf image 1](https://github.com/abs-tudelft-sbd20/lab-1-group-29/blob/Geolocate-city/perf%20image%201.png)

When we looked more closely at the details provided in the spark history server, we noticed that the joins and the input processing took up most of the time. So we decided to add some partitioning and caches. 
```scala
 brewery.cache
     citiesCoord.cache
```

With only caching the total run time was more than 25 minutes:
![perf image 2](https://github.com/abs-tudelft-sbd20/lab-1-group-29/blob/Geolocate-city/perf%20image%202.png)

With only partitioning the total run time was approximately 55 seconds:
![perf image 3](https://github.com/abs-tudelft-sbd20/lab-1-group-29/blob/Geolocate-city/perf%20image%203.png)

With both caching and partitioning the total run time was approximately 51 seconds:
![perf image 4](https://github.com/abs-tudelft-sbd20/lab-1-group-29/blob/Geolocate-city/perf%20image%204.png)

"Apache Spark provides an important feature to cache intermediate data and provide significant performance improvement while running multiple queries on the same data" [source](https://towardsdatascience.com/apache-spark-caching-603154173c48). There's one cache for the "citiesCoord" view so we do not have to read the .orc file again and again when calculating distances and another cache for the brewery data-frame since we reuse that as well. However, caching does not provide a big performance improvement. When using only caching the run time is still over 25 minutes, compared to 31 minutes with no optimization. Thus, it is clear that partitioning gives us the maximum performance improvement, where we see the run time go from 31 minutes to less than 60 seconds.
