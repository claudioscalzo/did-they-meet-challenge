import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.nio.file.{Paths, Files}

object People {

    // PARAMETERS TO SET
    val Xrange = 3
    val Yrange = 3
    val Trange = 3

    /** DATASET IMPORT
     * @param datasetSchema: the schema to use to read the CSV
     * @param filename: the path/name of the CSV file to import
     * @return the DataFrame with all the informations read from the CSV
     */
    def datasetImport(datasetSchema: StructType, filename: String): DataFrame = {

        // APACHE SPARK SESSION BUILDING
        println("-> Building the Spark Session...")
        val spark = SparkSession
            .builder()
            .config("spark.master", "local")
            .getOrCreate()
        
        // ACTUAL DATASET IMPORT
        spark.sqlContext
            .read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("delimiter", ",")
            .schema(datasetSchema)
            .load(filename)
    }

    /** DATAFRAME PREPARATION
     * @param peopleDF: the DataFrame of people
     * @param user1: the user1's ID
     * @param user2: the user2's ID
     * @return the prepared DataFrame
     */
    def prepareDataFrame(peopleDF: DataFrame, user1: String, user2: String): DataFrame = {

        peopleDF

            // Keep only our two users
            .filter(row => row(4).equals(user1) || row(4).equals(user2))

            // Timestamp infos extraction
            .withColumn("year", year(peopleDF("timestamp")))
            .withColumn("month", month(peopleDF("timestamp")))
            .withColumn("day", dayofmonth(peopleDF("timestamp"))) 
            .withColumn("hour", hour(peopleDF("timestamp"))) 
            .withColumn("minute", minute(peopleDF("timestamp"))) 
            .withColumn("second", second(peopleDF("timestamp"))).drop(peopleDF("timestamp"))

            // Position approximation       
            .withColumn("x", peopleDF("xpos").cast("int")).drop(peopleDF("xpos"))
            .withColumn("y", peopleDF("ypos").cast("int")).drop(peopleDF("ypos"))
    }


    /** HITS COMPUTATION
     *
     * @param mov1: the DataFrame to join
     * @param mov2: the copy of mov2
     * @return the joined dataframe with no duplicated columns
     */
    def computeHits(mov1: DataFrame, mov2: DataFrame): DataFrame = {

        mov1
            
            // Self join
            .join(mov2, (mov1("year") === mov2("year")) && 
                        (mov1("month") === mov2("month")) && 
                        (mov1("day") === mov2("day")) && 
                        (mov1("hour") === mov2("hour")) && 
                        (mov1("minute") === mov2("minute")) && 
                        (abs(mov1("second") - mov2("second")) < Trange) && 
                        (mov1("floor") === mov2("floor")) &&
                        (abs(mov1("x") - mov2("x")) < Xrange) &&
                        (abs(mov1("y") - mov2("y")) < Yrange) &&
                        !(mov1("uid") === mov2("uid")),
                        "inner")

            // Drop duplicates
            .drop(mov2("x")).drop(mov2("y")).drop(mov2("floor")).drop(mov2("uid")).drop(mov2("second"))
            .drop(mov2("year")).drop(mov2("month")).drop(mov2("day")).drop(mov2("hour")).drop(mov2("minute"))
    }

    /**
     * SHOW THE HITS DATAFRAME
     *
     * @param hitsDF: the DataFrame to visualize
     */
    def showHits(hitsDF: DataFrame): Unit = {

        // FUNCTIONS TO CREATE READABLE "date" and "hourOfDay" STRINGS
        def createDate: ((Int, Int, Int) => String) = { (i1, i2, i3) => i1.toString + "-" + i2.toString + "-" + i3.toString }
        val createDateUDF = udf(createDate)
        def createHourOfDay: ((Int, Int) => String) = { (i1, i2) => i1.toString + ":" + i2.toString }
        val createHourOfDayUDF = udf(createHourOfDay)

        // COMPACT DATAFRAME FOR BETTER VISUALIZATION
        var compactDF = hitsDF
            .withColumn("date", createDateUDF(hitsDF("day"), hitsDF("month"), hitsDF("year")))
            .withColumn("hourOfDay", createHourOfDayUDF(hitsDF("hour"), hitsDF("minute")))
            .drop(hitsDF("day")).drop(hitsDF("month")).drop(hitsDF("year"))
            .drop(hitsDF("hour")).drop(hitsDF("minute")).drop(hitsDF("second"))
        
        // SHOW SOME EXAMPLES (OR SHOW ALL IF HITS ARE < 20)
        compactDF
            .groupBy(compactDF("date"), compactDF("hourOfDay"))
            .agg(bround(avg(compactDF("x")),2).as("x"),
                 bround(avg(compactDF("y")),2).as("y"),
                 avg(compactDF("floor")).cast("int").as("floor"))
            .show(20, false)
    }
}