import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.nio.file.{Paths, Files}

object Main {

    // SUPPRESS SPARK's CONSOLE LOGGING
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    def main(args: Array[String]) {

        println("\n##################################################")
        println("[Program started]\n")
        
        // COMMAND LINE CHECK
        if (args.length != 3) {
            println("-> You need to pass me the right informations, in this way:")
            println("   program.jar <dataset> <user1> <user2>\n")
            return
        }

        // COMMAND LINE ARGUMENTS
        val filename = args(0)
        val user1 = args(1)
        val user2 = args(2)
        if (!Files.exists(Paths.get(filename))) {
            println("-> The file " + filename + " doesn't exist!\n")
            return
        }
        println("-> I'll find if the user " + user1 + " and the user " + user2 + " met each other.")
        println("   Wait a second...\n")

        // CUSTOM CSV SCHEMA CREATION
        val datasetSchema = StructType(Array(
            StructField("timestamp", TimestampType, true),
            StructField("xpos", DoubleType, true),
            StructField("ypos", DoubleType, true),
            StructField("floor", IntegerType, true),
            StructField("uid", StringType, true)))

        // DATASET IMPORT
        println("-> Loading the dataset...\n")
        val peopleDF = People.datasetImport(datasetSchema, filename)

        // DATAFRAME PREPARATION (TIME AND SPATIAL RANGE REDUCTION)
        println("\n-> Preparing the dataset for the computation...\n")
        val mov1, mov2 = People.prepareDataFrame(peopleDF, user1, user2)
        
        // COMPUTE THE COINCIDENCE OF MOVEMENTS
        println("-> Computing the hits history...\n")
        val hitsDF = People.computeHits(mov1, mov2)

        // FINAL CHECK
        if(hitsDF.count() > 0) {
            
            println("\n=> YES, they've met each other!")
            println("   Just a moment, I show you when and where...\n")
            
            People.showHits(hitsDF)
        }
        else {
            println("\n=> NO! Never met each other.")
        }

        println("##################################################")
    }
}
