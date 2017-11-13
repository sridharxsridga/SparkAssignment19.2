/*
 * Using udfs on dataframe
1. Change firstname, lastname columns into
Mr.first_two_letters_of_firstname<space>lastname
for example - michael, phelps becomes Mr.mi phelps
2. Add a new column called ranking using udfs on dataframe, where :
gold medalist, with age >= 32 are ranked as pro
gold medalists, with age <= 31 are ranked amateur
silver medalist, with age >= 32 are ranked as expert
silver medalists, with age <= 31 are ranked rookie
 * 
 * 
 * 
 * 
 */


package assignment

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions.udf

object SparkUDF {
  
   def main(args: Array[String]): Unit = {
    //specify the configuration for the spark application using instance of SparkConf
    val config = new SparkConf().setAppName("Assignment 19.2").setMaster("local")
    
    //setting the configuration and creating an instance of SparkContext 
    val sc = new SparkContext(config)
    
    //Entry point of our sqlContext
    val sqlContext = new HiveContext(sc)
    
    //to use toDF method 
    import sqlContext.implicits._

    //Scheam for sports_data table
    val sports_schema = StructType(List (
    StructField("firstname",StringType,true),
    StructField("lastname",StringType,false),
    StructField("sports",StringType,false),
    StructField("medal_type",StringType,false),
    StructField("age",IntegerType,false),
    StructField("year",IntegerType,false ),
     StructField("country",StringType,false )
    ))
    
    //Create RDD from textFile
    val sports_file =sc.textFile("/home/acadgild/sridhar_scala/assignment/sports_data")
    
    //Removing the column names from the rdd 
    val noHeader = sports_file.mapPartitionsWithIndex( 
    (i, iterator) => 
    if (i == 0 && iterator.hasNext) { 
       iterator.next 
       iterator 
    } else iterator) 
     
    //Store the columns in the Row case class
    val sports_rowsRDD = noHeader.map{lines => lines.split(",")}.map{col => Row(col(0),col(1),col(2),col(3),col(4).toInt,col(5).toInt,col(6).trim)}
    
    //Create the dataframe
    val sportsDF = sqlContext.createDataFrame(sports_rowsRDD, sports_schema)
    
    //Create a temp tample which can be used for using sql statement to query from
    sportsDF.registerTempTable("sports_data")
    
    //select all data from table
    val dataDF = sqlContext.sql("select * from sports_data")

    //UDF which concatenates firstname and last name to full name column
    val concatNames = udf{
         (firstName : String,lastName : String) => "Mr." + firstName.charAt(0)+firstName.charAt(1)+"  "+lastName
       }
         
    //UDF for calculating rank of sportsman
    val rank_sportsman = udf{
         (medal_type: String,age: Int )=>{
           if(medal_type.equals("gold") && (age >=32))
             "Pro"
           else if(medal_type.equals("gold") && (age <=31))
             "Amateur"
             else if(medal_type.equals("silver") && (age >=32))
             "expert"
                else if(medal_type.equals("silver") && (age <=31))
             "Rookie"
                else
                  "Null"
       
         }
       }
       
           
    //Include the  full name column to the dataframe   
    val dataWithNamesDF =dataDF.withColumn("FullName", concatNames('firstname,'lastname))
       
    dataWithNamesDF.show()
      
     //Include the  full name and rank columns to the dataframe 
    val rankSportsmanDF = dataWithNamesDF.withColumn("Rank", rank_sportsman('medal_type,'age))
    
    rankSportsmanDF.show
   }
   
   
   
}