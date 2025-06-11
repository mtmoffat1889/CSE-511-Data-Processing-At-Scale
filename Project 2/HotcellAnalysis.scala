package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()
  pickupInfo.createOrReplaceTempView("pickupInfo")

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  //Procotionary measure to ensure only valid points within the target area are selected
  val validPoints = spark.sql("select x, y, z, count(*) as pointCount from pickupInfo where x <= " + maxX + " and x >= " + minX + " and y <= " + maxY + " and y >= " + minY + " and z <= " + maxZ + " and z >= " + minZ + " group by x,y,z").persist()
  validPoints.createOrReplaceTempView("validPoints")

  //Calculates values needed to get ZScore
  val temp = spark.sql("select sum(pointCount*pointCount) as sumPointsSq, sum(pointCount) as sumPoints from validPoints").persist()
  val sumPointsSq: Double = temp.first().getLong(0).toDouble
  val sumPoints: Double = temp.first().getLong(1).toDouble

  //Finds the neighbors of each cell and calculates values needed for ZScore
  val getNeighbor = spark.sql("select points.x as x, points.y as y, points.z as z, count(*) as neighborCount, sum(neighbors.pointCount) as neighborWeight from validPoints as points inner join validPoints as neighbors on (((points.x-neighbors.x) <= 1 and (points.x-neighbors.x) >= -1) and ((points.y-neighbors.y) <= 1 and (points.y-neighbors.y) >= -1) and ((points.z-neighbors.z) <= 1 and (points.z-neighbors.z) >= -1)) group by points.x, points.y, points.z").persist()
  getNeighbor.createOrReplaceTempView("getNeighbor")

  //Registering the ZScore function to work in a sql command
  spark.udf.register("ZScore",(neighborWeight: Int, sumPoints: Double, neighborCount: Int, sumPointsSq: Double, numCells: Int) =>((
    HotcellUtils.ZScore(neighborWeight, sumPoints, neighborCount, sumPointsSq, numCells)
  )))

  //Calls the ZScore function for each cell
  val scoredCells = spark.sql("select x, y, z, ZScore(neighborWeight, " + sumPoints + ", neighborCount, " + sumPointsSq + ", " + numCells + ") as zscore from getNeighbor").persist()
  scoredCells.createOrReplaceTempView("scoredCells")

  //Sorts and returns the list
  return spark.sql("select x, y, z from scoredCells order by zscore desc")
}
}
