package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    val rectangle = queryRectangle.split(",")
    val point = pointString.split(",")

    val pointXCoordinate: Double = point(0).trim.toDouble
    val pointYCoordinate: Double = point(1).trim.toDouble

    val rectX1: Double = rectangle(0).trim.toDouble
    val rectY1: Double = rectangle(1).trim.toDouble
    val rectX2: Double = rectangle(2).trim.toDouble
    val rectY2: Double = rectangle(3).trim.toDouble

    if((pointXCoordinate >= rectX1 && pointXCoordinate <= rectX2) && (pointYCoordinate >= rectY1 && pointYCoordinate <= rectY2)){
      return true
    }
    return false
  }

  // YOU NEED TO CHANGE THIS PART

}
