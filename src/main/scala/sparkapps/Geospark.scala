package sparkapps

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.{JoinQuery, KNNQuery, RangeQuery}
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PointRDD, RectangleRDD}
import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory}

object Geospark {

  def geosparkJob() = {
    val conf = new SparkConf()
          .setAppName("MyGeoSparkApp")
          .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val resourceFolder = System.getProperty("user.dir")+"/src/main/resources/sparkapps/"
    val PointRDDInputFile = resourceFolder + "arealm.csv"
    val PointRDDSecondInputFile = resourceFolder + "zcta510.csv"
    val PointRDDOffset = 0
    val PointRDDSplitter = FileDataSplitter.CSV

    val objectRDD = new PointRDD(sc, PointRDDInputFile, PointRDDOffset, PointRDDSplitter, false, StorageLevel.MEMORY_ONLY)
    val rectangleRDD = new RectangleRDD(sc, PointRDDSecondInputFile, PointRDDOffset, PointRDDSplitter, false, StorageLevel.MEMORY_ONLY)
    val centerGeometryRDD = new PointRDD(sc, PointRDDInputFile, PointRDDOffset, PointRDDSplitter, false, StorageLevel.MEMORY_ONLY)

    val res1 = spatialRangeWithoutIndex(objectRDD)
    val res2 = spatialRangeWithIndex(objectRDD)
    val res3 = spatialKNNQueryWithoutIndex(objectRDD)
    val res4 = spatialKNNQueryWithIndex(objectRDD)
    val res5 = spatialJoinQueryWithoutIndex(objectRDD, rectangleRDD)
    val res6 = spatialJoinQueryWithIndex(objectRDD, rectangleRDD)
    val res7 = distanceJoinQueryWithoutIndex(objectRDD, centerGeometryRDD)
    val res8 = distanceJoinQueryWithIndex(objectRDD, centerGeometryRDD)

    println("\n\n")
    println("The results are :")
    println("---------------")
    println(s"- Spatial Range Without Index: $res1")
    println(s"- Spatial Range With Index: $res2")
    println(s"- Spatial KNN Query Without Index $res3")
    println(s"- Spatial KNN Query With Index $res4")
    println(s"- Spatial Join Query Without Index $res5")
    println(s"- Spatial Join Query With Index $res6")
    println(s"- Distance Join Query Without Index $res7")
    println(s"- Distance Join Query With Index $res8")
    // println(res4)
    println("\n\n")

    sc.stop()
  }

  def spatialRangeWithoutIndex(objRdd: PointRDD): Int = {
    val queryEnvelope = new Envelope (-113.79,-109.73,32.99,35.08)
    val resultSize = RangeQuery
          .SpatialRangeQuery(objRdd, queryEnvelope, false, false)
          .count()
    resultSize.toInt
  }

  def spatialRangeWithIndex(objRdd: PointRDD): Int = {
    /* Range query window format: minX, maxX, minY, maxY*/
    val queryEnvelope=new Envelope (-113.79,-109.73,32.99,35.08)

    /*
     * IndexType.RTREE enum means the index type is R-tree. We support R-Tree index and Quad-Tree index.
     * false means just build index on original spatial RDD instead of spatial partitioned RDD. ONLY set true when doing Spatial Join Query.
     */
    objRdd.buildIndex(IndexType.RTREE,false)

    /*
     * The first false means don't consider objects intersect but not fully covered by the query window.
     * The true means use spatial index which has been built before.
    */
    val resultSize = RangeQuery.SpatialRangeQuery(objRdd, queryEnvelope, false, true).count()

    resultSize.toInt
  }

  def spatialKNNQueryWithoutIndex(objRdd: PointRDD): Int = {

    val fact = new GeometryFactory()
    /* Range query window format: X Y */
    val queryPoint = fact.createPoint(new Coordinate(-109.73, 35.08))

    /* The number 5 means 5 nearest neighbors
     * The false means don't use spatial index.
     */
    val resultSize = KNNQuery.SpatialKnnQuery(objRdd, queryPoint, 5,false).size()

    resultSize.toInt
  }

/*---------------------------- Spatial KNN Query with Index ----------------------------*/

  def spatialKNNQueryWithIndex(objRdd: PointRDD): Int = {

    val fact = new GeometryFactory()
    /* Range query window format: X Y */
    val queryPoint = fact.createPoint(new Coordinate(-109.73, 35.08))

    /*
     * IndexType.RTREE enum means the index type is R-tree. We support R-Tree index and Quad-Tree index. But Quad-Tree doesn't support KNN.
     * false means just build index on original spatial RDD instead of spatial partitioned RDD. ONLY set true when doing Spatial Join Query.
     */
    objRdd.buildIndex(IndexType.RTREE,false)

    /* The number 5 means 5 nearest neighbors
     * The true means use spatial index.
    */
    val resultSize = KNNQuery.SpatialKnnQuery(objRdd, queryPoint, 5, true).size()

    resultSize.toInt
  }

  def spatialJoinQueryWithoutIndex(objRdd: PointRDD, rectRdd: RectangleRDD): Int = {
    /*
     * GridType.RTREE means use R-Tree spatial partitioning technique. It will take the leaf node boundaries as parition boundary.
     * We support R-Tree partitioning and Voronoi diagram partitioning.
     */
    objRdd.spatialPartitioning(GridType.RTREE)

    /*
     * Use the partition boundary of objRdd to repartition the query window RDD, This is mandatory.
     */
    rectRdd.spatialPartitioning(objRdd.grids)

    /*
     * The first false means don't use spatial index.
     * The second false means don't consider objects intersect but not fully covered by the rectangles.
     */
    val resultSize = JoinQuery.SpatialJoinQuery(objRdd,rectRdd,false,false).count()

    resultSize.toInt
  }

/*---------------------------- Spatial Join Query with Index ----------------------------*/

  def spatialJoinQueryWithIndex(objRdd: PointRDD, rectRdd: RectangleRDD): Int = {
    /*
     * GridType.RTREE means use R-Tree spatial partitioning technique. It will take the leaf node boundaries as parition boundary.
     * We support R-Tree partitioning and Voronoi diagram partitioning.
     */
    objRdd.spatialPartitioning(GridType.RTREE)

    /*
     * IndexType.RTREE enum means the index type is R-tree. We support R-Tree index and Quad-Tree index. But Quad-Tree doesn't support KNN.
     * True means build index on the spatial partitioned RDD. ONLY set true when doing Spatial Join Query.
     */
    objRdd.buildIndex(IndexType.RTREE,true)

    /*
     * Use the partition boundary of objRdd to repartition the query window RDD, This is mandatory.
     */
    rectRdd.spatialPartitioning(objRdd.grids)

    /*
     * The first false means don't use spatial index.
     * The second false means don't consider objects intersect but not fully covered by the rectangles.
     */
    val resultSize = JoinQuery.SpatialJoinQuery(objRdd,rectRdd,true,false).count()

    resultSize.toInt
  }

/*---------------------------- Distance Join Query without Index ----------------------------*/


  def distanceJoinQueryWithoutIndex(objRdd: PointRDD, centGeomRdd: PointRDD): Int = {
    /*
     * 0.1 means the distance between two objects from the two Spatial RDDs.
     */
    val queryRDD = new CircleRDD(centGeomRdd,0.1)
    /*
     * GridType.RTREE means use R-Tree spatial partitioning technique. It will take the leaf node boundaries as parition boundary.
     * We support R-Tree partitioning and Voronoi diagram partitioning.
     */
    objRdd.spatialPartitioning(GridType.RTREE)
    /*
     * Use the partition boundary of objRdd to repartition the query window RDD, This is mandatory.
     */
    queryRDD.spatialPartitioning(objRdd.grids)
    /*
     * The first false means don't use spatial index.
     * The second false means don't consider objects intersect but not fully covered by the rectangles.
     */
    val resultSize = JoinQuery.DistanceJoinQuery(objRdd,queryRDD,false,false).count()

    resultSize.toInt
  }



/*---------------------------- Distance Join Query with Index ----------------------------*/

def distanceJoinQueryWithIndex(objRdd: PointRDD, centGeomRdd: PointRDD): Int = {
    /*
     * 0.1 means the distance between two objects from the two Spatial RDDs.
     */
    val queryRDD = new CircleRDD(centGeomRdd, 0.1)
    /*
     * GridType.RTREE means use R-Tree spatial partitioning technique. It will take the leaf node boundaries as parition boundary.
     * We support R-Tree partitioning and Voronoi diagram partitioning.
     */
    objRdd.spatialPartitioning(GridType.RTREE)
    /*
     * IndexType.RTREE enum means the index type is R-tree. We support R-Tree index and Quad-Tree index. But Quad-Tree doesn't support KNN.
     * True means build index on the spatial partitioned RDD. ONLY set true when doing Spatial Join Query.
     */
    objRdd.buildIndex(IndexType.RTREE, true)
    /*
     * Use the partition boundary of objRdd to repartition the query window RDD, This is mandatory.
     */
    queryRDD.spatialPartitioning(objRdd.grids)
    /*
     * The first false means don't use spatial index.
     * The second false means don't consider objects intersect but not fully covered by the rectangles.
     */
    val resultSize = JoinQuery.DistanceJoinQuery(objRdd, queryRDD, true, false).count()

    resultSize.toInt
  }


  def main(args: Array[String]) = geosparkJob()
}
