import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._

object AImportApp {

  def main(sysArgs: Array[String]): Unit = {

    val df = Processor.loadFrame("src/main/resources/data1.json")
    val toJoin = Processor.loadFrame("src/main/resources/data2.json")

    val transformations = Seq(
      Rename("field2", "brandNewField"),
      Concat("concatField", "brandNewField", "field1"),
      Drop("field1"),
      Join(toJoin, "id")
    )

    val transformed = applyTransformations(df, transformations)

    transformed.write
      .mode("overwrite")
      .json("src/main/resources/data1.out")
  }

  //  def concat(dataFrame: DataFrame, newCol:String, concat_value:String ) : DataFrame = {
  //    dataFrame
  //      .withColumn(newCol, concat_ws(concat_value, ))
  //  }

  def applyTransformations(dataFrame: DataFrame, transformations: Seq[Transformation]): DataFrame = {
    var df = dataFrame
    transformations.foreach(t => df = t.transform(df))
    df
  }


}

object Processor {
  private val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("AImport")
    .getOrCreate()

  def loadFrame(fileName: String): DataFrame =
    spark.read.json(fileName)
}

sealed trait Transformation {
  def transform(dataFrame: DataFrame): DataFrame
}

case class Rename(oldName: String, newName: String) extends Transformation {
  override def transform(dataFrame: DataFrame): DataFrame =
    dataFrame.withColumnRenamed(oldName, newName)
}

case class Concat(newField: String, cols: String*) extends Transformation {
  override def transform(dataFrame: DataFrame): DataFrame =
    dataFrame.withColumn(newField, concat_ws(" ", cols.map(c => col(c)): _*))
}

case class Drop(fieldName: String) extends Transformation {
  override def transform(dataFrame: DataFrame): DataFrame = dataFrame.drop(fieldName)
}

case class Join(joinWith: DataFrame, usingColumns: String*) extends Transformation {

  override def transform(dataFrame: DataFrame): DataFrame =
    dataFrame.join(joinWith, usingColumns)
}