package com.express.cdw.spark

import java.math.BigDecimal
import java.sql.Date

import com.express.cdw
import com.express.cdw._
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, UserDefinedFunction}

/**
  * Common Spark UDFs used in Express CDW processing modules.
  *
  * @author mbadgujar
  */
package object udfs {

  // Check if string column is empty
  val CheckEmptyUDF: UserDefinedFunction = udf(!isNotEmpty(_: Any))

  // Check if string column is non-empty
  val CheckNotEmptyUDF: UserDefinedFunction = udf(isNotEmpty(_: Any))

  // UDF to generate Transaction IDs
  val GetTrxnIDUDF: UserDefinedFunction = udf((trxnDate: String, storeNum: Int, registerNum: Int, trxnNum: Int) => {
    trxnDate.replaceAll("-", "") + f"$storeNum%05d" + f"$registerNum%03d" + f"$trxnNum%05d"
  })

  //converts string to long
  val strToLongUDF: UserDefinedFunction = udf(strToLong(_: String))

  //converts string to date
  val strToDateUDF: UserDefinedFunction = udf(strToDate _)

  //UDF to calculate distance travelled
  val DistanceTravelled: UserDefinedFunction = udf(distanceTravelledFunc(_:Double, _:Double,_:Double,_:Double,0,0))

  val sortAssociatedMK : UserDefinedFunction = udf(sortMKsString _)

  /**
    * UDAF to group struct type columns to List/Array type
    *
    * @param struct [[StructType]] for struct column to be grouped
    */
  class CollectStruct(struct: StructType) extends UserDefinedAggregateFunction {

    override def inputSchema: org.apache.spark.sql.types.StructType = StructType(Seq(StructField("toBeGroupedCols", struct)))

    override def bufferSchema: StructType = StructType(Seq(StructField("outputList", ArrayType(struct))))

    override def dataType: DataType = ArrayType(struct)

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = Nil
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getSeq[Any](0) :+ input.getStruct(0)
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getSeq[Any](0) ++ buffer2.getSeq[Any](0)
    }

    override def evaluate(buffer: Row): Any = {
      buffer.get(0)
    }
  }

}

