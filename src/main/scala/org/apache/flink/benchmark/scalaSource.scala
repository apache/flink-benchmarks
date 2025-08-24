package org.apache.flink.benchmark

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.serializers._
import org.apache.flink.benchmark.functions.BaseSourceWithKeyRange

final case class ScalaOperation(id: Int, name: String)

final case class ScalaCaseClass(
                                 var id: Int,
                                 name: String,
                                 operationNames: Array[String],
                                 operations: Array[ScalaOperation],
                                 otherId1: Int, otherId2: Int, otherId3: Int, someObject: String
                               )

object ScalaCaseClass {
  val ti: TypeInformation[ScalaCaseClass] = deriveTypeInformation[ScalaCaseClass]
}

class ScalaSource(numEvents: Int, numKeys: Int) extends BaseSourceWithKeyRange[ScalaCaseClass](numEvents, numKeys) {
  private val serialVersionUID = 2941333602938145526L
  @transient private var template: ScalaCaseClass = null

  override protected def init(): Unit = {
    super.init()
    template = ScalaCaseClass(0, "myName", Array("op1", "op2", "op3", "op4"), Array(ScalaOperation(1, "op1"), ScalaOperation(2, "op2"), ScalaOperation(3, "op3")), 1, 2, 3, "null")
  }

  override protected def getElement(keyId: Int): ScalaCaseClass = {
    template.id = keyId
    template
  }
}
