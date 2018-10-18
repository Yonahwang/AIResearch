package SituationAwareness_2_0a.RebuildLightLDAModel

import java.math.BigDecimal

/**
  * 似乎无法重载=，那就用:=来赋值好了
  * 一个保证不奇怪的丢失精度的类
  */
class ArchDouble extends Serializable {
  private[ArchDouble] var self_double = 0.0
  private val DEF_DIV_SCALE = 10

  def +(d2: Double): Double = {
    val b1 = new BigDecimal(self_double.toString)
    val b2 = new BigDecimal(d2.toString)
    b1.add(b2).doubleValue()
  }

  def -(d2: Double): Double = {
    val b1 = new BigDecimal(self_double.toString)
    val b2 = new BigDecimal(d2.toString)
    b1.subtract(b2).doubleValue()
  }

  def *(d2: Double): Double = {
    val b1 = new BigDecimal(self_double.toString)
    val b2 = new BigDecimal(d2.toString)
    b1.multiply(b2).doubleValue()
  }

  def /(d2: Double): Double = {
    val b1 = new BigDecimal(self_double.toString)
    val b2 = new BigDecimal(d2.toString)
    b1.divide(b2, DEF_DIV_SCALE, BigDecimal.ROUND_HALF_UP).doubleValue()
  }

  def +=(d2: Double): ArchDouble = {
    this := this + d2
    this
  }

  def -=(d2: Double): ArchDouble = {
    this := this - d2
    this
  }

  def *=(d2: Double): ArchDouble = {
    this := this * d2
    this
  }

  def /=(d2: Double): ArchDouble = {
    this := this / d2
    this
  }

  def :=(d: Double): Unit = {
    self_double = d
  }

  def set(d: Double): ArchDouble = {
    :=(d)
    return this
  }

  def apply(): Double = self_double

  //-----------------------

  def +(d2: ArchDouble): Double = {
    this + d2.self_double
  }

  def -(d2: ArchDouble): Double = {
    this - d2.self_double
  }

  def *(d2: ArchDouble): Double = {
    this * d2.self_double
  }

  def /(d2: ArchDouble): Double = {
    this / d2.self_double
  }

  def +=(d2: ArchDouble): ArchDouble = {
    this := this + d2.self_double
    this
  }

  def -=(d2: ArchDouble): ArchDouble = {
    this := this - d2.self_double
    this
  }

  def *=(d2: ArchDouble): ArchDouble = {
    this := this * d2.self_double
    this
  }

  def /=(d2: ArchDouble): ArchDouble = {
    this := this / d2.self_double
    this
  }


  override def toString: String = self_double.toString
}
