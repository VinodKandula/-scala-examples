package com.examples

class ClassExamples {
  
}


class Complex(real: Double, imaginary: Double) {
  def re() = real
  def im() = imaginary
}

//Methods without arguments - c.im()
object ComplexNumbers {
  def main(args: Array[String]) {
    val c = new Complex(1.2, 3.4)
    println("imaginary part: " + c.im())
  }
}

class Complex_V1(real: Double, imaginary: Double) {
  def re = real
  def im = imaginary
}

//Inheritance and overriding
// scala.AnyRef is implicitly used
// mandatory to explicitly specify that a method overrides another one using the override modifier
class Complex_v2(real: Double, imaginary: Double) {
  def re = real
  def im = imaginary
  override def toString() =
    "" + re + (if (im < 0) "" else "+") + im + "i"
}