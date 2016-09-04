package com.examples

//Functions are objects
//it is often used in user-interface code, to register call-back functions which get called when some event occurs.

object TimerCallbackFunction {
  def oncePerSecond(callback: () => Unit) {
    while (true) { callback(); Thread sleep 1000 }
  }
  def timeFlies() {
    println("time flies like an arrow...")
  }
  def main(args: Array[String]) {
    oncePerSecond(timeFlies)
  }
}