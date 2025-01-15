package scalapractice

object ReverseString extends App{
  val str: String = "helloworld"
  var reversed: String = str.foldRight("")((char, rev) => rev + char)
  println(reversed)

}
