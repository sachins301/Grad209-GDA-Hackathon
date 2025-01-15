package scalapractice

import scala.collection.mutable

object ParenthesisMatching extends App {
  val input = "(())"
  val stack = mutable.Stack[Char]()
  var res = true
  for(c <- input){
    if (c == '(') stack.push('(')
    else if(stack.nonEmpty && stack.top == '(') stack.pop()
    else res = false
  }
  println(res)
}
