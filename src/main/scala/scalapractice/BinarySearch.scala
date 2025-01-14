package scalapractice

import scala.annotation.tailrec

object BinarySearch extends App{
  val lst = List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
  println(binarySearch(lst, 6))

  def binarySearch(lst: List[Int], target: Int): Option[Int]={
    def search(l: Int, r: Int): Option[Int]={
      if(l > r) None
      else{
        val mid = (l + r) / 2
        lst(mid) match {
          case x if x == target => Some(mid)
          case x if x < target  => search(mid + 1, r)
          case _                => search(l, mid - 1)
        }
      }
    }
    search(0, lst.length - 1)
  }
}
