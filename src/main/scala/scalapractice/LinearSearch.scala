package scalapractice

object LinearSearch extends App{
  val lst = List(2,7,4,9,3,6)
  print(linearSearch(lst, 6))

  def linearSearch(lst: List[Int], target: Int): Option[Int]={
    lst.zipWithIndex.find(_._1 == target).map(_._2)
  }
}
