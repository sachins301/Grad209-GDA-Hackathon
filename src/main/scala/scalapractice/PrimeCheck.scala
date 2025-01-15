package scalapractice

object PrimeCheck extends App {
  val num = 29
  def isPrime(n: Int): Boolean ={
    println(n)
    if (n > num / 2) true
    else{
      if (num % n == 0) false
      else isPrime(n + 1)
    }
  }
  println(isPrime(2))
}
