

case class ProductTest01(n01: Int, n02: Int, n03: Int, n04: Int, n05: Int, n06: Int, n07: Int, n08: Int, n09: Int, n10: Int, n11: Int, n12: Int, n13: Int, n14: Int, n15: Int, n16: Int, n17: Int, n18: Int, n19: Int, n20: Int, n21: Int, n22: Int, n23: Int) extends Product{
//    val nums = Test01(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)




}

object ProductTest01{
  def main(args: Array[String]): Unit = {
    val p = new ProductTest01(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)
    println(p.productElement(1))
    p.productIterator.foreach(println)
    println(p.productArity)
  }
}

