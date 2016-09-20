package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    println("before starting the warmup")
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {

    def innerBalanceWithArray(idx:Int, remLeft: Int): Boolean = {
      if (idx < chars.length) {
        val c = chars(idx)
        if (c == '(') innerBalanceWithArray(idx + 1, remLeft + 1)
        else if (c == ')') {
          if (remLeft == 0) false
          else innerBalanceWithArray(idx + 1, remLeft - 1)
        }
        else innerBalanceWithArray(idx + 1, remLeft)
      }
      else remLeft == 0
    }

    //innerBalance(chars.toList, 0)
    innerBalanceWithArray(0, 0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, leftPar: Int, rightPar: Int): Int = {
      if (idx < until) {
        val charIdx = chars(idx)
        if (charIdx == '(') traverse(idx + 1, until, leftPar + 1, rightPar)
        else if (charIdx == ')') traverse(idx + 1, until, leftPar, rightPar + 1)
        else traverse(idx + 1, until, leftPar, rightPar)
      }
      else leftPar + rightPar
    }

    def reduce(from: Int, until: Int): Int = {
      if (until - from < threshold) {
        traverse(from, until, 0, 0)
      }
      else {
        val mid = from + ((until - from) / 2)
        val balances = parallel(reduce(from, mid), reduce(mid, until))
        balances._1 + balances._2
      }
    }

    reduce(0, chars.length) == 0
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
