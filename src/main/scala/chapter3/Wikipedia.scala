package chapter3

import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.conf.Configuration
import com.twitter.scalding._
import TDsl._

/**
 * User: vgordon
 * Date: 9/26/13
 * Time: 6:06 PM
 */
object Wikipedia {
  def main(args: Array[String]) {
    ToolRunner.run(new Configuration, new Tool, "chapter3.Wikipedia" +: "--local" +: args)
  }
}

class Wikipedia(args: Args) extends Job(args) {
  val input = TypedTsv[(String, String)]("src/main/data/wikipedia.txt")
  val output = TypedTsv[(Int, Long)]("src/main/data/histogram.txt")

  private def getWords(sentence: String) : TraversableOnce[String] = sentence.split("\\s+")

  def flattenWords(articles: TypedPipe[(String, String)]) : TypedPipe[String] = {
    articles.flatMap(a => getWords(a._2))
  }

  def wordsLength(words: TypedPipe[String]) : TypedPipe[Int] = {
    val Word = "[\\w-]+".r
    def getWordLength(token: String) : Option[Int] = Word.findFirstIn(token).map(_.length)
    words.flatMap(getWordLength)
  }

  def lengthsHistogram(lengths: TypedPipe[Int]) : TypedPipe[(Int, Long)] = {
    lengths.groupBy(identity).size
  }

  lengthsHistogram(wordsLength(flattenWords(input))).write(output)
}
