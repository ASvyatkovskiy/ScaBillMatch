import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import scala.collection.mutable.ArrayBuffer

object Stemmer {

  def tokenize(content:String):Seq[String]={
    val analyzer=new EnglishAnalyzer()
    val tokenStream=analyzer.tokenStream("contents", content)
    val term=tokenStream.addAttribute(classOf[CharTermAttribute])

    tokenStream.reset() // must be called by the consumer before consumption to clean the stream^M
    var result = ArrayBuffer.empty[String]

    while(tokenStream.incrementToken()) {
        val termValue = term.toString
        if (!(termValue matches ".*[\\d\\.].*")) {
          result += term.toString.concat(" ")
        }
    }
    tokenStream.end()
    tokenStream.close()
    result
  }
}
