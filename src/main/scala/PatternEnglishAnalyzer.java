//package org.apache.lucene.analysis.en;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

import org.apache.lucene.analysis.pattern.PatternTokenizer;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;

import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;

public final class GenericEnglishAnalyzer extends StopwordAnalyzerBase {
  private final CharArraySet stemExclusionSet;
   
  /**
   * Returns an unmodifiable instance of the default stop words set.
   * @return default stop words set.
   */
  public static CharArraySet getDefaultStopSet(){
    return DefaultSetHolder.DEFAULT_STOP_SET;
  }
  
  /**
   * Atomically loads the DEFAULT_STOP_SET in a lazy fashion once the outer class 
   * accesses the static final set the first time.;
   */
  private static class DefaultSetHolder {
    static final CharArraySet DEFAULT_STOP_SET = StandardAnalyzer.STOP_WORDS_SET;
  }

  /**
   * Builds an analyzer with the default stop words: {@link #getDefaultStopSet}.
   */
  public GenericEnglishAnalyzer() {
    this(DefaultSetHolder.DEFAULT_STOP_SET);
  }
  
  /**
   * Builds an analyzer with the given stop words.
   * 
   * @param stopwords a stopword set
   */
  public GenericEnglishAnalyzer(CharArraySet stopwords) {
    this(stopwords, CharArraySet.EMPTY_SET);
  }

  /**
   * Builds an analyzer with the given stop words. If a non-empty stem exclusion set is
   * provided this analyzer will add a {@link SetKeywordMarkerFilter} before
   * stemming.
   * 
   * @param stopwords a stopword set
   * @param stemExclusionSet a set of terms not to be stemmed
   */
  public GenericEnglishAnalyzer(CharArraySet stopwords, CharArraySet stemExclusionSet) {
    super(stopwords);
    this.stemExclusionSet = CharArraySet.unmodifiableSet(CharArraySet.copy(stemExclusionSet));
  }

  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    //FIXME should not be hardcoded
    String regexp1 = "SECTION \\d\\.";
    String regexp2 = "\\[A> SECTION \\d\\.";
    String regexp3 = "SECTION \\[A> \\d\\.";
    Pattern pattern = Pattern.compile(regexp1 + '|' + regexp2 + '|' + regexp3);
    int group = -1;

    final Tokenizer source = new PatternTokenizer(pattern, group);
    TokenStream result = new StandardFilter(source);
    result = new EnglishPossessiveFilter(result);
    result = new LowerCaseFilter(result);
    result = new StopFilter(result, stopwords);
    if(!stemExclusionSet.isEmpty())
      result = new SetKeywordMarkerFilter(result, stemExclusionSet);
    result = new PorterStemFilter(result);
    return new TokenStreamComponents(source, result);
  }
}
