package de.dfki.mlt.wd_kbe;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.configuration2.ex.ConfigurationException;

import de.dfki.lt.tools.tokenizer.JTok;
import de.dfki.lt.tools.tokenizer.annotate.AnnotatedString;
import de.dfki.lt.tools.tokenizer.output.Outputter;
import de.dfki.lt.tools.tokenizer.output.Token;
import de.dfki.mlt.munderline.MunderLine;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import opennlp.tools.lemmatizer.LemmatizerME;
import opennlp.tools.lemmatizer.LemmatizerModel;

public class LanguagePreprocessor {
	private JTok jtok;
	private StanfordCoreNLP pipeline;
	private LemmatizerME lemmatizer;
	private MunderLine munderLine;
	private String lang;

	public LanguagePreprocessor(String lang) {
		this.lang = lang;
		try {
			this.jtok = new JTok();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (this.lang.equals("en")) {
			initializeENModuls();
		} else if (this.lang.equals("de")) {
			initializeDEModuls();
		}
	}

	private void initializeENModuls() {
		Properties props;
		props = new Properties();
		props.put("annotators", "tokenize, ssplit, pos, lemma");
		this.pipeline = new StanfordCoreNLP(props);
	}

	private void initializeDEModuls() {
		LemmatizerModel lemmatizerModel = null;
		try {
			this.munderLine = new MunderLine("DE_pipeline.conf");
			lemmatizerModel = new LemmatizerModel(new FileInputStream("src/main/resources/models/DE-lemmatizer.bin"));
		} catch (ConfigurationException | IOException e) {
			e.printStackTrace();
		}
		this.lemmatizer = new LemmatizerME(lemmatizerModel);
	}

	public String tokenizeLemmatizeText(String text, boolean isAlias) {
		String resultText = new String();
		List<String> tokensAsString = tokenize(text);
		if (this.lang.equals("en")) {
			resultText = lemmatizeEN(tokensAsString, isAlias);
		} else if (this.lang.equals("de")) {
			resultText = lemmatizeDE(tokensAsString, isAlias);
		}
		return resultText;
	}

	public List<String> tokenize(String text) {
		AnnotatedString annotatedString = jtok.tokenize(text, this.lang);
		List<Token> tokenList = Outputter.createTokens(annotatedString);
		List<String> tokensAsString = new ArrayList<String>();
		for (Token token : tokenList) {
			tokensAsString.add(token.getImage());
		}
		return tokensAsString;
	}

	public String lemmatizeDE(List<String> tokensAsString, boolean isAlias) {
		String[][] coNllTable = this.munderLine.processTokenizedSentence(tokensAsString);

		String[] tokens = new String[coNllTable.length];
		String[] posTags = new String[coNllTable.length];
		for (int i = 0; i < coNllTable.length; i++) {
			tokens[i] = coNllTable[i][1];
			posTags[i] = coNllTable[i][3];
			System.out.println(tokens[i] + " ### " + posTags[i]);
		}
		String[] lemmata = this.lemmatizer.lemmatize(tokens, posTags);
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < lemmata.length; i++) {
			if ((isAlias && checkAliasConditionDE(posTags[i])) || !isAlias) {
				builder.append(lemmata[i] + " ");
			}
		}
		return builder.toString().trim();
	}

	public String lemmatizeEN(List<String> tokensAsString, boolean isAlias) {
		StringBuilder builder = new StringBuilder();
		Annotation document = null;
		for (String token : tokensAsString) {
			document = new Annotation(token);
			this.pipeline.annotate(document);
			List<CoreMap> sentences = document.get(SentencesAnnotation.class);
			for (CoreMap sentence : sentences) {
				for (CoreLabel coreLabel : sentence.get(TokensAnnotation.class)) {
					String image = coreLabel.get(LemmaAnnotation.class);
					String tag = coreLabel.get(PartOfSpeechAnnotation.class);
					if ((isAlias && checkAliasConditionEN(tag)) || !isAlias) {
						image = replaceParantheses(image).toLowerCase();
						builder.append(image + " ");
					}
				}
			}
		}
		return builder.toString().trim();
	}

	/**
	 * Accept only English verbs, nouns and prepositions
	 * 
	 * @param tag
	 * @return
	 */
	private boolean checkAliasConditionEN(String tag) {
		return (tag.startsWith("VB") || tag.startsWith("NN") || tag.startsWith("IN"));
	}

	/**
	 * Accept only German (STTS tags) verbs, prepositions, nouns and some particles
	 * 
	 * @param tag
	 * @return
	 */
	private boolean checkAliasConditionDE(String tag) {
		return (tag.startsWith("V") || tag.startsWith("N") || tag.startsWith("APP") || tag.equals("PTKNEG")
				|| tag.equals("PTKREL") || tag.equals("PTKVZ") || tag.equals("PTKZU"));
	}

	public String replaceParantheses(String image) {
		return image = image.replaceAll("-lrb-", "\\(").replaceAll("-rrb-", "\\)").replaceAll("-lcb-", "\\{")
				.replaceAll("-rcb-", "\\}").replaceAll("-lsb-", "\\[").replaceAll("-rsb-", "\\]");
	}

}
