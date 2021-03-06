package de.dfki.mlt.wd2es;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
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

public class LanguageProcessor {
	private JTok jtok;
	private StanfordCoreNLP pipeline;
	private LemmatizerME lemmatizer;
	private MunderLine munderLine;

	public LanguageProcessor() {
		try {
			this.jtok = new JTok();
		} catch (IOException e) {
			e.printStackTrace();
		}
		initializeENModuls();
		initializeDEModuls();
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
			String lemmatizerModelPath = "models/DE-lemmatizer.bin";
			InputStream in = this.getClass().getClassLoader().getResourceAsStream(lemmatizerModelPath);
			// if can't be loaded from classpath, try to load it from the file system
			if (null == in) {
				in = Files.newInputStream(Paths.get(lemmatizerModelPath));
			}
			lemmatizerModel = new LemmatizerModel(in);
		} catch (ConfigurationException | IOException e) {
			e.printStackTrace();
		}
		this.lemmatizer = new LemmatizerME(lemmatizerModel);
	}

	public List<String> tokenize(String text, String lang) {
		AnnotatedString annotatedString = jtok.tokenize(text, lang);
		List<Token> tokenList = Outputter.createTokens(annotatedString);
		List<String> tokensAsString = new ArrayList<String>();
		for (Token token : tokenList) {
			tokensAsString.add(token.getImage());
		}
		return tokensAsString;
	}

	public String lemmatizeDE(String text, boolean isAlias) {
		List<String> tokensAsString = tokenize(text, "de");
		String[][] coNllTable = this.munderLine.processTokenizedSentence(tokensAsString);

		String[] tokens = new String[coNllTable.length];
		String[] posTags = new String[coNllTable.length];
		for (int i = 0; i < coNllTable.length; i++) {
			tokens[i] = coNllTable[i][1];
			posTags[i] = coNllTable[i][3];
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

	public String lemmatizeEN(String text, boolean isAlias) {
		List<String> tokensAsString = tokenize(text, "en");
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
