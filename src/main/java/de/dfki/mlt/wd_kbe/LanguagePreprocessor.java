package de.dfki.mlt.wd_kbe;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import de.dfki.lt.tools.tokenizer.JTok;
import de.dfki.lt.tools.tokenizer.annotate.AnnotatedString;
import de.dfki.lt.tools.tokenizer.output.Outputter;
import de.dfki.lt.tools.tokenizer.output.Token;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class LanguagePreprocessor {
	private JTok jtok;
	private StanfordCoreNLP pipeline;

	public LanguagePreprocessor() {
		try {
			jtok = new JTok();
		} catch (IOException e) {
			e.printStackTrace();
		}
		Properties props;
		props = new Properties();
		props.put("annotators", "tokenize, ssplit, pos, lemma");
		pipeline = new StanfordCoreNLP(props);
	}

	public String tokenizer(String text, boolean isAlias) {
		AnnotatedString annotatedString = jtok.tokenize(text, "en");
		List<Token> tokenList = Outputter.createTokens(annotatedString);
		StringBuilder builder = new StringBuilder();
		for (Token token : tokenList) {
			if (isAlias) {
				String alias = lemmatizeAliases(token.getImage());
				if (!alias.isEmpty())
					builder.append(alias.toLowerCase() + " ");
			} else {
				builder.append(lemmatize(token.getImage()) + " ");
			}
		}
		return builder.toString().trim();
	}

	public String lemmatize(String documentText) {
		StringBuilder builder = new StringBuilder();
		Annotation document = new Annotation(documentText);
		this.pipeline.annotate(document);
		List<CoreMap> sentences = document.get(SentencesAnnotation.class);
		for (CoreMap sentence : sentences) {
			for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
				String image = token.get(LemmaAnnotation.class);
				image = replaceParantheses(image);
				builder.append(image);
			}
		}
		return builder.toString();
	}

	public String lemmatizeAliases(String documentText) {
		StringBuilder builder = new StringBuilder();
		Annotation document = new Annotation(documentText);
		this.pipeline.annotate(document);
		List<CoreMap> sentences = document.get(SentencesAnnotation.class);
		for (CoreMap sentence : sentences) {
			for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
				String tag = token.get(PartOfSpeechAnnotation.class);
				if (tag.startsWith("VB") || tag.startsWith("NN") || tag.startsWith("IN")) {
					String image = token.get(LemmaAnnotation.class);
					image = replaceParantheses(image);
					builder.append(image);
				}
			}
		}
		return builder.toString();
	}

	public String replaceParantheses(String image) {
		return image = image.replaceAll("-lrb-", "\\(").replaceAll("-rrb-", "\\)").replaceAll("-lcb-", "\\{")
				.replaceAll("-rcb-", "\\}").replaceAll("-lsb-", "\\[").replaceAll("-rsb-", "\\]");
	}

}
