package de.dfki.mlt.kbe.wd_kb_extractor;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Test;

import de.dfki.mlt.wd_kbe.LanguagePreprocessor;

public class LanguagePreprocessorTest {
	LanguagePreprocessor langProcessorEN = new LanguagePreprocessor("en");
	LanguagePreprocessor langProcessorDE = new LanguagePreprocessor("de");

	@Test
	public void testTokenizer() throws ConfigurationException, IOException {
		String alias = "Norwegian List of Lights ID";
		List<String> actualTokenizedAlias = langProcessorEN.tokenize(alias);
		String[] expTokens = "Norwegian List of Lights ID".split(" ");
		assertThat(actualTokenizedAlias).isEqualTo(Arrays.asList(expTokens));
	}

	@Test
	public void testLemmatizerEN() {
		String text = "mirrors data from";
		List<String> tokensAsString = langProcessorEN.tokenize(text);
		String actLemmText = langProcessorEN.lemmatizeEN(tokensAsString, false);
		String expLemmText = "mirror datum from";
		assertThat(actLemmText).isEqualTo(expLemmText);
		
		String text3 = "is designed by";
		List<String> tokensAsString3 = langProcessorEN.tokenize(text3);
		String actLemmText3 = langProcessorEN.lemmatizeEN(tokensAsString3, false);
		String expLemmText3 = "be design by";
		assertThat(actLemmText3).isEqualTo(expLemmText3);
		
		String text2 = "Stanford is located in Palo Alto.";
		List<String> tokensAsString2 = langProcessorEN.tokenize(text2);
		String actLemmText2 = langProcessorEN.lemmatizeEN(tokensAsString2, false);
		String expLemmText2 = "stanford be locate in palo alto .";
		assertThat(actLemmText2).isEqualTo(expLemmText2);
		
		
	}

	
	@Test
	public void testLemmatizerENForAlias() {
		String alias = "Victorian Heritage Database ID";
		List<String> tokensAsString = langProcessorEN.tokenize(alias);
		String actLemmAlias = langProcessorEN.lemmatizeEN(tokensAsString, true);
		String expLemmAlias = "heritage database id";
		assertThat(actLemmAlias).isEqualTo(expLemmAlias);
	}

	@Test
	public void testLemmatizerDE() {
		String text = "Landesteil im Vereinigten Königreich Großbritannien und Nordirland";
		List<String> tokensAsString = langProcessorDE.tokenize(text);
		String actLemmText = langProcessorDE.lemmatizeDE(tokensAsString, false);
		String expLemmText = "landesteil in vereinigt königreich großbritannien und nordirland";
		assertThat(actLemmText).isEqualTo(expLemmText);
	}

	@Test
	public void testLemmatizerDEForAlias() {
		String alias = "Staat auf der Iberischen Halbinsel in Westeuropa am Mittelmeer und Atlantik";
		List<String> tokensAsString = langProcessorDE.tokenize(alias);
		String actLemmAlias = langProcessorDE.lemmatizeDE(tokensAsString, true);
		String expLemmAlias = "staat auf halbinsel in westeuropa an mittelmeer atlantik";
		assertThat(actLemmAlias).isEqualTo(expLemmAlias);
	}

}
