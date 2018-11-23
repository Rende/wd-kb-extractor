package de.dfki.mlt.kbe.wd_kb_extractor;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Test;

import de.dfki.mlt.wd_kbe.LanguageProcessor;

public class LanguageProcessorTest {
	LanguageProcessor langProcessor = new LanguageProcessor();

	@Test
	public void testTokenizer() throws ConfigurationException, IOException {
		String alias = "Norwegian List of Lights ID";
		List<String> actualTokenizedAlias = langProcessor.tokenize(alias, "en");
		String[] expTokens = "Norwegian List of Lights ID".split(" ");
		assertThat(actualTokenizedAlias).isEqualTo(Arrays.asList(expTokens));
	}

	@Test
	public void testLemmatizerEN() {
		String text = "mirrors data from";
		String actLemmText = langProcessor.lemmatizeEN(text, false);
		String expLemmText = "mirror datum from";
		assertThat(actLemmText).isEqualTo(expLemmText);

		String text3 = "is designed by";
		String actLemmText3 = langProcessor.lemmatizeEN(text3, false);
		String expLemmText3 = "be design by";
		assertThat(actLemmText3).isEqualTo(expLemmText3);

//		String text2 = "Stanford is located in Palo Alto.";
//		String actLemmText2 = langProcessor.lemmatizeEN(text2, false);
//		String expLemmText2 = "stanford be locate in palo alto .";
//		assertThat(actLemmText2).isEqualTo(expLemmText2);

	}

	@Test
	public void testLemmatizerENForAlias() {
		String alias = "Victorian Heritage Database ID";
		String actLemmAlias = langProcessor.lemmatizeEN(alias, true);
		String expLemmAlias = "heritage database id";
		assertThat(actLemmAlias).isEqualTo(expLemmAlias);
	}

	@Test
	public void testLemmatizerDE() {
		String text = "Landesteil im Vereinigten Königreich Großbritannien und Nordirland";
		String actLemmText = langProcessor.lemmatizeDE(text, false);
		String expLemmText = "landesteil in vereinigt königreich großbritannien und nordirland";
		assertThat(actLemmText).isEqualTo(expLemmText);
	}

	@Test
	public void testLemmatizerDEForAlias() {
		String alias = "Staat auf der Iberischen Halbinsel in Westeuropa am Mittelmeer und Atlantik";
		String actLemmAlias = langProcessor.lemmatizeDE(alias, true);
		String expLemmAlias = "staat auf halbinsel in westeuropa an mittelmeer atlantik";
		assertThat(actLemmAlias).isEqualTo(expLemmAlias);
	}

}
