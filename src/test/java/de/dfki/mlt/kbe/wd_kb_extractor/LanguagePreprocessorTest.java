package de.dfki.mlt.kbe.wd_kb_extractor;

import static org.assertj.core.api.Assertions.assertThat;


import org.junit.Test;

import de.dfki.mlt.wd_kbe.LanguagePreprocessor;

public class LanguagePreprocessorTest {
	private LanguagePreprocessor languagePreprocessor = new LanguagePreprocessor();

	@Test
	public void testTokenizer() {
		String alias = "Norwegian List of Lights ID";
		String alias2 = "Victorian Heritage Database ID";
		String alias3 = "mirrors data from";
		
		String actualTokenizedAlias = languagePreprocessor.tokenizer(alias, true);
		String actualTokenizedAlias2 = languagePreprocessor.tokenizer(alias2, true);
		String actualTokenizedAlias3 = languagePreprocessor.tokenizer(alias3, true);
		
		String expectedTokenizedAlias = "list of light id";
		String expectedTokenizedAlias2 = "heritage database id";
		String expectedTokenizedAlias3 = "mirror datum from";
		
		assertThat(actualTokenizedAlias).isEqualTo(expectedTokenizedAlias);
		assertThat(actualTokenizedAlias2).isEqualTo(expectedTokenizedAlias2);
		assertThat(actualTokenizedAlias3).isEqualTo(expectedTokenizedAlias3);
	}

}
