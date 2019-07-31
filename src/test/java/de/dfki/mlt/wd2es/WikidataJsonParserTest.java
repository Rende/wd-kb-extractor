package de.dfki.mlt.wd2es;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import de.dfki.mlt.wd2es.WikibaseDatatype;
import de.dfki.mlt.wd2es.WikidataJsonParser;

public class WikidataJsonParserTest {

	private WikidataJsonParser parser = new WikidataJsonParser();

	@SuppressWarnings("unchecked")
	@Test
	public void testConstructEntityDataMap() throws IOException {
		ClassLoader classLoader = getClass().getClassLoader();
		InputStream inputStream = classLoader.getResourceAsStream("test-entity.json");
		InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
		BufferedReader reader = new BufferedReader(streamReader);
		JSONObject jsonObject = new JSONObject(reader.readLine());
		reader.close();
		streamReader.close();
		inputStream.close();
		HashMap<String, Object> dataAsMap = parser.parseJson(jsonObject);
		HashMap<String, String> labels = (HashMap<String, String>) dataAsMap.get("labels");
		assertThat(labels.get("en")).isEqualTo("planet");
		assertThat(labels.get("de")).isEqualTo("Planet");

		HashMap<String, String> descriptions = (HashMap<String, String>) dataAsMap.get("descriptions");
		assertThat(descriptions.get("en")).isEqualTo("celestial body directly orbiting a star or stellar remnant");
		assertThat(descriptions.get("de")).isEqualTo(
				"nicht selbstleuchtender Himmelskörper, der sich um einen Stern bewegt und seine Umlaufbahn freigeräumt hat");
		
		HashMap<String, Set<String>> lemAliases = (HashMap<String, Set<String>>) dataAsMap.get("lem-aliases");
		assertThat(lemAliases.get("en")).containsExactly("body");
		System.out.println(dataAsMap.toString());
	}
	
	@Test
	public void test() throws JSONException, IOException {
		ClassLoader classLoader = getClass().getClassLoader();
		InputStream inputStream = classLoader.getResourceAsStream("berlin.json");
		InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
		BufferedReader reader = new BufferedReader(streamReader);
		JSONObject jsonObject = new JSONObject(reader.readLine());
		reader.close();
		streamReader.close();
		inputStream.close();
		HashMap<String, Object> dataAsMap = parser.parseJson(jsonObject);
		assertThat(dataAsMap.get("id")).isEqualTo("Q64");
	}
	
	

	@Test
	public void testWikibaseDatatype() {

		WikibaseDatatype url = WikibaseDatatype.fromString("url");
		WikibaseDatatype item = WikibaseDatatype.fromString("wikibase-item");
		WikibaseDatatype string = WikibaseDatatype.fromString("string");
		WikibaseDatatype externalId = WikibaseDatatype.fromString("external-id");
		
		assertThat(url).isEqualTo(WikibaseDatatype.URL);
		assertThat(item).isEqualTo(WikibaseDatatype.Item);
		assertThat(string).isEqualTo(WikibaseDatatype.String);
		assertThat(externalId).isEqualTo(WikibaseDatatype.ExternalId);
	}

}
