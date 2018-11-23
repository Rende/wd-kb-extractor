package de.dfki.mlt.kbe.wd_kb_extractor;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.HashMap;
import java.util.Set;

import org.json.JSONObject;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import de.dfki.mlt.wd_kbe.es.ElasticsearchService;
import de.dfki.mlt.wd_kbe.preferences.Config;

public class ElasticsearchServiceTest {

	private ElasticsearchService esService = new ElasticsearchService();

	@SuppressWarnings("unchecked")
	@Test
	public void testConstructEntityDataMap() throws IOException {
		if (Config.getInstance().getString(Config.LANG).equals("en")) {
			String id = "P3443";
			String query = "https://www.wikidata.org/w/api.php?action=wbgetentities&languages=en&format=json&ids=" + id;
			JSONObject jsonObject = processQuery(query);
			JSONObject jsonEntityObject = jsonObject.getJSONObject("entities").getJSONObject(id);

			HashMap<String, Object> dataAsMap = esService.constructEntityDataMap(jsonEntityObject);
			Set<String> tokenizedAliases = (Set<String>) dataAsMap.get("tok-aliases");
			assertThat(tokenizedAliases).containsExactly("vhd id", "heritage database id");
			System.out.println(dataAsMap.toString());
		}
	}

	public JSONObject processQuery(String query) throws UnsupportedEncodingException, IOException {
		URL get = new URL(query);
		Reader reader = new InputStreamReader(get.openStream(), "UTF-8");
		StringWriter output = new StringWriter();
		int next = reader.read();
		while (next != -1) {
			output.write(next);
			next = reader.read();
		}
		String result = output.toString();
		return new JSONObject(result);
	}

}
