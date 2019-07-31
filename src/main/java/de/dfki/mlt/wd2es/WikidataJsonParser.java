/**
 * 
 */
package de.dfki.mlt.wd_kbe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

import de.dfki.mlt.wd_kbe.preferences.Config;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class WikidataJsonParser {

	private final String[] languages = Config.getInstance().getStringArray(Config.LANG);
	private LanguageProcessor langProcessor = new LanguageProcessor();

	public HashMap<String, Object> parseJson(JSONObject jsonObject) {
		String type = jsonObject.getString("type");
		String dataType = "item";
		if (checkAttrAvailable(jsonObject, "datatype"))
			dataType = jsonObject.getString("datatype");
		String id = jsonObject.getString("id");
		HashMap<String, String> labels = getTextMap("labels", jsonObject);
		HashMap<String, String> lemLabels = getLemmatizedTextMap(labels);
		HashMap<String, String> descriptions = getTextMap("descriptions", jsonObject);
		HashMap<String, String> lemDescriptions = getLemmatizedTextMap(descriptions);
		HashMap<String, List<String>> aliases = getAliasMap(jsonObject);
		HashMap<String, Set<String>> lemAliases = getLemmatizedAliases(aliases);
		List<HashMap<String, String>> claims = getClaims(jsonObject);
		HashMap<String, String> siteLinks = getSiteLinks(jsonObject);

		HashMap<String, Object> dataAsMap = new HashMap<>();
		dataAsMap.put("type", type);
		dataAsMap.put("datatype", dataType);
		dataAsMap.put("id", id);
		dataAsMap.put("labels", labels);
		dataAsMap.put("lem-labels", lemLabels);
		dataAsMap.put("descriptions", descriptions);
		dataAsMap.put("lem-descriptions", lemDescriptions);
		dataAsMap.put("aliases", aliases);
		dataAsMap.put("lem-aliases", lemAliases);
		dataAsMap.put("claims", claims);
		dataAsMap.put("sitelinks", siteLinks);

		return dataAsMap;
	}

	private HashMap<String, String> getTextMap(String attr, JSONObject jsonObject) {
		HashMap<String, String> multilingualTextMap = new HashMap<>();
		for (String lang : languages) {
			if (checkAttrAvailable(jsonObject.getJSONObject(attr), lang)) {
				JSONObject innerObj = jsonObject.getJSONObject(attr).getJSONObject(lang);
				String value = innerObj.getString("value");
				multilingualTextMap.put(lang, value);
			}
		}
		return multilingualTextMap;
	}

	private HashMap<String, String> getLemmatizedTextMap(HashMap<String, String> textMap) {
		HashMap<String, String> lemMap = new HashMap<>();
		for (Map.Entry<String, String> entry : textMap.entrySet()) {
			if (entry.getKey().equals("en")) {
				lemMap.put(entry.getKey(), langProcessor.lemmatizeEN(entry.getValue(), false));
			} else if (entry.getKey().equals("de")) {
				lemMap.put(entry.getKey(), langProcessor.lemmatizeDE(entry.getValue(), false));
			}

		}
		return lemMap;
	}

	private HashMap<String, List<String>> getAliasMap(JSONObject jsonObject) {
		HashMap<String, List<String>> multilingualAliasMap = new HashMap<>();
		for (String lang : languages) {
			List<String> aliases = new ArrayList<String>();
			if (checkAttrAvailable(jsonObject.getJSONObject("aliases"), lang)) {
				JSONArray aliasArray = jsonObject.getJSONObject("aliases").getJSONArray(lang);
				for (Object aliasObject : aliasArray) {
					aliases.add(((JSONObject) aliasObject).getString("value"));
				}
			}
			multilingualAliasMap.put(lang, aliases);
		}
		return multilingualAliasMap;
	}

	private HashMap<String, Set<String>> getLemmatizedAliases(HashMap<String, List<String>> aliases) {
		HashMap<String, Set<String>> lemMap = new HashMap<>();
		for (Map.Entry<String, List<String>> entry : aliases.entrySet()) {
			Set<String> lemAliases = new HashSet<String>();
			if (entry.getKey().equals("en")) {
				for (String alias : entry.getValue()) {
					lemAliases.add(langProcessor.lemmatizeEN(alias, true));
				}
			} else if (entry.getKey().equals("de")) {
				for (String alias : entry.getValue()) {
					lemAliases.add(langProcessor.lemmatizeDE(alias, true));
				}
			}
			lemMap.put(entry.getKey(), lemAliases);
		}
		return lemMap;
	}

	private List<HashMap<String, String>> getClaims(JSONObject jsonObject) {
		List<HashMap<String, String>> claimList = new ArrayList<HashMap<String, String>>();
		if (checkAttrAvailable(jsonObject, "claims")) {
			JSONObject claims = jsonObject.getJSONObject("claims");
			Iterator<String> iterator = claims.keys();
			while (iterator.hasNext()) {
				String propertyId = iterator.next();
				JSONArray snakArray = claims.getJSONArray(propertyId);
				if (checkAttrAvailable(((JSONObject) snakArray.get(0)).getJSONObject("mainsnak"), "datatype")) {
					JSONObject mainsnak = ((JSONObject) snakArray.get(0)).getJSONObject("mainsnak");
					WikibaseDatatype datatype = WikibaseDatatype.fromString(mainsnak.getString("datatype"));
					if (datatype != null && checkAttrAvailable(mainsnak, "datavalue")) {
						JSONObject datavalueObj = mainsnak.getJSONObject("datavalue");
						HashMap<String, String> claim = getClaim(datatype, propertyId, datavalueObj);
						claimList.add(claim);
					}
				}
			}
		}
		return claimList;
	}

	private HashMap<String, String> getClaim(WikibaseDatatype datatype, String propertyId, JSONObject valueObject) {
		HashMap<String, String> claim = new HashMap<String, String>();
		claim.put("property-id", propertyId);
		switch (datatype) {
		case Item:
			JSONObject itemObj = valueObject.getJSONObject("value");
			String wikibaseItemId = itemObj.getString("id");
			claim.put(datatype.getDatatype(), wikibaseItemId);
			break;
		case ExternalId:
			String externalId = valueObject.getString("value");
			claim.put(datatype.getDatatype(), externalId);
			break;
		case Time:
			JSONObject timeObj = valueObject.getJSONObject("value");
			String time = timeObj.getString("time");
			claim.put(datatype.getDatatype(), time);
			break;
		case URL:
			String url = valueObject.getString("value");
			claim.put(datatype.getDatatype(), url);
			break;
		case String:
			String text = valueObject.getString("value");
			claim.put(datatype.getDatatype(), text);
			break;
		case MonolingualText:
			JSONObject monoObj = valueObject.getJSONObject("value");
			String mono = monoObj.getString("text");
			claim.put(datatype.getDatatype(), mono);
			break;
		case Lexeme:
			JSONObject lexemeObj = valueObject.getJSONObject("value");
			String lexemeId = lexemeObj.getString("id");
			claim.put(datatype.getDatatype(), lexemeId);
			break;
		case Sense:
			JSONObject senseObj = valueObject.getJSONObject("value");
			String senseId = senseObj.getString("id");
			claim.put(datatype.getDatatype(), senseId);
			break;
		case GlobeCoordinate:
			JSONObject globeObj = valueObject.getJSONObject("value");
			double lati = globeObj.getDouble("latitude");
			double longi = globeObj.getDouble("longitude");
			claim.put(datatype.getDatatype(), "lat:" + lati + ", long:" + longi);
			break;
		case CommonsMedia:
			String commonsMedia = valueObject.getString("value");
			claim.put(datatype.getDatatype(), commonsMedia);
			break;
		default:
			break;
		}

		return claim;
	}

	private HashMap<String, String> getSiteLinks(JSONObject jsonObject) {
		HashMap<String, String> multilingualSiteMap = new HashMap<>();
		for (String lang : languages) {
			if (checkAttrAvailable(jsonObject, "sitelinks")
					&& checkAttrAvailable(jsonObject.getJSONObject("sitelinks"), lang + "wiki")) {
				String wikipediaTitle = jsonObject.getJSONObject("sitelinks").getJSONObject(lang + "wiki")
						.getString("title").replace(" ", "_");
				multilingualSiteMap.put(lang, wikipediaTitle);
			}
		}
		return multilingualSiteMap;
	}

	public boolean checkAttrAvailable(JSONObject jsonObject, String attribute) {
		return jsonObject.has(attribute) && !jsonObject.isNull(attribute);
	}
}
