package de.dfki.mlt.wd_kbe.es;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONArray;
import org.json.JSONObject;

import de.dfki.lt.tools.tokenizer.JTok;
import de.dfki.lt.tools.tokenizer.annotate.AnnotatedString;
import de.dfki.lt.tools.tokenizer.output.Outputter;
import de.dfki.lt.tools.tokenizer.output.Token;
import de.dfki.mlt.wd_kbe.App;
import de.dfki.mlt.wd_kbe.Helper;
import de.dfki.mlt.wd_kbe.preferences.Config;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class ElasticsearchService {

	private Client client;
	private BulkProcessor bulkProcessor;
	private JTok jtok;
	protected StanfordCoreNLP pipeline;

	public ElasticsearchService() {
		getClient();
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

	private Client getClient() {
		if (client == null) {
			Settings settings = Settings.builder()
					.put(Config.CLUSTER_NAME, Config.getInstance().getString(Config.CLUSTER_NAME)).build();
			try {
				client = new PreBuiltTransportClient(settings).addTransportAddress(
						new InetSocketTransportAddress(InetAddress.getByName("134.96.187.233"), 9300));
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
		return client;
	}

	public boolean checkAndCreateIndex(String indexName) throws IOException, InterruptedException {
		boolean result = false;
		IndicesAdminClient indicesAdminClient = getClient().admin().indices();
		final IndicesExistsResponse indexExistReponse = indicesAdminClient.prepareExists(indexName).execute()
				.actionGet();
		if (indexExistReponse.isExists()) {
			deleteIndex(indicesAdminClient, indexName);
		}
		if (createIndex(indicesAdminClient, indexName)) {
			result = putMappingForEntity(indicesAdminClient);
		}
		return result;
	}

	private void deleteIndex(IndicesAdminClient indicesAdminClient, String indexName) {
		final DeleteIndexRequestBuilder delIdx = indicesAdminClient.prepareDelete(indexName);
		delIdx.execute().actionGet();
	}

	private boolean createIndex(IndicesAdminClient indicesAdminClient, String indexName) {
		final CreateIndexRequestBuilder createIndexRequestBuilder = indicesAdminClient.prepareCreate(indexName)
				.setSettings(Settings.builder()
						.put(Config.NUMBER_OF_SHARDS, Config.getInstance().getInt(Config.NUMBER_OF_SHARDS))
						.put(Config.NUMBER_OF_REPLICAS, Config.getInstance().getInt(Config.NUMBER_OF_REPLICAS)));
		CreateIndexResponse createIndexResponse = null;
		createIndexResponse = createIndexRequestBuilder.execute().actionGet();
		return createIndexResponse != null && createIndexResponse.isAcknowledged();
	}

	private boolean putMappingForEntity(IndicesAdminClient indicesAdminClient) throws IOException {
		XContentBuilder mappingBuilder = XContentFactory.jsonBuilder().startObject()
				.startObject(Config.getInstance().getString(Config.ENTITY_TYPE_NAME)).field("dynamic", "true")
				.startObject("properties").startObject("type").field("type", "keyword").field("index", "true")
				.endObject().startObject("label").field("type", "keyword").field("index", "true").endObject()
				.startObject("tok-label").field("type", "keyword").field("index", "true").endObject()
				.startObject("wiki-title").field("type", "keyword").field("index", "true").endObject()
				.startObject("claims").startObject("properties").startObject("property-id").field("type", "keyword")
				.field("index", "true").endObject().startObject("object-id").field("type", "keyword")
				.field("index", "true").endObject().endObject().endObject()// end claims
				.endObject() // properties
				.endObject() // documentType
				.endObject();

		App.logger.debug("Mapping for entity:" + mappingBuilder.string());
		PutMappingResponse putMappingResponse = indicesAdminClient
				.preparePutMapping(Config.getInstance().getString(Config.INDEX_NAME))
				.setType(Config.getInstance().getString(Config.ENTITY_TYPE_NAME)).setSource(mappingBuilder).execute()
				.actionGet();
		return putMappingResponse.isAcknowledged();
	}

	public void stopConnection() throws UnknownHostException {
		getBulkProcessor().close();
		getClient().close();
	}

	private BulkProcessor getBulkProcessor() {
		if (bulkProcessor == null) {
			bulkProcessor = BulkProcessor.builder(getClient(), new BulkProcessor.Listener() {
				@Override
				public void beforeBulk(long executionId, BulkRequest request) {
					App.logger.debug("Number of request processed: " + request.numberOfActions());
				}

				@Override
				public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
					if (response.hasFailures()) {
						App.logger.error("Elasticsearch Service getBulkProcessor() " + response.buildFailureMessage());
					}
				}

				@Override
				public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
					App.logger.error("Elasticsearch Service getBulkProcessor() " + failure.getMessage());

				}
			}).setBulkActions(10000).setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
					.setFlushInterval(TimeValue.timeValueSeconds(5)).setConcurrentRequests(1)
					.setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)).build();
		}

		return bulkProcessor;
	}

	public void insertEntities(JSONObject jsonObject) throws IOException {
		HashMap<String, Object> dataAsMap = new HashMap<String, Object>();
		String type = jsonObject.getString("type");
		dataAsMap.put("type", type);
		String id = jsonObject.getString("id");
		List<String> aliases = getAliases(jsonObject);
		dataAsMap.put("aliases", aliases);
		List<String> tokenizedAliases = getTokenizedAliases(aliases);
		dataAsMap.put("tok-aliases", tokenizedAliases);
		if (Helper.checkAttributeAvailable(jsonObject.getJSONObject("labels"), "en")) {
			JSONObject labelObject = jsonObject.getJSONObject("labels").getJSONObject("en");
			String label = labelObject.getString("value");
			aliases.add(label);
			dataAsMap.put("label", label);
			dataAsMap.put("tok-label", tokenizer(label, false));
		} else {
			dataAsMap.put("label", "no label");
			dataAsMap.put("tok-label", "no label");
		}

		if (Helper.checkAttributeAvailable(jsonObject, "sitelinks")
				&& Helper.checkAttributeAvailable(jsonObject.getJSONObject("sitelinks"), "enwiki")) {
			String wikipediaTitle = jsonObject.getJSONObject("sitelinks").getJSONObject("enwiki").getString("title")
					.replace(" ", "_");
			dataAsMap.put("wiki-title", wikipediaTitle);
		}

		HashMap<String, Object> claimMap = new HashMap<String, Object>();
		if (type.equals("item")) {
			JSONObject claims = jsonObject.getJSONObject("claims");
			Iterator<String> iterator = claims.keys();
			while (iterator.hasNext()) {
				String propertyId = iterator.next();
				JSONArray snakArray = claims.getJSONArray(propertyId);
				if (Helper.checkAttributeAvailable(((JSONObject) snakArray.get(0)).getJSONObject("mainsnak"),
						"datavalue")) {
					JSONObject dataJson = ((JSONObject) snakArray.get(0)).getJSONObject("mainsnak")
							.getJSONObject("datavalue");
					String dataType = dataJson.getString("type");
					if (dataType.equals("wikibase-entityid")) {
						String dataValue = dataJson.getJSONObject("value").getString("id");
						claimMap.put("property-id", propertyId);
						claimMap.put("object-id", dataValue);
					}
				}
			}
		}
		dataAsMap.put("claims", claimMap);

		IndexRequest indexRequest = Requests.indexRequest().index(Config.getInstance().getString(Config.INDEX_NAME))
				.type(Config.getInstance().getString(Config.ENTITY_TYPE_NAME)).id(id)
				.source(dataAsMap, XContentType.JSON);
		getBulkProcessor().add(indexRequest);

	}

	public String tokenizer(String text, boolean isAlias) {
		AnnotatedString annotatedString = jtok.tokenize(text, "en");
		List<Token> tokenList = Outputter.createTokens(annotatedString);
		StringBuilder builder = new StringBuilder();
		for (Token token : tokenList) {
			if (isAlias)
				builder.append(lemmatizeAliases(token.getImage()) + " ");
			else
				builder.append(lemmatize(token.getImage()) + " ");
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
				if (tag.startsWith("V") || tag.startsWith("N")) {
					String image = token.get(LemmaAnnotation.class);
					image = replaceParantheses(image);
					builder.append(image + " ");
				}
			}
		}
		return builder.toString().trim();
	}

	public String replaceParantheses(String image) {
		return image = image.replaceAll("-lrb-", "\\(").replaceAll("-rrb-", "\\)").replaceAll("-lcb-", "\\{")
				.replaceAll("-rcb-", "\\}").replaceAll("-lsb-", "\\[").replaceAll("-rsb-", "\\]");
	}

	private List<String> getAliases(JSONObject jsonObject) {
		List<String> aliases = new ArrayList<String>();
		if (Helper.checkAttributeAvailable(jsonObject.getJSONObject("aliases"), "en")) {
			JSONArray aliasArray = jsonObject.getJSONObject("aliases").getJSONArray("en");
			for (Object aliasObject : aliasArray) {
				aliases.add(((JSONObject) aliasObject).getString("value"));
			}
		}
		return aliases;
	}

	private List<String> getTokenizedAliases(List<String> aliases) {
		List<String> tokenizedAliases = new ArrayList<String>();
		for (String alias : aliases) {
			tokenizedAliases.add(tokenizer(alias, true));
		}
		return tokenizedAliases;
	}
}