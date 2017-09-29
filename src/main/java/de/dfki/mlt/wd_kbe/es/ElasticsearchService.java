package de.dfki.mlt.wd_kbe.es;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.common.collect.ImmutableList;

import de.dfki.mlt.wd_kbe.App;
import de.dfki.mlt.wd_kbe.Helper;
import de.dfki.mlt.wd_kbe.preferences.Config;

public class ElasticsearchService {

	private Client client;
	private BulkProcessor bulkProcessor;

	public ElasticsearchService() {
		getClient();
	}

	private Client getClient() {
		if (client == null) {
			Map<String, String> userConfig = getUserConfig();
			List<InetSocketAddress> transportAddresses = getTransportAddresses();
			List<TransportAddress> transportNodes;
			transportNodes = new ArrayList<>(transportAddresses.size());
			for (InetSocketAddress address : transportAddresses) {
				transportNodes.add(new InetSocketTransportAddress(address));
			}
			Settings settings = Settings.settingsBuilder().put(userConfig)
					.build();

			TransportClient transportClient = TransportClient.builder()
					.settings(settings).build();
			for (TransportAddress transport : transportNodes) {
				transportClient.addTransportAddress(transport);
			}

			// verify that we actually are connected to a cluster
			ImmutableList<DiscoveryNode> nodes = ImmutableList
					.copyOf(transportClient.connectedNodes());
			if (nodes.isEmpty()) {
				throw new RuntimeException(
						"Client is not connected to any Elasticsearch nodes!");
			}

			client = transportClient;
		}
		return client;
	}

	public boolean checkAndCreateIndex(String indexName) throws IOException,
			InterruptedException {
		boolean result = false;
		IndicesAdminClient indicesAdminClient = getClient().admin().indices();
		final IndicesExistsResponse indexExistReponse = indicesAdminClient
				.prepareExists(indexName).execute().actionGet();
		if (indexExistReponse.isExists()) {
			deleteIndex(indicesAdminClient, indexName);
		}
		if (createIndex(indicesAdminClient, indexName)) {
			result = putMappingForEntity(indicesAdminClient)
					&& putMappingForClaim(indicesAdminClient);
		}
		return result;
	}

	private void deleteIndex(IndicesAdminClient indicesAdminClient,
			String indexName) {
		final DeleteIndexRequestBuilder delIdx = indicesAdminClient
				.prepareDelete(indexName);
		delIdx.execute().actionGet();
	}

	private boolean createIndex(IndicesAdminClient indicesAdminClient,
			String indexName) {
		final CreateIndexRequestBuilder createIndexRequestBuilder = indicesAdminClient
				.prepareCreate(indexName).setSettings(
						Settings.settingsBuilder()
								.put(Config.NUMBER_OF_SHARDS,
										Config.getInstance().getInt(
												Config.NUMBER_OF_SHARDS))
								.put(Config.NUMBER_OF_REPLICAS,
										Config.getInstance().getInt(
												Config.NUMBER_OF_REPLICAS)));
		CreateIndexResponse createIndexResponse = null;
		createIndexResponse = createIndexRequestBuilder.execute().actionGet();
		return createIndexResponse != null
				&& createIndexResponse.isAcknowledged();
	}

	private boolean putMappingForEntity(IndicesAdminClient indicesAdminClient)
			throws IOException {
		XContentBuilder mappingBuilder = XContentFactory
				.jsonBuilder()
				.startObject()
				.startObject(
						Config.getInstance().getString(Config.ENTITY_TYPE_NAME))
				.startObject("properties").startObject("id")
				.field("type", "string").field("index", "not_analyzed")
				.endObject().startObject("type").field("type", "string")
				.field("index", "not_analyzed").endObject()
				.startObject("org_label").field("type", "string")
				.field("index", "not_analyzed").endObject()
				.startObject("label").field("type", "string").endObject()
				.startObject("wikipedia_title").field("type", "string")
				.field("index", "not_analyzed").endObject().endObject() // properties
				.endObject()// documentType
				.endObject();

		App.logger.info("Mapping for entity:" + mappingBuilder.string());
		PutMappingResponse putMappingResponse = indicesAdminClient
				.preparePutMapping(
						Config.getInstance().getString(Config.INDEX_NAME))
				.setType(
						Config.getInstance().getString(Config.ENTITY_TYPE_NAME))
				.setSource(mappingBuilder).execute().actionGet();
		return putMappingResponse.isAcknowledged();
	}

	private boolean putMappingForClaim(IndicesAdminClient indicesAdminClient)
			throws IOException {
		XContentBuilder mappingBuilder = XContentFactory
				.jsonBuilder()
				.startObject()
				.startObject(
						Config.getInstance().getString(Config.CLAIM_TYPE_NAME))
				.startObject("properties").startObject("entity_id")
				.field("type", "string").field("index", "not_analyzed")
				.endObject().startObject("property_id").field("type", "string")
				.field("index", "not_analyzed").endObject()
				.startObject("data_type").field("type", "string")
				.field("index", "not_analyzed").endObject()
				.startObject("data_value").field("type", "string")
				.field("index", "not_analyzed").endObject().endObject() // properties
				.endObject()// document Type
				.endObject();
		App.logger.info("Mapping for claim:" + mappingBuilder.string());
		PutMappingResponse putMappingResponse = indicesAdminClient
				.preparePutMapping(
						Config.getInstance().getString(Config.INDEX_NAME))
				.setType(Config.getInstance().getString(Config.CLAIM_TYPE_NAME))
				.setSource(mappingBuilder).execute().actionGet();
		return putMappingResponse.isAcknowledged();
	}

	public static Map<String, String> getUserConfig() {
		Map<String, String> config = new HashMap<>();
		config.put(Config.BULK_FLUSH_MAX_ACTIONS, Config.getInstance()
				.getString(Config.BULK_FLUSH_MAX_ACTIONS));
		config.put(Config.CLUSTER_NAME,
				Config.getInstance().getString(Config.CLUSTER_NAME));

		return config;
	}

	public static List<InetSocketAddress> getTransportAddresses() {
		List<InetSocketAddress> transports = new ArrayList<>();
		try {
			transports.add(new InetSocketAddress(InetAddress.getByName(Config
					.getInstance().getString(Config.HOST)), Config
					.getInstance().getInt(Config.PORT)));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return transports;
	}

	public void stopConnection() {
		getClient().close();
		getBulkProcessor().close();
	}

	private BulkProcessor getBulkProcessor() {
		if (bulkProcessor == null) {
			bulkProcessor = BulkProcessor
					.builder(getClient(), new BulkProcessor.Listener() {
						@Override
						public void beforeBulk(long executionId,
								BulkRequest request) {
							App.logger.info("Number of request processed: "
									+ request.numberOfActions());
						}

						@Override
						public void afterBulk(long executionId,
								BulkRequest request, BulkResponse response) {
							if (response.hasFailures()) {
								App.logger
										.error("Elasticsearch Service getBulkProcessor() "
												+ response
														.buildFailureMessage());
							}
						}

						@Override
						public void afterBulk(long executionId,
								BulkRequest request, Throwable failure) {
							App.logger
									.error("Elasticsearch Service getBulkProcessor() "
											+ failure.getMessage());

						}
					})
					.setBulkActions(10000)
					.setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
					.setFlushInterval(TimeValue.timeValueSeconds(5))
					.setConcurrentRequests(1)
					.setBackoffPolicy(
							BackoffPolicy.exponentialBackoff(
									TimeValue.timeValueMillis(100), 3)).build();
		}

		return bulkProcessor;
	}

	public void createEntityIndexRequest(JSONObject jsonObj) throws IOException {
		String type = jsonObj.getString("type");
		String id = jsonObj.getString("id");
		String wikipediaTitle = "";
		String orgLabel = "";
		if (Helper.checkAttributeAvailable(jsonObj.getJSONObject("labels"),
				"en")) {
			JSONObject labelObj = jsonObj.getJSONObject("labels")
					.getJSONObject("en");
			orgLabel = labelObj.getString("value");
			if (Helper.checkAttributeAvailable(
					jsonObj.getJSONObject("sitelinks"), "enwiki")) {
				wikipediaTitle = jsonObj.getJSONObject("sitelinks")
						.getJSONObject("enwiki").getString("title")
						.replace(" ", "_");
			}
			XContentBuilder builder = XContentFactory.jsonBuilder()
					.startObject().field("id", id).field("type", type)
					.field("org_label", orgLabel).field("label", orgLabel)
					.field("wikipedia_title", wikipediaTitle).endObject();

			IndexRequest indexRequest = Requests
					.indexRequest()
					.index(Config.getInstance().getString(Config.INDEX_NAME))
					.type(Config.getInstance().getString(
							Config.ENTITY_TYPE_NAME)).source(builder.string());
			getBulkProcessor().add(indexRequest);

			// if there are aliases, will be inserted as independent docs
			if (Helper.checkAttributeAvailable(
					jsonObj.getJSONObject("aliases"), "en")) {
				JSONArray aliasArr = jsonObj.getJSONObject("aliases")
						.getJSONArray("en");
				String label = "";
				for (Object aliasObj : aliasArr) {
					label = ((JSONObject) aliasObj).getString("value");
					builder = XContentFactory.jsonBuilder().startObject()
							.field("id", id).field("type", type)
							.field("org_label", orgLabel).field("label", label)
							.field("wikipedia_title", wikipediaTitle)
							.endObject();

					getBulkProcessor().add(
							Requests.indexRequest()
									.index(Config.getInstance().getString(
											Config.INDEX_NAME))
									.type(Config.getInstance().getString(
											Config.ENTITY_TYPE_NAME))
									.source(builder.string()));
				}
			}
		}

	}

	public void createClaimIndexRequest(JSONObject jsonObj) throws IOException {
		String entityType = jsonObj.getString("type");
		String entityId = jsonObj.getString("id");
		if (entityType.equals("item")) {
			IndexRequest indexRequest = new IndexRequest();
			JSONObject claims = jsonObj.getJSONObject("claims");
			Iterator<String> itr = claims.keys();
			while (itr.hasNext()) {
				String propertyId = itr.next();
				JSONArray snakArray = claims.getJSONArray(propertyId);
				XContentBuilder builder = buildClaimRequest(entityId,
						propertyId, snakArray);
				if (builder != null) {
					indexRequest = Requests
							.indexRequest()
							.index(Config.getInstance().getString(
									Config.INDEX_NAME))
							.type(Config.getInstance().getString(
									Config.CLAIM_TYPE_NAME))
							.source(builder.string());
				}
				getBulkProcessor().add(indexRequest);
			}
		}
	}

	private XContentBuilder buildClaimRequest(String entityId,
			String propertyId, JSONArray snakArray) throws IOException {
		XContentBuilder builder = null;
		JSONObject dataJson = new JSONObject();
		String dataValue = "";
		String dataType = "";
		if (Helper.checkAttributeAvailable(
				((JSONObject) snakArray.get(0)).getJSONObject("mainsnak"),
				"datavalue")) {
			dataJson = ((JSONObject) snakArray.get(0))
					.getJSONObject("mainsnak").getJSONObject("datavalue");
			dataType = dataJson.getString("type");

			switch (dataType) {
			case "string":
				dataValue = dataJson.getString("value");
				break;
			case "wikibase-entityid":
				dataValue = dataJson.getJSONObject("value").getString("id");
				break;
			case "globecoordinate":
				dataValue = dataJson.getJSONObject("value").get("latitude")
						+ ";"
						+ dataJson.getJSONObject("value").get("longitude");
				break;
			case "quantity":
				dataValue = dataJson.getJSONObject("value").getString("amount")
						+ ";"
						+ dataJson.getJSONObject("value").getString("unit");
				break;
			case "time":
				dataValue = dataJson.getJSONObject("value").getString("time");
				break;
			default:
				break;
			}
			builder = XContentFactory.jsonBuilder().startObject()
					.field("entity_id", entityId)
					.field("property_id", propertyId)
					.field("data_type", dataType)
					.field("data_value", dataValue).endObject();
		}
		return builder;
	}

}