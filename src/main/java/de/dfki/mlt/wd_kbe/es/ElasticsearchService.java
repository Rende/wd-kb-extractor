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
			result = putMappingForEntity(indicesAdminClient);
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
				.field("dynamic", "true").startObject("properties")
				.startObject("type").field("type", "string")
				.field("index", "not_analyzed").endObject()
				.startObject("label").field("type", "string")
				.field("index", "not_analyzed").endObject()
				.startObject("wiki-title").field("type", "string")
				.field("index", "not_analyzed").endObject()
				.startObject("claims").startObject("properties")
				.startObject("property-id").field("type", "string").field("index", "not_analyzed").endObject()
				.startObject("object-id").field("type", "string").field("index", "not_analyzed").endObject().endObject()
				.endObject()// end claims
				.endObject() // properties
				.endObject()// documentType
				.endObject();

		App.logger.debug("Mapping for entity:" + mappingBuilder.string());
		PutMappingResponse putMappingResponse = indicesAdminClient
				.preparePutMapping(
						Config.getInstance().getString(Config.INDEX_NAME))
				.setType(
						Config.getInstance().getString(Config.ENTITY_TYPE_NAME))
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
		getBulkProcessor().close();
		getClient().close();
	}

	private BulkProcessor getBulkProcessor() {
		if (bulkProcessor == null) {
			bulkProcessor = BulkProcessor
					.builder(getClient(), new BulkProcessor.Listener() {
						@Override
						public void beforeBulk(long executionId,
								BulkRequest request) {
							App.logger.debug("Number of request processed: "
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

	public void insertEntities(JSONObject jsonObj) throws IOException {
		String type = jsonObj.getString("type");
		String id = jsonObj.getString("id");
		String wikipediaTitle = "";
		String label = "";
		if (Helper.checkAttributeAvailable(jsonObj.getJSONObject("labels"),
				"en")) {
			JSONObject labelObj = jsonObj.getJSONObject("labels")
					.getJSONObject("en");
			label = labelObj.getString("value");
		} else {
			label = "no label";
		}
		if (Helper.checkAttributeAvailable(jsonObj, "sitelinks")
				&& Helper.checkAttributeAvailable(
						jsonObj.getJSONObject("sitelinks"), "enwiki")) {
			wikipediaTitle = jsonObj.getJSONObject("sitelinks")
					.getJSONObject("enwiki").getString("title")
					.replace(" ", "_");
		}
		List<String> aliases = getAliasList(jsonObj, label);
		XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
				.field("type", type).field("label", label)
				.field("wiki-title", wikipediaTitle).field("aliases", aliases)
				.startArray("claims");

		if (type.equals("item")) {
			JSONObject claims = jsonObj.getJSONObject("claims");
			Iterator<String> itr = claims.keys();
			while (itr.hasNext()) {
				String propertyId = itr.next();
				JSONArray snakArray = claims.getJSONArray(propertyId);
				if (Helper.checkAttributeAvailable(((JSONObject) snakArray
						.get(0)).getJSONObject("mainsnak"), "datavalue")) {
					JSONObject dataJson = ((JSONObject) snakArray.get(0))
							.getJSONObject("mainsnak").getJSONObject(
									"datavalue");
					String dataType = dataJson.getString("type");
					if (dataType.equals("wikibase-entityid")) {
						String dataValue = dataJson.getJSONObject("value")
								.getString("id");
						builder.startObject().field("property_id", propertyId)
								.field("object-id", dataValue).endObject();
					}
				}
			}
		}
		builder.endArray().endObject();

		IndexRequest indexRequest = Requests.indexRequest()
				.index(Config.getInstance().getString(Config.INDEX_NAME))
				.type(Config.getInstance().getString(Config.ENTITY_TYPE_NAME))
				.id(id).source(builder.string());
		getBulkProcessor().add(indexRequest);

	}

	private List<String> getAliasList(JSONObject jsonObj, String label) {
		List<String> aliases = new ArrayList<String>();
		aliases.add(label);
		if (Helper.checkAttributeAvailable(jsonObj.getJSONObject("aliases"),
				"en")) {
			JSONArray aliasArr = jsonObj.getJSONObject("aliases").getJSONArray(
					"en");
			for (Object aliasObj : aliasArr) {
				aliases.add(((JSONObject) aliasObj).getString("value"));
			}
		}
		return aliases;
	}
}