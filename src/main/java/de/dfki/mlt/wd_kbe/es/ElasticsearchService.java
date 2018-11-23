package de.dfki.mlt.wd_kbe.es;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;

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

import de.dfki.mlt.wd_kbe.App;
import de.dfki.mlt.wd_kbe.preferences.Config;

public class ElasticsearchService {

	private Client client;
	private BulkProcessor bulkProcessor;

	public ElasticsearchService() {
		getClient();
	}

	@SuppressWarnings("resource")
	private Client getClient() {
		if (this.client == null) {
			Settings settings = Settings.builder()
					.put(Config.CLUSTER_NAME, Config.getInstance().getString(Config.CLUSTER_NAME)).build();
			try {
				this.client = new PreBuiltTransportClient(settings).addTransportAddress(
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

	public void insertEntity(HashMap<String, Object> dataAsMap) throws IOException {
		String id = dataAsMap.get("id").toString();
		dataAsMap.remove("id");
		IndexRequest indexRequest = Requests.indexRequest().index(Config.getInstance().getString(Config.INDEX_NAME))
				.type(Config.getInstance().getString(Config.ENTITY_TYPE_NAME)).id(id)
				.source(dataAsMap, XContentType.JSON);
		getBulkProcessor().add(indexRequest);
	}

}