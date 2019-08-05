package de.dfki.mlt.wd2es.es;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.dfki.mlt.wd2es.preferences.Config;

public class ElasticsearchService {

	private static final Logger logger = LoggerFactory.getLogger(ElasticsearchService.class);
	private Client client;
	private BulkProcessor bulkProcessor;

	public ElasticsearchService() {
		try {
			getClient();
			int bulkActions = Config.getInstance().getInt(Config.ES_BULK_ACTIONS);
			int bulkSize = Config.getInstance().getInt(Config.ES_BULK_SIZE);
			int flushInterval = Config.getInstance().getInt(Config.ES_BULK_FLUSH_INTERVAL);
			createBulkProcessor(bulkActions, bulkSize, flushInterval, 0);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("resource")
	private Client getClient() throws UnknownHostException {
		if (this.client == null) {
			Settings settings = Settings.builder()
					.put("cluster.name", Config.getInstance().getString(Config.ES_CLUSTER_NAME)).build();
			this.client = new PreBuiltTransportClient(settings).addTransportAddress(
					new TransportAddress(InetAddress.getByName(Config.getInstance().getString(Config.ES_HOST)),
							Config.getInstance().getInt(Config.ES_PORT)));
		}
		return this.client;
	}

	private void createBulkProcessor(int bulkActions, int bulkSize, int flushInterval, int concurrentRequests)
			throws UnknownHostException {
		if (this.bulkProcessor == null) {
			this.bulkProcessor = BulkProcessor.builder(getClient(), new BulkProcessor.Listener() {
				public void beforeBulk(long executionId, BulkRequest request) {
				}

				public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
					if (response.hasFailures()) {
						logger.error("ES afterBulk failure message: " + response.buildFailureMessage());
					}
				}

				public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
				}
			}).setBulkActions(bulkActions).setBulkSize(new ByteSizeValue(bulkSize, ByteSizeUnit.MB))
					.setFlushInterval(TimeValue.timeValueSeconds(flushInterval))
					.setConcurrentRequests(concurrentRequests)
					.setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)).build();
		}
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

	/**
	 * Defines schema for Wikidata-Entity
	 * 
	 * @param indicesAdminClient
	 * @return
	 * @throws IOException
	 */
	private boolean putMappingForEntity(IndicesAdminClient indicesAdminClient) throws IOException {
		XContentBuilder builder = XContentFactory.jsonBuilder();
		builder.startObject();

		builder.field("dynamic", "true");
		builder.startObject("properties");

		builder.startObject("type");
		builder.field("type", "keyword");
		builder.endObject();

		builder.startObject("datatype");
		builder.field("type", "keyword");
		builder.endObject();

		builder.startObject("labels");
		builder.field("type", "nested");
		builder.endObject();

		builder.startObject("lem-labels");
		builder.field("type", "nested");
		builder.endObject();

		builder.startObject("descriptions");
		builder.field("type", "nested");
		builder.endObject();

		builder.startObject("lem-descriptions");
		builder.field("type", "nested");
		builder.endObject();

		builder.startObject("aliases");
		builder.field("type", "nested");
		builder.endObject();

		builder.startObject("lem-aliases");
		builder.field("type", "nested");
		builder.endObject();

		builder.startObject("claims");
		builder.field("type", "nested");
		builder.endObject();

		builder.startObject("sitelinks");
		builder.field("type", "nested");
		builder.startObject("en");
		builder.field("type", "keyword");
		builder.endObject();
		builder.startObject("de");
		builder.field("type", "keyword");
		builder.endObject();
		builder.endObject();

		AcknowledgedResponse putMappingResponse = (AcknowledgedResponse) indicesAdminClient
				.preparePutMapping(new String[] { Config.getInstance().getString(Config.INDEX_NAME) })
				.setSource(builder).execute().actionGet();
		logger.debug("putMappingForEntity response: " + putMappingResponse.isAcknowledged());
		return putMappingResponse.isAcknowledged();
	}

	public void insertEntity(HashMap<String, Object> dataAsMap) throws IOException {
		String id = dataAsMap.get("id").toString();
		dataAsMap.remove("id");
		IndexRequest indexRequest = Requests.indexRequest().index(Config.getInstance().getString(Config.INDEX_NAME))
				.id(id).source(dataAsMap, XContentType.JSON);
		this.bulkProcessor.add(indexRequest);
	}

	public void stopConnection() throws UnknownHostException, InterruptedException {
		this.bulkProcessor.flush();
		TimeUnit.SECONDS.sleep(3);
		this.bulkProcessor.close();
		getClient().close();
	}

}