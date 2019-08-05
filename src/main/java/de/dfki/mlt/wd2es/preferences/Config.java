/**
 *
 */
package de.dfki.mlt.wd2es.preferences;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * @author Aydan Rende, DFKI
 *
 */
public final class Config {

	public static final String DIRECTORY_PATH = "directory.path";
	public static final String ES_CLUSTER_NAME = "es.cluster.name";
	public static final String ES_HOST = "es.host";
	public static final String ES_PORT = "es.port";
	public static final String ES_BULK_ACTIONS = "es.bulk.actions";
	public static final String ES_BULK_SIZE = "es.bulk.size";
	public static final String ES_BULK_FLUSH_INTERVAL = "es.bulk.flush.interval";
	public static final String NUMBER_OF_SHARDS = "number_of_shards";
	public static final String NUMBER_OF_REPLICAS = "number_of_replicas";
	public static final String INDEX_NAME = "index.name";
	public static final String LANG = "lang";

	private static PropertiesConfiguration config;

	private Config() { 

	}

	private static void loadProps() {
		try {
			config = new PropertiesConfiguration("config.properties");
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
	}

	public static PropertiesConfiguration getInstance() {

		if (config == null) {
			loadProps();
		}
		return config;
	}

}
