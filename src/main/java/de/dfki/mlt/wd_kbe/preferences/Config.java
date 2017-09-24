/**
 *
 */
package de.dfki.mlt.wd_kbe.preferences;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * @author Aydan Rende, DFKI
 *
 */
public final class Config {

	public static final String DIRECTORY_PATH = "directory.path";
	public static final String BULK_FLUSH_MAX_ACTIONS = "bulk.flush.max.actions";
	public static final String THREAD_NUM = "thread.num";
	public static final String CLUSTER_NAME = "cluster.name";
	public static final String HOST = "host";
	public static final String PORT = "port";
	public static final String NUMBER_OF_SHARDS = "number_of_shards";
	public static final String NUMBER_OF_REPLICAS = "number_of_replicas";

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
