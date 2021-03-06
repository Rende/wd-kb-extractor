package de.dfki.mlt.wd2es;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import de.dfki.mlt.wd2es.es.ElasticsearchService;
import de.dfki.mlt.wd2es.preferences.Config;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class App {
	public static final Logger logger = Logger.getLogger(App.class);

	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		logger.debug("Wikidata knowledgebase extraction started.");
		ElasticsearchService esService = new ElasticsearchService();
		try {
			esService.checkAndCreateIndex(Config.getInstance().getString(Config.INDEX_NAME));

		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		WikidataJsonParser parser = new WikidataJsonParser();
		BufferedReader reader = null;
		try {
			reader = getBufferedReaderForCompressedFile(Config.getInstance().getString(Config.DIRECTORY_PATH));
		} catch (FileNotFoundException | CompressorException e) {
			e.printStackTrace();
		}
		String line = new String();
		JSONObject jsonObject = new JSONObject();
		if (reader != null) {
			try {
				while ((line = reader.readLine()) != null) {
					if (!(line.equals("[") || line.equals("]"))) {
						jsonObject = new JSONObject(line);
						HashMap<String, Object> dataMap = parser.parseJson(jsonObject);
						esService.insertEntity(dataMap);
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {
			reader.close();
			esService.stopConnection();
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		long elapsedTimeMillis = System.currentTimeMillis() - start;
		float elapsedTimeHour = elapsedTimeMillis / (60 * 60 * 1000F);
		logger.info("Time spent in hours: " + elapsedTimeHour);

	}

	public static BufferedReader getBufferedReaderForCompressedFile(String fileIn)
			throws FileNotFoundException, CompressorException {
		FileInputStream inputStream = new FileInputStream(fileIn);
		BufferedInputStream bufferedStream = new BufferedInputStream(inputStream);
		CompressorInputStream compressorStream = new CompressorStreamFactory()
				.createCompressorInputStream(bufferedStream);
		BufferedReader reader = new BufferedReader(new InputStreamReader(compressorStream, StandardCharsets.UTF_8));
		return reader;
	}
}
