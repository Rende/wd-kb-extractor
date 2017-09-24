package de.dfki.mlt.wd_kbe;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.json.JSONException;
import org.json.JSONObject;

import de.dfki.mlt.wd_kbe.es.ElasticsearchService;
import de.dfki.mlt.wd_kbe.preferences.Config;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {

		ElasticsearchService esService = new ElasticsearchService();
		try {
			esService.checkAndCreateIndex("wikidata-index-2");

		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		BufferedReader reader = null;
		try {
			reader = getBufferedReaderForCompressedFile(Config.getInstance()
					.getString(Config.DIRECTORY_PATH));
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
						esService.createEntityIndexRequest(jsonObject);
						esService.createClaimIndexRequest(jsonObject);
					}
				}
			} catch (JSONException | IOException e) {
				e.printStackTrace();
			}
		}
		try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static BufferedReader getBufferedReaderForCompressedFile(
			String fileIn) throws FileNotFoundException, CompressorException {
		FileInputStream inputStream = new FileInputStream(fileIn);
		BufferedInputStream bufferedStream = new BufferedInputStream(
				inputStream);
		CompressorInputStream compressorStream = new CompressorStreamFactory()
				.createCompressorInputStream(bufferedStream);
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				compressorStream, StandardCharsets.UTF_8));
		return reader;
	}
}
