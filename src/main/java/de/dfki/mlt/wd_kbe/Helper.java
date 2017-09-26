/**
 *
 */
package de.dfki.mlt.wd_kbe;

import org.json.JSONObject;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class Helper {
	public static boolean checkAttributeAvailable(JSONObject jsonObject,
			String attribute) {
		return jsonObject.has(attribute) && !jsonObject.isNull(attribute);
	}

}
