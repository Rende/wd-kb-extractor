/**
 * 
 */
package de.dfki.mlt.wd_kbe;

/**
 * @author Aydan Rende, DFKI
 *
 */
public enum WikibaseDatatype {

	CommonsMedia("commonsMedia"),
	GlobeCoordinate("globe-coordinate"),
	Item("wikibase-item"),
	Property("wikibase-property"),
	String("string"),
	MonolingualText("monolingualtext"),
	Quantity("quatity"),
	Time("time"),
	URL("url"),
	MathExp("math"),
	ExternalId("external-id"),
	GeoShape("geo-shape"),
	TabularData("tabular-data"),
	Lexeme("wikibase-lexeme"),
	Form("wikibase-form"),
	Sense("wikibase-sense");
	
	private String datatype;
	
	WikibaseDatatype(String datatype) {
		this.datatype = datatype;
	}

	public String getDatatype() {
		return datatype;
	}

	public static WikibaseDatatype fromString(String text) {
		for (WikibaseDatatype type : WikibaseDatatype.values()) {
			if (type.datatype.equalsIgnoreCase(text)) {
				return type;
			}
		}
		return null;
	}
	
}
