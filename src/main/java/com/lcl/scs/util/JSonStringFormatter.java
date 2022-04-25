package com.lcl.scs.util;

import java.io.StringWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonValue;
import javax.json.JsonWriter;
import javax.json.JsonWriterFactory;
import javax.json.stream.JsonGenerator;

public class JSonStringFormatter {
	public static String format(JsonValue json) {
	    StringWriter stringWriter = new StringWriter();
	    prettyPrint(json, stringWriter);
	    return stringWriter.toString();
	}

	public static void prettyPrint(JsonValue json, Writer writer) {
	    Map<String, Object> config =
	            Collections.singletonMap(JsonGenerator.PRETTY_PRINTING, true);
	    JsonWriterFactory writerFactory = Json.createWriterFactory(config);
	    try (JsonWriter jsonWriter = writerFactory.createWriter(writer)) {
	        jsonWriter.write(json);
	    }
	}
}
