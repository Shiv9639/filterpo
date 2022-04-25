package com.lcl.scs.util.mongo;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@ConfigurationProperties(prefix = "spring.data.mongodb")
@Configuration("mongoSettings")
public class MongoSettings {
	//@Value("${spring.data.mongodb.uri}")
	private String uri;
	//@Value("${spring.data.mongodb.database}")
	private String database;
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}
	public String getDatabase() {
		return database;
	}
	public void setDatabase(String database) {
		this.database = database;
	}
	
}
