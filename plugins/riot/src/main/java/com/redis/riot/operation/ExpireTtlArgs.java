package com.redis.riot.operation;

import picocli.CommandLine.Option;

public class ExpireTtlArgs {

	@Option(names = "--ttl-field", required = true, description = "Expire timeout field.", paramLabel = "<field>")
	private String ttlField;

	@Option(names = "--ttl", required = true, description = "Expire timeout duration in milliseconds.", paramLabel = "<ms>")
	private long ttlValue;

	public String getTtlField() {
		return ttlField;
	}

	public void setTtlField(String ttlField) {
		this.ttlField = ttlField;
	}

	public long getTtlValue() {
		return ttlValue;
	}

	public void setTtlValue(long ttlValue) {
		this.ttlValue = ttlValue;
	}

	@Override
	public String toString() {
		return "ExpireTtlArgs [ttlField=" + ttlField + ", ttlValue=" + ttlValue + "]";
	}

}