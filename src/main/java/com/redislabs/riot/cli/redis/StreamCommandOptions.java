package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.RedisDataStructureMapWriter;
import com.redislabs.riot.redis.writer.XaddIdMapWriter;
import com.redislabs.riot.redis.writer.XaddIdMaxlenMapWriter;
import com.redislabs.riot.redis.writer.XaddMapWriter;
import com.redislabs.riot.redis.writer.XaddMaxlenMapWriter;

import picocli.CommandLine.Option;

public class StreamCommandOptions {

	@Option(names = "--xadd-trim", description = "Use efficient trimming (~ flag)")
	private boolean xaddTrim;
	@Option(names = "--xadd-maxlen", description = "Limit stream to maxlen entries", paramLabel = "<int>")
	private Long xaddMaxlen;
	@Option(names = "--xadd-id", description = "Field used for stream entry IDs", paramLabel = "<field>")
	private String xaddId;

	public RedisDataStructureMapWriter writer() {
		if (xaddId == null) {
			if (xaddMaxlen == null) {
				return new XaddMapWriter();
			}
			XaddMaxlenMapWriter writer = new XaddMaxlenMapWriter();
			writer.setApproximateTrimming(xaddTrim);
			writer.setMaxlen(xaddMaxlen);
			return writer;
		}
		if (xaddMaxlen == null) {
			XaddIdMapWriter writer = new XaddIdMapWriter();
			writer.setIdField(xaddId);
			return writer;
		}
		XaddIdMaxlenMapWriter writer = new XaddIdMaxlenMapWriter();
		writer.setApproximateTrimming(xaddTrim);
		writer.setIdField(xaddId);
		writer.setMaxlen(xaddMaxlen);
		return writer;
	}

}
