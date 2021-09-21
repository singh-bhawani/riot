package com.redis.riot.gen;

import com.redis.riot.RiotApp;

import picocli.CommandLine.Command;

@Command(name = "riot-gen", subcommands = { GeneratorImportCommand.class, FakerHelpCommand.class })
public class RiotGen extends RiotApp {

	public static void main(String[] args) {
		System.exit(new RiotGen().execute(args));
	}

}