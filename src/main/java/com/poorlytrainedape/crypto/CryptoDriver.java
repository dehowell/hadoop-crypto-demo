package com.poorlytrainedape.crypto;

import org.apache.hadoop.util.ProgramDriver;

public class CryptoDriver {

	public static void main(String[] args) {
		try {
			ProgramDriver driver = new ProgramDriver();
			driver.addClass("encode", Encoder.class, "Map-reduce job to encode or decode text.");
			driver.addClass("analyze", LetterFrequencies.class, "Map-reduce job to analyze the letter frequencies of text.");
			driver.driver(args);
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}
}
