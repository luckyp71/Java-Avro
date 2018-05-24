package org.example.java_avro.example;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.example.java_avro.schema.Stock_Symbol;

public class Example1 {

	public static void main(String args[]) {
		serialize();
		deserialize();
	}

	public static void serialize() {

		// Serialize stock1, stock2 stock3 to disk
		DatumWriter<Stock_Symbol> stockDatumWriter = new SpecificDatumWriter<>(Stock_Symbol.class);
		DataFileWriter<Stock_Symbol> dataFileWriter = new DataFileWriter<>(stockDatumWriter);

		Stock_Symbol stock1 = Stock_Symbol.newBuilder().setSymbol("NVDA").setPrice((long) 4000)
				.setCompany("NVIDIA Corporation").build();

		Stock_Symbol stock2 = Stock_Symbol.newBuilder().setSymbol("MSFT").setPrice((long) 3000)
				.setCompany("Microsoft Corporation").build();

		Stock_Symbol stock3 = Stock_Symbol.newBuilder().setSymbol("AAPL").setPrice((long) 500).setCompany("Apple Inc")
				.build();
		try {
			// create the avro file for serialization
			dataFileWriter.create(stock1.getSchema(), new File("stocks.avro"));

			// append all the stocks
			dataFileWriter.append(stock1);
			dataFileWriter.append(stock2);
			dataFileWriter.append(stock3);

			dataFileWriter.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void deserialize() {
		try {
			DatumReader<Stock_Symbol> stockDatumReader = new SpecificDatumReader<>(Stock_Symbol.class);
			@SuppressWarnings("resource")
			DataFileReader<Stock_Symbol> dataFileReader = new DataFileReader<Stock_Symbol>(new File("stocks.avro"),
					stockDatumReader);
			Stock_Symbol stock = null;
			while(dataFileReader.hasNext()) {
				stock = dataFileReader.next(stock);
				System.out.println("Deserialized: "+stock);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}