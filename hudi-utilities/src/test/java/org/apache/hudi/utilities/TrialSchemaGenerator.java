package org.apache.hudi.utilities;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class TrialSchemaGenerator {

  public static void main(String args[]) {
    boolean jsonFormat = true;
    String schemaFile = "/Users/sivabala/Documents/personal/projects/siva_hudi/hudi/hudi-utilities/src/test/resources/delta-streamer-config/source.avsc";
    Generator generator = null;
    try {
      generator = getGenerator(null, schemaFile);
    } catch (IOException ioe) {
      System.err.println("Error occurred while trying to read schema file");
      System.exit(1);
    }

    DatumWriter<Object> dataWriter = new GenericDatumWriter<>(generator.schema());
    try (OutputStream output = getOutput(null)) {
      Encoder encoder = EncoderFactory.get().jsonEncoder(generator.schema(), output, jsonFormat);
      for (int i = 0; i < 10; i++) {
        GenericRecord rec = (GenericRecord) generator.generate();
        System.out.println("Grec "+ i+" -> "+ rec);
        dataWriter.write(rec, encoder);
      }
      encoder.flush();
      output.write('\n');
    } catch (IOException ioe) {
      System.err.println(
          "Error occurred while trying to write to output file: " + ioe.getLocalizedMessage()
      );
      System.exit(1);
    }
  }


  private static Generator getGenerator(String schema, String schemaFile) throws IOException {
    if (schema != null) {
      return new Generator.Builder().schemaString(schema).build();
    } else if (!schemaFile.equals("-")) {
      return new Generator.Builder().schemaFile(new File(schemaFile)).build();
    } else {
      System.err.println("Reading schema from stdin...");
      return new Generator.Builder().schemaStream(System.in).build();
    }
  }

  private static OutputStream getOutput(String outputFile) throws IOException {
    if (outputFile != null && !outputFile.equals("-")) {
      return new FileOutputStream(outputFile);
    } else {
      return System.out;
    }
  }
}
