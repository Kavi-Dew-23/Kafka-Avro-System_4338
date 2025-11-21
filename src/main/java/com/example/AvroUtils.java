package com.example;

import java.io.IOException;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import java.io.ByteArrayOutputStream;

public class AvroUtils {

    public static byte[] serialize(Order order) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<Order> writer = new SpecificDatumWriter<>(Order.class);

        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(order, encoder);
        encoder.flush();

        return out.toByteArray();
    }

    public static Order deserialize(byte[] data) throws IOException {
        DatumReader<Order> reader = new SpecificDatumReader<>(Order.class);
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        return reader.read(null, decoder);
    }
}
