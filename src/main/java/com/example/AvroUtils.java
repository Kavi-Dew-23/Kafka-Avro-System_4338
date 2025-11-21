package com.example;

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

    public static Order deserialize(byte[] data) throws Exception {
        DatumReader<Order> reader = new SpecificDatumReader<>(Order.class);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        return reader.read(null, decoder);
    }
}
