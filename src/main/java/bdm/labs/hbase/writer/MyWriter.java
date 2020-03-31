package bdm.labs.hbase.writer;

import adult.avro.Adult;

import java.io.IOException;

public interface MyWriter {

    public void open(String file) throws IOException;

    public void put(Adult a);

    public void reset();

    public int flush() throws IOException;

    public void close() throws IOException;

}
