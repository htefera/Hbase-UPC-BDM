package bdm.labs.hbase;

import java.io.IOException;

import adult.avro.Adult;
import adult.data_model.Generator;
import bdm.labs.hbase.reader.MyHBaseReader;
import bdm.labs.hbase.reader.MyReader;
import bdm.labs.hbase.writer.MyHBaseWriter;
import bdm.labs.hbase.writer.MyWriter;

public class Main {

	private static MyReader input;
	private static MyWriter output;
	private static String file;

    public static void read() throws IOException {
        input.open(file);
        String line = input.next();
        while (line != null) {
            if (!line.equals("")) {
                System.out.println(line);
            }
            line = input.next();
        }
        input.close();
    }

    public static void write(long number) throws IOException {
        output.open(file);
        for (int inst = 0; inst < number; ++inst) {
            Adult a = Generator.generateNewInstance(System.currentTimeMillis());
            output.put(a);
            output.flush();
        }
        output.close();
    }

	public static void main(String[] args) {
		try {
			if (args[0].equals("-write")) {
                if (args[1].equals("-hbase-all")) {

                    output = new MyHBaseWriter();
                    file = args[3]; //here, file is replaced for the table name in HBase
                }

                write(Integer.parseInt(args[2]));


            }

			else if (args[0].equals("-read")) {
                long startTime = System.nanoTime();
                if (args[1].equals("-hbase-all")) {
                    input = new MyHBaseReader();
                }
                file = args[2]; //here, file is replaced for the table name in HBase
                read();
                long elapsedTime = System.nanoTime() - startTime;
                System.out.println("Time taken in MilliSecond"+ elapsedTime/1000000);
			}


		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
