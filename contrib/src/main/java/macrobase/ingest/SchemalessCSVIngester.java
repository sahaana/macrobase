package macrobase.ingest;


import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

public class SchemalessCSVIngester {

    private String filename;
    private String splitBy;

    public SchemalessCSVIngester(String filename){
        this.filename = filename;
        this.splitBy = ",";
    }

    public MBStream<Datum> getStream() throws Exception {
        String line;
        RealVector record;
        String[] readLine;
        BufferedReader br = new BufferedReader(new FileReader(filename));

        MBStream<Datum> output = new MBStream<>();
        while ((line = br.readLine()) != null){
            int i = 0;
            readLine = line.split(splitBy);
            record = new ArrayRealVector(readLine.length);
            for (String entry: readLine){
                record.setEntry(i++, Double.parseDouble(entry));
            }
            output.add(new Datum(new ArrayList<>(), record));
        }
        return output;
    }
}


