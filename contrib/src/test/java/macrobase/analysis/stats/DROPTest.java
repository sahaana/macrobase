package macrobase.analysis.stats;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.CSVIngester;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by meep_me on 8/22/16.
 */
public class DROPTest {

    @Test
    public void injestTest() throws Exception{
        Map<Integer, String> TABLE_NAMES = Stream.of(
                "1,CinC",
                "2,InlineSkate",
                "3,HandOutlines",
                "4,Haptics",
                "5,MALLAT",
                "6,StarLightCurves",
                "7,Phoneme",
                "8,ElectricDevices",
                "9,CinC-PAA"
        ).collect(Collectors.toMap(k -> Integer.parseInt(k.split(",")[0]), v -> v.split(",")[1]));
        Map<Integer, Integer> TABLE_SIZE = Stream.of(
                "1,1500",
                "2,1500",
                "3,2500",
                "4,1000",
                "5,1000",
                "6,1000",
                "7,1000",
                "8,50",
                "9,25"
        ).collect(Collectors.toMap(k -> Integer.parseInt(k.split(",")[0]), v -> Integer.parseInt(v.split(",")[1])));

        int datasetID = 1;

        ArrayList<String> metrics = new ArrayList<>();
        for (int i = 0; i < TABLE_SIZE.get(datasetID); i++){
            metrics.add(Integer.toString(i));
        }


        int k = 10;
        int num_Nt = 50;
        int processedDim = 100;
        int b = 200;
        int s = 20;
        double lbr = .9;
        double epsilon = .01;


        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.CSV_INPUT_FILE, String.format("/Users/meep_me/Desktop/Spring Rotation/workspace/OPTIMIZER/macrobase/contrib/src/test/resources/data/optimizer/%s.csv", TABLE_NAMES.get(datasetID)));
        conf.set(MacroBaseConf.ATTRIBUTES, new ArrayList<>());
        conf.set(MacroBaseConf.METRICS, metrics);

        CSVIngester ingester = new CSVIngester(conf);
        List<Datum> data = ingester.getStream().drain();

        DROP drop = new DROP(new MacroBaseConf(), k, num_Nt, processedDim, epsilon, lbr, b, s);
        drop.consume(data);

        List<Datum> transformed = drop.getStream().drain();
        for (Datum td: transformed){
            double[] val = td.metrics().toArray();
            assert(val.length == k);
        }
    }

}
