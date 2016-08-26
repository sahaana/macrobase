package macrobase.analysis.stats;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.CSVIngester;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by meep_me on 8/22/16.
 */
public class DROPTest {
    /*
    @Test
    public void simpleTest() throws Exception{
        /* this was used to check against python
        double[][] testInput = {
                {-1, -1, -1},
                {-2, -1, 2},
                {-3, -2, 1},
                {1, 1, 4},
                {2, 1, -2},
                {4, 3, 2}
        };

        int k = 3;
        int num_Nt = 1;

        double[][] testInput = {
                {15, 10, 0.2, 0.1},
                {5, 6, 0.1, 0.2},
                {1, 1, 0.5, 0.1},
                {4, 2, 0.3, 0.1},
                {0, 2, 0, 0.1},
                {1,10,2, 0.3},
                {20,1,3,.01},
                {1,4,0,.1},
                {10,1,0,.5},
                {18,.8,0,.01},
                {21,0,2,.2},
                {1,2,0,.7},
                {15,0,1,.9},
                {31,3,0,0},
                {1,8,9,12.1}
        };

        List<Datum> data = new ArrayList<>();
        for (int i = 0; i < testInput.length; i++){
            Datum d = new Datum(new ArrayList<>(), new ArrayRealVector(testInput[i]));
            data.add(d);
        }

        DROP drop = new DROP(new MacroBaseConf(), k, num_Nt);
        drop.consume(data);

        List<Datum> transformed = drop.getStream().drain();
        for (Datum td: transformed){
            double[] val = td.metrics().toArray();
            assert(val.length == k);
        }
    } */

    @Test
    public void injestTest() throws Exception{
        Map<Integer, String> TABLE_NAMES = ImmutableMap.of(1, "CinC", 2, "InlineSkate", 3, "HandOutlines", 4, "MALLAT");
        int datasetID = 1;


        int k = 10;
        int num_Nt = 20;


        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.CSV_INPUT_FILE, String.format("/Users/meep_me/Desktop/Spring Rotation/workspace/OPTIMIZER/macrobase/contrib/src/test/resources/data/optimizer/%s.csv", TABLE_NAMES.get(datasetID)));
        conf.set(MacroBaseConf.ATTRIBUTES, new ArrayList<>());
        conf.set(MacroBaseConf.METRICS, new ArrayList<>());
        conf.set(MacroBaseConf.METRICS, Lists.newArrayList("0.000000000000000000e+00", "1.000000000000000000e+00", "2.000000000000000000e+00", "3.000000000000000000e+00", "4.000000000000000000e+00", "5.000000000000000000e+00", "6.000000000000000000e+00", "7.000000000000000000e+00", "8.000000000000000000e+00", "9.000000000000000000e+00", "1.000000000000000000e+01", "1.100000000000000000e+01", "1.200000000000000000e+01", "1.300000000000000000e+01", "1.400000000000000000e+01", "1.500000000000000000e+01", "1.600000000000000000e+01", "1.700000000000000000e+01", "1.800000000000000000e+01", "1.900000000000000000e+01", "2.000000000000000000e+01", "2.100000000000000000e+01", "2.200000000000000000e+01", "2.300000000000000000e+01", "2.400000000000000000e+01"));

        CSVIngester ingester = new CSVIngester(conf);
        List<Datum> data = ingester.getStream().drain();

        DROP drop = new DROP(new MacroBaseConf(), k, num_Nt);
        drop.consume(data);

        List<Datum> transformed = drop.getStream().drain();
        for (Datum td: transformed){
            double[] val = td.metrics().toArray();
            assert(val.length == k);
        }
    }

}
