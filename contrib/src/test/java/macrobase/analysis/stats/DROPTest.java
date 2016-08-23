package macrobase.analysis.stats;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by meep_me on 8/22/16.
 */
public class DROPTest {

    @Test
    public void simpleTest() throws Exception{
        double[][] testInput = {
                {15, 10, 0.2, 0.1},
                {5, 6, 0.1, 0.2},
                {1, 1, 0.5, 0.1},
                {4, 2, 0.3, 0.1},
                {0, 2, 0, 0.1}
        };

        List<Datum> data = new ArrayList<>();
        for (int i = 0; i < 5; i++){
            Datum d = new Datum(new ArrayList<>(), new ArrayRealVector(testInput[i]));
            data.add(d);
        }

        DROP drop = new DROP(new MacroBaseConf());
        drop.consume(data);

        List<Datum> transformed = drop.getStream().drain();
        for (Datum td: transformed){
            double[] val = td.metrics().toArray();
            assert(val.length == 2);
        }

    }

}
