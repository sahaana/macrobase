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
        /* this was used to check against python
        double[][] testInput = {
                {-1, -1, -1},
                {-2, -1, 2},
                {-3, -2, 1},
                {1, 1, 4},
                {2, 1, -2},
                {4, 3, 2}
        };
        */

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

        DROP drop = new DROP(new MacroBaseConf());
        drop.consume(data);

        List<Datum> transformed = drop.getStream().drain();
        for (Datum td: transformed){
            double[] val = td.metrics().toArray();
            assert(val.length == 3);
        }

    }

}
