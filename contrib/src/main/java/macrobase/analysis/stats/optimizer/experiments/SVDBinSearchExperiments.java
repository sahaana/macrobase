package macrobase.analysis.stats.optimizer.experiments;

import macrobase.analysis.stats.PCASkiingDROP;
import macrobase.analysis.stats.optimizer.PCASkiingOptimizer;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by meep_me on 9/1/16.
 */
public class SVDBinSearchExperiments extends Experiment {
    public static String baseString = "contrib/src/main/java/macrobase/analysis/stats/optimizer/experiments/SVDNoDROPExperiments/";
    public static DateFormat day = new SimpleDateFormat("MM-dd");
    public static DateFormat minute = new SimpleDateFormat("HH_mm");


    private static String kAndRuntimeOutFile(String dataset, double lbr, double qThresh, PCASkiingOptimizer.PCAAlgo algo, Date date){
        String output = String.format("%s_%s_%s_lbr%.2f_q%.2f",minute.format(date),dataset, algo, lbr, qThresh);
        return String.format(baseString + day.format(date) + "/%s.csv", output);
    }

    //java ${JAVA_OPTS} -cp "assembly/target/*:core/target/classes:frontend/target/classes:contrib/target/classes" macrobase.analysis.stats.optimizer.experiments.SVDDropExperiments
    public static void main(String[] args) throws Exception{
        Date date = new Date();
        int numTrials = 50;
        long[] tempKRuntime;
        long tempRuntime;
        int tempK;

        String dataset = args[0];
        double lbr = Double.parseDouble(args[1]);
        System.out.println(dataset);
        System.out.println(lbr);

        PCASkiingOptimizer.PCAAlgo algo = PCASkiingOptimizer.PCAAlgo.SVD;
        double qThresh = 1.96;
        Map<Integer, Long> Kruntimes = new HashMap<>();

        MacroBaseConf conf = new MacroBaseConf();

        List<Datum> data = getData(dataset);

        tempK = 0;
        tempRuntime = 0;

        for (int i = 0; i < numTrials; i++){
            PCASkiingDROP drop = new PCASkiingDROP(conf, qThresh, lbr, algo);
            tempKRuntime = drop.baselineSVD(data);
            tempK += tempKRuntime[0];
            tempRuntime += tempKRuntime[1];
        }

        //this is dumb
        Kruntimes.put(tempK / numTrials, tempRuntime / numTrials);

        mapIntLongToCSV(Kruntimes, kAndRuntimeOutFile(dataset,lbr,qThresh,algo,date));

    }
}
