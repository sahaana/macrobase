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
public class OracleBinSearchExperiments extends Experiment {
    public static String baseString = "contrib/src/main/java/macrobase/analysis/stats/optimizer/experiments/OracleExperiments/";
    public static DateFormat day = new SimpleDateFormat("MM-dd");
    public static DateFormat minute = new SimpleDateFormat("HH_mm");


    private static String kAndRuntimeOutFile(String dataset, double lbr, double qThresh, PCASkiingOptimizer.PCAAlgo algo, double propn, Date date){
        String output = String.format("%s_%s_%s_lbr%.2f_q%.2f_p%.4f",minute.format(date),dataset, algo, lbr, qThresh, propn);
        return String.format(baseString + day.format(date) + "/KR/%s.csv", output);
    }

    private static String lbrOutFile(String dataset, double lbr, double qThresh, PCASkiingOptimizer.PCAAlgo algo, double propn, Date date){
        String output = String.format("%s_%s_%s_lbr%.2f_q%.2f_p%.4f",minute.format(date),dataset, algo, lbr, qThresh, propn);
        return String.format(baseString + day.format(date) + "/lbr/%s.csv", output);
    }

    //todo: spit out lbr too

    //java ${JAVA_OPTS} -cp "assembly/target/*:core/target/classes:frontend/target/classes:contrib/target/classes" macrobase.analysis.stats.optimizer.experiments.SVDDropExperiments
    public static void main(String[] args) throws Exception{
        Date date = new Date();
        long[] tempKRuntime;
        long tempRuntime;
        int tempK;

        String dataset = args[0];
        double lbr = Double.parseDouble(args[1]);
        PCASkiingOptimizer.PCAAlgo algo =  PCASkiingOptimizer.PCAAlgo.valueOf(args[2]);
        double propn = Double.parseDouble(args[3]);
        int numTrials = Integer.parseInt(args[4]);
        System.out.println(dataset);
        System.out.println(lbr);
        System.out.println(algo);
        System.out.println(propn);
        System.out.println(numTrials);

        double qThresh = 1.96;
        Map<Integer, Long> Kruntimes = new HashMap<>();
        double[] finalLBR = new double[]{0, 0, 0};

        MacroBaseConf conf = new MacroBaseConf();

        List<Datum> data = getData(dataset);

        tempK = 0;
        tempRuntime = 0;

        for (int i = 0; i < numTrials; i++){
            PCASkiingDROP drop = new PCASkiingDROP(conf, qThresh, lbr, algo);
            tempKRuntime = drop.oracleSVD(data, propn);
            tempK += tempKRuntime[0];
            tempRuntime += tempKRuntime[1];
            double[] templbr = drop.getFinalLBR();
            for (int ii = 0; ii < 3; ii++) {
                finalLBR[ii] += templbr[ii];
            }
        }

        //this is dumb //why?
        Kruntimes.put(tempK / numTrials, tempRuntime / numTrials);
        for (int ii = 0; ii < 3; ii++) {
            finalLBR[ii] = finalLBR[ii]/numTrials;
        }

        mapIntLongToCSV(Kruntimes, kAndRuntimeOutFile(dataset, lbr, qThresh, algo, propn, date));
        doubleListToCSV(finalLBR, lbrOutFile(dataset, lbr, qThresh, algo, propn, date));

    }
}
