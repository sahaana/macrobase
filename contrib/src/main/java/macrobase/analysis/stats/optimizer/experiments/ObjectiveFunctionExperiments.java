package macrobase.analysis.stats.optimizer.experiments;

import macrobase.analysis.stats.PCASkiingDROP;
import macrobase.analysis.stats.optimizer.PCASkiingOptimizer;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.SchemalessCSVIngester;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by meep_me on 9/1/16.
 */
public class ObjectiveFunctionExperiments extends Experiment {
    public static String baseString = "contrib/src/main/java/macrobase/analysis/stats/optimizer/experiments/ObjectiveFuncExperiments/";
    public static DateFormat day = new SimpleDateFormat("MM-dd");
    public static DateFormat minute = new SimpleDateFormat("HH_mm");


    private static String timeOutFile(String dataset, double lbr, double qThresh, PCASkiingOptimizer.PCAAlgo algo, PCASkiingOptimizer.work reuse, Date date){
        String output = String.format("%s_%s_%s_lbr%.2f_q%.2f_%s",minute.format(date),dataset, algo, lbr, qThresh, reuse);
        return String.format(baseString + day.format(date) + "/KEXPvTime/%s.csv", output);
    }

    private static String kOutFile(String dataset, double lbr, double qThresh, PCASkiingOptimizer.PCAAlgo algo, PCASkiingOptimizer.work reuse, Date date){
        String output = String.format("%s_%s_%s_lbr%.2f_q%.2f_%s",minute.format(date),dataset, algo, lbr, qThresh, reuse);
        return String.format(baseString + day.format(date) + "/KEXPvK/%s.csv", output);
    }

    private static String predictedOutFile(String dataset, double lbr, double qThresh, int kExp, PCASkiingOptimizer.PCAAlgo algo, PCASkiingOptimizer.work reuse, Date date){
        String output = String.format("%s_%s_lbr%.2f_q%.2f_kexp%d_%s_%s",minute.format(date),dataset,lbr,qThresh,kExp,algo,reuse);
        return String.format(baseString + day.format(date) + "/objValue/%s_predicted.csv", output);
    }

    private static String trueOutFile(String dataset, double lbr, double qThresh, int kExp, PCASkiingOptimizer.PCAAlgo algo, PCASkiingOptimizer.work reuse, Date date){
        String output = String.format("%s_%s_lbr%.2f_q%.2f_kexp%d_%s_%s",minute.format(date),dataset,lbr,qThresh,kExp,algo,reuse);
        return String.format(baseString + day.format(date) + "/objValue/%s_true.csv", output);
    }

    //java ${JAVA_OPTS} -cp "assembly/target/*:core/target/classes:frontend/target/classes:contrib/target/classes" macrobase.analysis.stats.optimizer.experiments.SVDDropExperiments
    public static void main(String[] args) throws Exception{
        Date date = new Date();

        String dataset = args[0];
        double lbr = Double.parseDouble(args[1]);
        double qThresh = Double.parseDouble(args[2]);

        System.out.println(dataset);
        System.out.println(lbr);
        System.out.println(qThresh);

        PCASkiingOptimizer.PCAAlgo[] algos = {PCASkiingOptimizer.PCAAlgo.SVD, PCASkiingOptimizer.PCAAlgo.TROPP, PCASkiingOptimizer.PCAAlgo.FAST};
        PCASkiingOptimizer.work[] options = {PCASkiingOptimizer.work.NOREUSE, PCASkiingOptimizer.work.REUSE};
        int[] kExps = {1,2,3};

        Map<Integer, Long> runtimes;
        Map<Integer, Integer> finalKs;

        MacroBaseConf conf = new MacroBaseConf();

        SchemalessCSVIngester ingester = new SchemalessCSVIngester(String.format("contrib/src/test/resources/data/optimizer/raw/%s.csv", dataset));
        List<Datum> data = ingester.getStream().drain();

        for (PCASkiingOptimizer.PCAAlgo algo: algos){
            for (PCASkiingOptimizer.work reuse: options){
                runtimes = new HashMap<>();
                finalKs = new HashMap<>();
                for (int kExp: kExps){
                    PCASkiingDROP drop = new PCASkiingDROP(conf, qThresh, lbr, kExp, algo, reuse);
                    drop.consume(data);
                    runtimes.put(kExp,drop.totalTime());
                    finalKs.put(kExp, drop.finalK());
                    mapDoubleToCSV(drop.getTrueObjective(), trueOutFile(dataset,lbr,qThresh,kExp,algo,reuse,date));
                    mapDoubleToCSV(drop.getPredictedObjective(), predictedOutFile(dataset,lbr,qThresh,kExp,algo,reuse,date));
                }
                mapIntLongToCSV(runtimes, timeOutFile(dataset,lbr,qThresh,algo,reuse,date));
                mapIntToCSV(finalKs, kOutFile(dataset,lbr,qThresh,algo,reuse,date));
            }
        }
    }
}
