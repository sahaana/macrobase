package macrobase.analysis.stats.optimizer.experiments;

import macrobase.analysis.stats.PCASkiingDROP;
import macrobase.analysis.stats.optimizer.PCASkiingOptimizer;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.SchemalessCSVIngester;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by meep_me on 9/1/16.
 */
public class IncreasingDatasizeExperiments extends Experiment {
    public static String baseString = "contrib/src/main/java/macrobase/analysis/stats/optimizer/experiments/IncreasingDataExperiments/";
    public static DateFormat day = new SimpleDateFormat("MM-dd");
    public static DateFormat minute = new SimpleDateFormat("HH_mm");


    private static String timeOutFile(String dataset, double lbr, double qThresh, PCASkiingOptimizer.PCAAlgo algo, PCASkiingOptimizer.work reuse, Date date){
        String output = String.format("%s_%s_%s_lbr%.2f_q%.2f_%s",minute.format(date),dataset, algo, lbr, qThresh, reuse);
        return String.format(baseString + day.format(date) + "/NvTime/%s.csv", output);
    }

    private static String kOutFile(String dataset, double lbr, double qThresh, PCASkiingOptimizer.PCAAlgo algo, PCASkiingOptimizer.work reuse, Date date){
        String output = String.format("%s_%s_%s_lbr%.2f_q%.2f_%s",minute.format(date),dataset, algo, lbr, qThresh, reuse);
        return String.format(baseString + day.format(date) + "/NvK/%s.csv", output);
    }

    //java ${JAVA_OPTS} -cp "assembly/target/*:core/target/classes:frontend/target/classes:contrib/target/classes" macrobase.analysis.stats.optimizer.experiments.SVDDropExperiments
    public static void main(String[] args) throws Exception{
        Date date = new Date();
        int numTrials = 50;
        long tempRuntime;
        int tempK;

        String dataset = args[0];
        double lbr = Double.parseDouble(args[1]);
        double qThresh = Double.parseDouble(args[2]);
        int kExp = Integer.parseInt(args[3]);
        PCASkiingOptimizer.work reuse = PCASkiingOptimizer.work.valueOf(args[4]);
        System.out.println(dataset);
        System.out.println(lbr);
        System.out.println(qThresh);
        System.out.println(kExp);
        System.out.println(reuse);


        PCASkiingOptimizer.PCAAlgo[] algos = {PCASkiingOptimizer.PCAAlgo.SVD, PCASkiingOptimizer.PCAAlgo.TROPP, PCASkiingOptimizer.PCAAlgo.FAST};

        Map<Integer, Long> runtimes;
        Map<Integer, Integer> finalKs;

        MacroBaseConf conf = new MacroBaseConf();

        List<Datum> data;



        for (PCASkiingOptimizer.PCAAlgo algo: algos) {
            data = getData(dataset);
            runtimes = new HashMap<>();
            finalKs = new HashMap<>();

            for (int d = 0; d <= 3; d++) {
                tempK = 0;
                tempRuntime = 0;

                for (int i = 0; i < numTrials; i++) {
                    PCASkiingDROP drop = new PCASkiingDROP(conf, qThresh, lbr, kExp, algo, reuse);
                    drop.consume(data);

                    tempK += drop.finalK();
                    tempRuntime += drop.totalTime();
                }
                runtimes.put(data.size(), tempRuntime / numTrials);
                finalKs.put(data.size(), tempK / numTrials);

                data.addAll(data);
            }
            mapIntLongToCSV(runtimes, timeOutFile(dataset,lbr,qThresh,algo,reuse,date));
            mapIntToCSV(finalKs, kOutFile(dataset,lbr,qThresh,algo,reuse,date));
        }

    }
}
