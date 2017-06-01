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
public class IncreasingDatasizeExperiments extends Experiment {
    public static String baseString = "contrib/src/main/java/macrobase/analysis/stats/optimizer/experiments/IncreasingDataExperiments/";
    public static DateFormat day = new SimpleDateFormat("MM-dd");
    public static DateFormat minute = new SimpleDateFormat("HH_mm");


    private static String timeOutFile(String dataset, double lbr, double qThresh, PCASkiingOptimizer.PCAAlgo algo, PCASkiingOptimizer.work reuse, Date date, PCASkiingOptimizer.optimize opt){
        String output = String.format("%s_%s_%s_lbr%.2f_q%.2f_%s_%s",minute.format(date),dataset, algo, lbr, qThresh, reuse, opt);
        return String.format(baseString + day.format(date) + "/NvTime/%s.csv", output);
    }

    private static String kOutFile(String dataset, double lbr, double qThresh, PCASkiingOptimizer.PCAAlgo algo, PCASkiingOptimizer.work reuse, Date date, PCASkiingOptimizer.optimize opt){
        String output = String.format("%s_%s_%s_lbr%.2f_q%.2f_%s_%s",minute.format(date),dataset, algo, lbr, qThresh, reuse, opt);
        return String.format(baseString + day.format(date) + "/NvK/%s.csv", output);
    }

    //java ${JAVA_OPTS} -cp "assembly/target/*:core/target/classes:frontend/target/classes:contrib/target/classes" macrobase.analysis.stats.optimizer.experiments.SVDDropExperiments
    public static void main(String[] args) throws Exception{
        Date date = new Date();
        int numTrials = 50;
        long tempRuntime;
        int tempK;

        String dataset1 = args[0];
        String dataset2 = args[1];
        String dataset3 = args[2];
        String dataset4 = args[3];
        double lbr = Double.parseDouble(args[4]);
        double qThresh = Double.parseDouble(args[5]);
        int kExp = Integer.parseInt(args[6]);
        PCASkiingOptimizer.work reuse = PCASkiingOptimizer.work.valueOf(args[7]);
        PCASkiingOptimizer.optimize opt =   PCASkiingOptimizer.optimize.valueOf(args[8]);
        System.out.println(dataset1);
        System.out.println(dataset2);
        System.out.println(dataset3);
        System.out.println(dataset4);
        System.out.println(lbr);
        System.out.println(qThresh);
        System.out.println(kExp);
        System.out.println(reuse);
        System.out.println(opt);


        String[] datasets = new String[] {dataset1, dataset2, dataset3, dataset4};
        PCASkiingOptimizer.PCAAlgo[] algos = {PCASkiingOptimizer.PCAAlgo.TROPP, PCASkiingOptimizer.PCAAlgo.FAST};

        Map<Integer, Long> runtimes;
        Map<Integer, Integer> finalKs;

        MacroBaseConf conf = new MacroBaseConf();

        List<Datum> data;

        for (PCASkiingOptimizer.PCAAlgo algo: algos) {
            runtimes = new HashMap<>();
            finalKs = new HashMap<>();

            for (String dataset: datasets) {
                data = getData(dataset);
                tempK = 0;
                tempRuntime = 0;

                for (int i = 0; i < numTrials; i++) {
                    PCASkiingDROP drop = new PCASkiingDROP(conf, qThresh, lbr, kExp, algo, reuse, opt);
                    drop.consume(data);

                    tempK += drop.finalK();
                    tempRuntime += drop.totalTime();
                }
                runtimes.put(data.size(), tempRuntime / numTrials);
                finalKs.put(data.size(), tempK / numTrials);
            }
            mapIntLongToCSV(runtimes, timeOutFile(dataset1,lbr,qThresh,algo,reuse,date,opt));
            mapIntToCSV(finalKs, kOutFile(dataset1,lbr,qThresh,algo,reuse,date,opt));
        }

    }
}