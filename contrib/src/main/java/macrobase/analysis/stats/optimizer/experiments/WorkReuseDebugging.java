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
public class WorkReuseDebugging extends Experiment {
    public static String baseString = "contrib/src/main/java/macrobase/analysis/stats/optimizer/experiments/reuseDebug/";
    public static DateFormat day = new SimpleDateFormat("MM-dd");
    public static DateFormat minute = new SimpleDateFormat("HH_mm");


    private static String timeOutFile(String dataset, double lbr, double qThresh, PCASkiingOptimizer.PCAAlgo algo, Date date, PCASkiingOptimizer.optimize opt){
        String output = String.format("%s_%s_%s_lbr%.2f_q%.2f_%s",minute.format(date),dataset, algo, lbr, qThresh, opt);
        return String.format(baseString + day.format(date) + "/PvTime/%s.csv", output);
    }

    private static String kOutFile(String dataset, double lbr, double qThresh, PCASkiingOptimizer.PCAAlgo algo, Date date, PCASkiingOptimizer.optimize opt){
        String output = String.format("%s_%s_%s_lbr%.2f_q%.2f_%s",minute.format(date),dataset, algo, lbr, qThresh, opt);
        return String.format(baseString + day.format(date) + "/PvK/%s.csv", output);
    }

    private static String transformOutFile(String dataset, Date date, double propn){
        String output = String.format("%s_%s_reuse%.2f",minute.format(date),dataset, propn);
        return String.format("/lfs/raiders6/0/sahaana/data/MIC_DROP/reuse/"+day.format(date)+"/%s.csv", output);
    }

    //java ${JAVA_OPTS} -cp "assembly/target/*:core/target/classes:frontend/target/classes:contrib/target/classes" macrobase.analysis.stats.optimizer.experiments.SVDDropExperiments
    public static void main(String[] args) throws Exception{
        Date date = new Date();
        int numTrials = 200;
        long tempRuntime;
        int tempK;

        String dataset = args[0];
        double lbr = Double.parseDouble(args[1]);
        double qThresh = Double.parseDouble(args[2]);
        int kExp = Integer.parseInt(args[3]);
        PCASkiingOptimizer.optimize opt =   PCASkiingOptimizer.optimize.valueOf(args[4]);
        System.out.println(dataset);
        System.out.println(lbr);
        System.out.println(qThresh);
        System.out.println(kExp);
        System.out.println(opt);


        PCASkiingOptimizer.PCAAlgo[] algos = {PCASkiingOptimizer.PCAAlgo.TROPP};
        PCASkiingOptimizer.work reuse = PCASkiingOptimizer.work.REUSE;
        //double[] works = new double[] {0.0, 0.005, 0.01, 0.025, 0.05, 0.10, 0.15, 0.20, 0.25, 0.50,0.60,0.70,0.80,0.90,1};
        double[] works = new double[] {0.0, .05, .1, .15, .2, .25, .3, .35, .4, .45, .5, .55, .6};

        Map<Double, Long> runtimes;
        Map<Double, Integer> finalKs;

        MacroBaseConf conf = new MacroBaseConf();

        List<Datum> data = getData(dataset);

        for (PCASkiingOptimizer.PCAAlgo algo: algos) {
            runtimes = new HashMap<>();
            finalKs = new HashMap<>();

            for (int i = 0; i < numTrials; i++) {
                for (double pReuse: works){
                    PCASkiingDROP drop = new PCASkiingDROP(conf, qThresh, lbr, kExp, algo, reuse, opt, pReuse);
                    drop.consume(data);

                    tempRuntime = runtimes.getOrDefault(pReuse, (long) 0);
                    tempK = finalKs.getOrDefault(pReuse, 0);

                    runtimes.put(pReuse, tempRuntime + drop.totalTime());
                    finalKs.put(pReuse, tempK + drop.finalK());

                    if (i == numTrials-1){
                        double2dListToCSV(drop.getFinalTransform(), transformOutFile(dataset,date,pReuse));
                    }
                }
            }
            mapDoubleLongToCSV(scaleLongMapWCount(runtimes, numTrials), timeOutFile(dataset,lbr,qThresh,algo,date,opt));
            mapDoubleIntToCSV(scaleIntMapWCount(finalKs, numTrials), kOutFile(dataset,lbr,qThresh,algo,date,opt));
        }
    }
}
