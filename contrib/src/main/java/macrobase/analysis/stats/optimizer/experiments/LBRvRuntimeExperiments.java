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
public class LBRvRuntimeExperiments extends Experiment {
    public static String baseString = "contrib/src/main/java/macrobase/analysis/stats/optimizer/experiments/LBRvTimeExperiments/";
    public static DateFormat day = new SimpleDateFormat("MM-dd");
    public static DateFormat minute = new SimpleDateFormat("HH_mm");


    private static String timeOutFile(String dataset, double qThresh, int kExp, PCASkiingOptimizer.PCAAlgo algo, PCASkiingOptimizer.work reuse, Date date){
        String output = String.format("%s_%s_%s_q%.2f_kexp%d_%s",minute.format(date),dataset, algo, qThresh, kExp, reuse);
        return String.format(baseString + day.format(date) + "/%s.csv", output);
    }

    //java ${JAVA_OPTS} -cp "assembly/target/*:core/target/classes:frontend/target/classes:contrib/target/classes" macrobase.analysis.stats.optimizer.experiments.SVDDropExperiments
    public static void main(String[] args) throws Exception{
        Date date = new Date();
        int numTrials = 50;
        long tempRuntime;

        String dataset = args[0];
        double qThresh = Double.parseDouble(args[1]);
        int kExp = Integer.parseInt(args[2]);
        System.out.println(dataset);
        System.out.println(qThresh);
        System.out.println(kExp);

        double[] lbrs = {0.80,.85,0.9,0.95,0.98};
        PCASkiingOptimizer.PCAAlgo[] algos = {PCASkiingOptimizer.PCAAlgo.SVD, PCASkiingOptimizer.PCAAlgo.TROPP, PCASkiingOptimizer.PCAAlgo.FAST};
        PCASkiingOptimizer.work[] options = {PCASkiingOptimizer.work.NOREUSE, PCASkiingOptimizer.work.REUSE};
        Map<Double, Long> runtimes; //lbr > runtime for all configs

        MacroBaseConf conf = new MacroBaseConf();

        SchemalessCSVIngester ingester = new SchemalessCSVIngester(String.format("contrib/src/test/resources/data/optimizer/raw/%s.csv", dataset));
        List<Datum> data = ingester.getStream().drain();

        for (PCASkiingOptimizer.PCAAlgo algo: algos){
            for (PCASkiingOptimizer.work reuse: options){
                runtimes = new HashMap<>();
                for (double lbr: lbrs){
                    tempRuntime = 0;
                    for (int i = 0; i < numTrials; i++){
                        PCASkiingDROP drop = new PCASkiingDROP(conf, qThresh, lbr, kExp, algo, reuse);
                        drop.consume(data);
                        tempRuntime += drop.totalTime();
                    }
                    runtimes.put(lbr,tempRuntime/numTrials);
                }
                mapDoubleLongToCSV(runtimes, timeOutFile(dataset,qThresh,kExp,algo,reuse,date));
            }
        }
    }
}
