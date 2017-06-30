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
public class FullDROPExperiments extends Experiment {
    public static String baseString = "contrib/src/main/java/macrobase/analysis/stats/optimizer/experiments/DROPExperiments/";
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

    private static String pKOutFile(String dataset, double lbr, double qThresh, PCASkiingOptimizer.PCAAlgo algo, PCASkiingOptimizer.work reuse, Date date, PCASkiingOptimizer.optimize opt){
        String output = String.format("%s_%s_%s_lbr%.2f_q%.2f_%s_%s_pred",minute.format(date),dataset, algo, lbr, qThresh, reuse, opt);
        return String.format(baseString + day.format(date) + "/NTvK/%s.csv", output);
    }

    private static String rKOutFile(String dataset, double lbr, double qThresh, PCASkiingOptimizer.PCAAlgo algo, PCASkiingOptimizer.work reuse, Date date, PCASkiingOptimizer.optimize opt){
        String output = String.format("%s_%s_%s_lbr%.2f_q%.2f_%s_%s",minute.format(date),dataset, algo, lbr, qThresh, reuse, opt);
        return String.format(baseString + day.format(date) + "/NTvK/%s.csv", output);
    }

    private static String pTrainOutFile(String dataset, double lbr, double qThresh, PCASkiingOptimizer.PCAAlgo algo, PCASkiingOptimizer.work reuse, Date date, PCASkiingOptimizer.optimize opt){
        String output = String.format("%s_%s_%s_lbr%.2f_q%.2f_%s_%s_pred",minute.format(date),dataset, algo, lbr, qThresh, reuse, opt);
        return String.format(baseString + day.format(date) + "/NTvTime/%s.csv", output);
    }

    private static String trainOutFile(String dataset, double lbr, double qThresh, PCASkiingOptimizer.PCAAlgo algo, PCASkiingOptimizer.work reuse, Date date, PCASkiingOptimizer.optimize opt){
        String output = String.format("%s_%s_%s_lbr%.2f_q%.2f_%s_%s",minute.format(date),dataset, algo, lbr, qThresh, reuse, opt);
        return String.format(baseString + day.format(date) + "/NTvTime/%s.csv", output);
    }

    private static String pObjOutFile(String dataset, double lbr, double qThresh, PCASkiingOptimizer.PCAAlgo algo, PCASkiingOptimizer.work reuse, Date date, PCASkiingOptimizer.optimize opt){
        String output = String.format("%s_%s_%s_lbr%.2f_q%.2f_%s_%s_pred",minute.format(date),dataset, algo, lbr, qThresh, reuse, opt);
        return String.format(baseString + day.format(date) + "/NTvObj/%s.csv", output);
    }

    private static String rObjOutFile(String dataset, double lbr, double qThresh, PCASkiingOptimizer.PCAAlgo algo, PCASkiingOptimizer.work reuse, Date date, PCASkiingOptimizer.optimize opt){
        String output = String.format("%s_%s_%s_lbr%.2f_q%.2f_%s_%s",minute.format(date),dataset, algo, lbr, qThresh, reuse, opt);
        return String.format(baseString + day.format(date) + "/NTvObj/%s.csv", output);
    }

    private static String examinedOutFile(String dataset, double lbr, double qThresh, PCASkiingOptimizer.PCAAlgo algo, PCASkiingOptimizer.work reuse, Date date, PCASkiingOptimizer.optimize opt){
        String output = String.format("%s_%s_%s_lbr%.2f_q%.2f_%s_%s",minute.format(date),dataset, algo, lbr, qThresh, reuse, opt);
        return String.format(baseString + day.format(date) + "/NT/%s.csv", output);
    }

    private static String LBROutFile(String dataset, double lbr, double qThresh, PCASkiingOptimizer.PCAAlgo algo, PCASkiingOptimizer.work reuse, Date date, PCASkiingOptimizer.optimize opt){
        String output = String.format("%s_%s_%s_lbr%.2f_q%.2f_%s_%s",minute.format(date),dataset, algo, lbr, qThresh, reuse, opt);
        return String.format(baseString + day.format(date) + "/LBR/%s.csv", output);
    }

    //java ${JAVA_OPTS} -cp "assembly/target/*:core/target/classes:frontend/target/classes:contrib/target/classes" macrobase.analysis.stats.optimizer.experiments.SVDDropExperiments
    public static void main(String[] args) throws Exception{
        Date date = new Date();
        long tempRuntime;
        int tempK;

        String dataset = args[0];
        double lbr = Double.parseDouble(args[1]);
        double qThresh = Double.parseDouble(args[2]);
        int kExp = 2; //Not used for anything because it's KNN objective function based
        PCASkiingOptimizer.work reuse = PCASkiingOptimizer.work.valueOf(args[3]);
        PCASkiingOptimizer.optimize opt =   PCASkiingOptimizer.optimize.valueOf(args[4]);
        int numTrials = Integer.parseInt(args[5]);
        System.out.println(dataset);
        System.out.println(lbr);
        System.out.println(qThresh);
        System.out.println(kExp);
        System.out.println(reuse);
        System.out.println(opt);
        System.out.println(numTrials);


        PCASkiingOptimizer.PCAAlgo[] algos = {PCASkiingOptimizer.PCAAlgo.TROPP, PCASkiingOptimizer.PCAAlgo.FAST};

        Map<Integer, Long> runtimes;
        Map<Integer, Integer> finalKs;

        Map<Integer, Integer> kReals;
        Map<Integer, Double> krcounts;
        Map<Integer, Integer> kPreds;
        Map<Integer, Double> kcounts;
        Map<Integer, Double> trainTimes;
        Map<Integer, Double> tcounts;
        Map<Integer, Double> predTrainTimes;
        Map<Integer, Double> pcounts;
        Map<Integer, Double> pObj;
        Map<Integer, Double> pocounts;
        Map<Integer, Double> tObj;
        Map<Integer, Double> tocounts;
        Map<Integer, Integer> dataExamined;
        Map<Integer, Double> LBRCounts;
        Map<Integer, double[]> LBRs;

        MacroBaseConf conf = new MacroBaseConf();

        List<Datum> data;

        for (PCASkiingOptimizer.PCAAlgo algo: algos) {
            runtimes = new HashMap<>();
            finalKs = new HashMap<>();
            tempK = 0;
            tempRuntime = 0;

            kReals = new HashMap<>();
            krcounts = new HashMap<>();
            kPreds = new HashMap<>();
            kcounts = new HashMap<>();
            trainTimes = new HashMap<>();
            tcounts = new HashMap();
            predTrainTimes = new HashMap<>();
            pcounts = new HashMap();
            pObj = new HashMap<>();
            pocounts = new HashMap<>();
            tObj = new HashMap<>();
            tocounts = new HashMap<>();
            dataExamined = new HashMap<>();
            LBRs = new HashMap<>();
            LBRCounts = new HashMap<>();

            data = getData(dataset);

            for (int i = 0; i < numTrials; i++) {
                PCASkiingDROP drop = new PCASkiingDROP(conf, qThresh, lbr, kExp, algo, reuse, opt);
                drop.consume(data);

                //update k and total time
                tempK += drop.finalK();
                tempRuntime += drop.totalTime();
                int Nt = drop.getNt();

                //update LBR
                for (Map.Entry<Integer, double[]> entry : drop.getLBR().entrySet()) {
                    int key = entry.getKey();
                    double[] val = entry.getValue();
                    double[] prev = LBRs.getOrDefault(key, new double[]{0, 0, 0});

                    LBRCounts.put(key, 1 + LBRCounts.getOrDefault(key, 0.0));

                    for (int ii = 0 ; ii < 3; i++) {
                        val[ii] += prev[ii];
                    }
                    LBRs.put(key, val);
                }

                //update histogram of how much data was examined
                dataExamined.put(Nt, 1 + dataExamined.getOrDefault(Nt,0));

                //update real k
                for (Map.Entry<Integer, Integer> entry: drop.getKList().entrySet()){
                    int key = entry.getKey();
                    int val = entry.getValue();

                    krcounts.put(key, 1 + krcounts.getOrDefault(key, 0.0));
                    kReals.put(key, val + kReals.getOrDefault(key, 0));
                }

                //update predicted k
                for (Map.Entry<Integer, Integer> entry: drop.getKPred().entrySet()){
                    int key = entry.getKey();
                    int val = entry.getValue();

                    kcounts.put(key, 1 + kcounts.getOrDefault(key, 0.0));
                    kPreds.put(key, val + kPreds.getOrDefault(key, 0));
                }

                //update predicted times
                for (Map.Entry<Integer, Double> entry: drop.getPredTrainTimes().entrySet()) {
                    int key = entry.getKey();
                    double val = entry.getValue();

                    pcounts.put(key, 1 + pcounts.getOrDefault(key,0.0));
                    predTrainTimes.put(key, val + predTrainTimes.getOrDefault(key,0.0));
                }

                //update true train times
                for (Map.Entry<Integer, Double> entry: drop.getTrainTimes().entrySet()) {
                    int key = entry.getKey();
                    double val = entry.getValue();

                    tcounts.put(key, 1 + tcounts.getOrDefault(key,0.0));
                    trainTimes.put(key, val + trainTimes.getOrDefault(key,0.0));
                }

                //update predicted objective
                for (Map.Entry<Integer, Double> entry: drop.getPredictedObjective().entrySet()) {
                    int key = entry.getKey();
                    double val = entry.getValue();

                    pocounts.put(key, 1 + pocounts.getOrDefault(key,0.0));
                    pObj.put(key, val + pObj.getOrDefault(key,0.0));
                }

                //update true objective
                for (Map.Entry<Integer, Double> entry: drop.getTrueObjective().entrySet()) {
                    int key = entry.getKey();
                    double val = entry.getValue();

                    tocounts.put(key, 1 + tocounts.getOrDefault(key,0.0));
                    tObj.put(key, val + tObj.getOrDefault(key,0.0));
                }

            }
            mapDoubleToCSV(scaleDoubleMap(trainTimes,tcounts), trainOutFile(dataset,lbr,qThresh,algo,reuse,date,opt));
            mapDoubleToCSV(scaleDoubleMap(predTrainTimes, pcounts), pTrainOutFile(dataset,lbr,qThresh,algo,reuse,date,opt));
            mapIntToCSV(scaleIntMap(kReals, krcounts), rKOutFile(dataset,lbr,qThresh,algo,reuse,date,opt));
            mapIntToCSV(scaleIntMap(kPreds, kcounts), pKOutFile(dataset,lbr,qThresh,algo,reuse,date,opt));
            mapDoubleToCSV(scaleDoubleMap(tObj,tocounts), rObjOutFile(dataset,lbr,qThresh,algo,reuse,date,opt));
            mapDoubleToCSV(scaleDoubleMap(pObj, pocounts), pObjOutFile(dataset,lbr,qThresh,algo,reuse,date,opt));

            mapIntToCSV(dataExamined, examinedOutFile(dataset,lbr,qThresh,algo,reuse,date,opt));

            mapDouble3ToCSV(scaleDouble3Map(LBRs, LBRCounts),LBROutFile(dataset,lbr,qThresh,algo,reuse,date,opt));

            runtimes.put(data.size(), tempRuntime / numTrials);
            finalKs.put(data.size(), tempK / numTrials);
            mapIntLongToCSV(runtimes, timeOutFile(dataset,lbr,qThresh,algo,reuse,date,opt));
            mapIntToCSV(finalKs, kOutFile(dataset,lbr,qThresh,algo,reuse,date,opt));
        }

    }
}
