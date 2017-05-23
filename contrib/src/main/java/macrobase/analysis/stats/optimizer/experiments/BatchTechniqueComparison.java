package macrobase.analysis.stats.optimizer.experiments;

import macrobase.analysis.stats.PAASkiingDROP;
import macrobase.analysis.stats.FFTSkiingDROP;
import macrobase.analysis.stats.PCASkiingDROP;
import macrobase.analysis.stats.RPSkiingDROP;
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
import java.util.List;
import java.util.Map;

/**
 * Created by meep_me on 9/1/16.
 */
public class BatchTechniqueComparison extends Experiment {
    public static String baseString = "contrib/src/main/java/macrobase/analysis/stats/optimizer/experiments/baselineExperiments/";
    public static DateFormat day = new SimpleDateFormat("MM-dd");
    public static DateFormat minute = new SimpleDateFormat("HH_mm");

    private static String LBROutFile(String dataset, double qThresh, String tag, Date date){
        String output = String.format("%s_%s_q%.3f_%s",minute.format(date),dataset,qThresh, tag);
        return String.format(baseString + day.format(date) + "/KvLBR/%s.csv", output);
    }

    private static String timeOutFile(String dataset, double qThresh, String tag, Date date){
        String output = String.format("%s_%s_q%.3f_%s", minute.format(date), dataset,qThresh, tag);
        return String.format(baseString + day.format(date) + "/KvTime/%s.csv", output);
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

        Map<String,Map<Integer, Double>> fftResults;
        Map<String,Map<Integer, Double>> paaResults;
        Map<String,Map<Integer, Double>> rpResults;
        Map<String,Map<Integer, Double>> pcaResults;
        Map<String,Map<Integer, Double>> pcaTroppResults;
        Map<String,Map<Integer, Double>> pcaFastResults;


        MacroBaseConf conf = new MacroBaseConf();

        List<Datum> data = getData(dataset);

        PAASkiingDROP paaDrop = new PAASkiingDROP(conf, qThresh, lbr);
        FFTSkiingDROP fftDrop = new FFTSkiingDROP(conf, qThresh, lbr);
        RPSkiingDROP rpSkiingDROP = new RPSkiingDROP(conf, qThresh, lbr);
        PCASkiingDROP pcaDrop = new PCASkiingDROP(conf, qThresh, lbr, PCASkiingOptimizer.PCAAlgo.SVD);
        PCASkiingDROP pcaTroppDrop = new PCASkiingDROP(conf, qThresh, lbr, PCASkiingOptimizer.PCAAlgo.TROPP);
        PCASkiingDROP pcaFastDrop = new PCASkiingDROP(conf, qThresh, lbr, PCASkiingOptimizer.PCAAlgo.FAST);



        paaResults = paaDrop.genBasePlots(data);
        fftResults = fftDrop.genBasePlots(data);
        pcaResults = pcaDrop.genBasePlots(data);
        pcaTroppResults = pcaTroppDrop.genBasePlots(data);
        pcaFastResults = pcaFastDrop.genBasePlots(data);
        rpResults = rpSkiingDROP.genBasePlots(data);

        mapDoubleToCSV(paaResults.get("LBR"), LBROutFile(dataset,qThresh,"PAA", date));
        mapDoubleToCSV(fftResults.get("LBR"), LBROutFile(dataset,qThresh, "FFT", date));
        mapDoubleToCSV(rpResults.get("LBR"),  LBROutFile(dataset,qThresh, "RP", date));
        mapDoubleToCSV(pcaResults.get("LBR"), LBROutFile(dataset,qThresh, "PCASVD", date));
        mapDoubleToCSV(pcaTroppResults.get("LBR"), LBROutFile(dataset,qThresh, "PCATROPP", date));
        mapDoubleToCSV(pcaFastResults.get("LBR"), LBROutFile(dataset,qThresh, "PCAFAST", date));


        mapDoubleToCSV(paaResults.get("time"), timeOutFile(dataset,qThresh,"PAA", date));
        mapDoubleToCSV(fftResults.get("time"), timeOutFile(dataset,qThresh, "FFT", date));
        mapDoubleToCSV(rpResults.get("time"),  timeOutFile(dataset,qThresh, "RP", date));
        mapDoubleToCSV(pcaResults.get("time"), timeOutFile(dataset,qThresh, "PCASVD", date));
        mapDoubleToCSV(pcaTroppResults.get("time"), timeOutFile(dataset,qThresh, "PCATROPP", date));
        mapDoubleToCSV(pcaFastResults.get("time"), timeOutFile(dataset,qThresh, "PCAFAST", date));
        

    }

}
