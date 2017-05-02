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
import java.util.*;

/**
 * Created by meep_me on 9/1/16.
 */
public class SVDDropExperiments {
    public static String baseString = "contrib/src/main/java/macrobase/analysis/stats/optimizer/experiments/SVDExperiments/";
    public static DateFormat day = new SimpleDateFormat("MM-dd");
    public static DateFormat minute = new SimpleDateFormat("HH_mm");

    private static void mapIntToCSV(Map<Integer, Integer> dataMap, String file){
        File f = new File(file);
        f.getParentFile().mkdirs();
        String eol =  System.getProperty("line.separator");
        try (Writer writer = new FileWriter(f)) {
            for (Map.Entry<Integer, Integer> entry: dataMap.entrySet()) {
                writer.append(Integer.toString(entry.getKey()))
                        .append(',')
                        .append(Integer.toString(entry.getValue()))
                        .append(eol);
            }
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
        }
    }

    private static void mapArrayToCSV2(Map<Integer, double[]> dataMap, String file){
        File f = new File(file);
        f.getParentFile().mkdirs();
        String eol =  System.getProperty("line.separator");
        try (Writer writer = new FileWriter(f)) {
            for (Map.Entry<Integer, double[]> entry: dataMap.entrySet()) {
                writer.append(Integer.toString(entry.getKey()))
                        .append(',')
                        .append(Double.toString(entry.getValue()[0]))
                        .append(',')
                        .append(Double.toString(entry.getValue()[1]))
                        .append(eol);
            }
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
        }
    }


    private static String kOutFile(String dataset, double lbr, double qThresh, int kExp, PCASkiingOptimizer.work reuse, Date date){
        String output = String.format("%s_%s_lbr%.2f_q%.2f_kexp%d_%s",minute.format(date),dataset,lbr,qThresh,kExp,reuse);
        return String.format(baseString + day.format(date) + "/NTvK/%s.csv", output);
    }

    private static String timeEstimateOutFile(String dataset, double lbr, double qThresh, int kExp, PCASkiingOptimizer.work reuse, Date date){
        String output = String.format("%s_%s_lbr%.2f_q%.2f_kexp%d_%s",minute.format(date),dataset,lbr,qThresh,kExp,reuse);
        return String.format(baseString + day.format(date) + "/MDtimeEst/%s.csv", output);
    }


    //java ${JAVA_OPTS} -cp "assembly/target/*:core/target/classes:frontend/target/classes:contrib/target/classes" macrobase.analysis.stats.optimizer.experiments.SVDDropExperiments
    public static void main(String[] args) throws Exception{
        Date date = new Date();


        String dataset = args[0];
        double lbr = Double.parseDouble(args[1]);
        double qThresh = Double.parseDouble(args[2]);
        int kExp = Integer.parseInt(args[3]);
        PCASkiingOptimizer.PCAAlgo algo = PCASkiingOptimizer.PCAAlgo.valueOf(args[4]);
        PCASkiingOptimizer.work reuse = PCASkiingOptimizer.work.valueOf(args[5]);
        System.out.println(dataset);
        System.out.println(lbr);
        System.out.println(qThresh);
        System.out.println(kExp);
        System.out.println(algo);
        System.out.println(reuse);


        Map<Integer, Integer> kResults;
        Map<Integer, double[]> MDTimeResults;


        MacroBaseConf conf = new MacroBaseConf();

        SchemalessCSVIngester ingester = new SchemalessCSVIngester(String.format("contrib/src/test/resources/data/optimizer/raw/%s.csv", dataset));
        List<Datum> data = ingester.getStream().drain();

        PCASkiingDROP drop = new PCASkiingDROP(conf, qThresh, lbr, kExp, algo, reuse);
        drop.consume(data);

        kResults = drop.getKList();
        mapIntToCSV(kResults, kOutFile(dataset,lbr,qThresh,kExp,reuse,date));

        MDTimeResults = drop.getMDRuntimes();
        mapArrayToCSV2(MDTimeResults, timeEstimateOutFile(dataset,lbr,qThresh,kExp,reuse,date));
    }

}
