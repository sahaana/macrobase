package macrobase.analysis.stats.optimizer.experiments;

//import macrobase.analysis.stats.DROP;
import macrobase.analysis.stats.PIPCASkiingDROP;
import macrobase.analysis.stats.SVDPCASkiingDROP;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.SchemalessCSVIngester;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by meep_me on 9/1/16.
 */
public class SkiingBatchDROP {

    private static void mapDoubleToCSV(Map<Integer, Double> dataMap, String file){
        String eol =  System.getProperty("line.separator");
        try (Writer writer = new FileWriter(file)) {
            for (Map.Entry<Integer, Double> entry: dataMap.entrySet()) {
                writer.append(Integer.toString(entry.getKey()))
                        .append(',')
                        .append(Double.toString(entry.getValue()))
                        .append(eol);
            }
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
        }
    }

    private static void mapIntToCSV(Map<Integer, Integer> dataMap, String file){
        String eol =  System.getProperty("line.separator");
        try (Writer writer = new FileWriter(file)) {
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


    private static void mapArrayToCSV(Map<Integer, double[]> dataMap, String file){
        String eol =  System.getProperty("line.separator");
        try (Writer writer = new FileWriter(file)) {
            for (Map.Entry<Integer, double[]> entry: dataMap.entrySet()) {
                writer.append(Integer.toString(entry.getKey()))
                        .append(',')
                        .append(Double.toString(entry.getValue()[0]))
                        .append(',')
                        .append(Double.toString(entry.getValue()[1]))
                        .append(',')
                        .append(Double.toString(entry.getValue()[2]))
                        .append(eol);
            }
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
        }
    }


    private static String LBROutFile(String dataset, double lbr, double ep){
        String output = String.format("%s_lbr%.4f_ep%.3f",dataset, lbr, ep);
        return String.format("contrib/src/main/java/macrobase/analysis/stats/optimizer/experiments/batch/skiing/Nt/%s.csv", output);
    }

    private static String timeOutFile(String dataset, double lbr, double ep, String tag){
        String output = String.format("%s_%s_lbr%.4f_ep%.3f",dataset, tag, lbr,ep);
        return String.format("contrib/src/main/java/macrobase/analysis/stats/optimizer/experiments/batch/skiing/time/%s.csv", output);
    }

    private static String kOutFile(String dataset, double lbr, double ep){
        String output = String.format("%s_lbr%.4f_ep%.3f",dataset,lbr,ep);
        return String.format("contrib/src/main/java/macrobase/analysis/stats/optimizer/experiments/batch/skiing/k/%s.csv", output);
    }

    private static String kPredOutFile(String dataset, double lbr, double ep){
        String output = String.format("%s_lbr%.4f_ep%.3f",dataset,lbr,ep);
        return String.format("contrib/src/main/java/macrobase/analysis/stats/optimizer/experiments/batch/skiing/kPred/%s.csv", output);
    }

    private static String kItersOutFile(String dataset, double lbr, double ep){
        String output = String.format("%s_lbr%.4f_ep%.3f",dataset, lbr,ep);
        return String.format("contrib/src/main/java/macrobase/analysis/stats/optimizer/experiments/batch/skiing/kIter/%s.csv", output);
    }

    //java ${JAVA_OPTS} -cp "assembly/target/*:core/target/classes:frontend/target/classes:contrib/target/classes" macrobase.analysis.stats.optimizer.experiments.SkiingBatchDROP
    public static void main(String[] args) throws Exception{

        String dataset = args[0];
        double lbr = Double.parseDouble(args[1]);
        double epsilon = Double.parseDouble(args[2]);
        System.out.println(dataset);
        System.out.println(lbr);
        System.out.println(epsilon);

        /*String dataset = "CinC";
        double lbr = .98;
        double epsilon = .2;
        */

        Map<Integer, double[]> LBRResults;
        Map<Integer, Double> timeResults;
        Map<Integer, Double> predTimeResults;
        Map<Integer, Integer> kResults;
        Map<Integer, Integer> kIters;
        Map<Integer, Integer> kPred;

        MacroBaseConf conf = new MacroBaseConf();

        SchemalessCSVIngester ingester = new SchemalessCSVIngester(String.format("/Users/meep_me/Desktop/Spring Rotation/workspace/OPTIMIZER/macrobase/contrib/src/test/resources/data/optimizer/raw/%s.csv", dataset));// new CSVIngester(conf);
        List<Datum> data = ingester.getStream().drain();

        SVDPCASkiingDROP drop = new SVDPCASkiingDROP(conf, epsilon, lbr);
        //PIPCASkiingDROP drop = new PIPCASkiingDROP(conf, epsilon, lbr);
        drop.consume(data);

        LBRResults = drop.getLBR();
        mapArrayToCSV(LBRResults, LBROutFile(dataset,lbr,epsilon));

        timeResults = drop.getTime();
        mapDoubleToCSV(timeResults, timeOutFile(dataset,lbr,epsilon,"Actual"));

        predTimeResults = drop.getPredTime();
        mapDoubleToCSV(predTimeResults, timeOutFile(dataset,lbr,epsilon, "Predicted"));

        kResults = drop.getKList();
        mapIntToCSV(kResults, kOutFile(dataset,lbr,epsilon));

        kPred = drop.getKPred();
        mapIntToCSV(kPred, kPredOutFile(dataset,lbr,epsilon));

        kIters = drop.getKItersList();
        //mapIntToCSV(kIters, kItersOutFile(dataset,b,s,lbr,epsilon));

    }

}
