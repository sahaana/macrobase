package macrobase.analysis.stats.optimizer.experiments;

//import macrobase.analysis.stats.DROP;
import macrobase.analysis.stats.PAASkiingDROP;
import macrobase.analysis.stats.FFTSkiingDROP;
import macrobase.analysis.stats.PCASkiingDROP;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.SchemalessCSVIngester;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by meep_me on 9/1/16.
 */
public class BatchTechniqueComparison {
    private static Map<Integer, String> TABLE_NAMES = Stream.of(
            "1,CinC",
            "2,InlineSkate",
            "3,HandOutlines",
            "4,Haptics",
            "5,MALLAT",
            "6,StarLightCurves",
            "7,Phoneme",
            "8,ElectricDevices",
            "9,CinC-PAA"
    ).collect(Collectors.toMap(k -> Integer.parseInt(k.split(",")[0]), v -> v.split(",")[1]));

    private static Map<Integer, Integer> TABLE_SIZE = Stream.of(
            "1,1500",
            "2,1500",
            "3,2500",
            "4,1000",
            "5,1000",
            "6,1000",
            "7,1000",
            "8,50",
            "9,25"
    ).collect(Collectors.toMap(k -> Integer.parseInt(k.split(",")[0]), v -> Integer.parseInt(v.split(",")[1])));

    private static ArrayList<String> getMetrics(int datasetID){
        ArrayList<String> metrics = new ArrayList<>();
        for (int i = 0; i < TABLE_SIZE.get(datasetID); i++){
            metrics.add(Integer.toString(i));
        }
        return metrics;
    }

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


    private static String LBROutFile(String dataset, int b, int s, double lbr, double ep, String tag){
        String output = String.format("%s_b%d_s%d_lbr%.3f_ep%.3f_%s",dataset,b, s, lbr, ep, tag);
        return String.format("contrib/src/main/java/macrobase/analysis/stats/optimizer/experiments/batch/baseline/%s.csv", output);
    }

    private static String timeOutFile(String dataset, int b, int s, double lbr, double ep){
        String output = String.format("%s_b%d_s%d_lbr%.3f_ep%.3f",dataset,b, s, lbr,ep);
        return String.format("contrib/src/main/java/macrobase/analysis/stats/optimizer/experiments/batch/skiing/time/%s.csv", output);
    }




    //java ${JAVA_OPTS} -cp "assembly/target/*:core/target/classes:frontend/target/classes:contrib/target/classes" macrobase.analysis.stats.optimizer.experiments.SkiingBatchDROP
    public static void main(String[] args) throws Exception{
        //int k = 20;
        int maxNt = 25;
        //int datasetID = 7;

        String dataset = args[0];
        double lbr = Double.parseDouble(args[1]);
        double epsilon = Double.parseDouble(args[2]);
        System.out.println(dataset);
        System.out.println(lbr);
        System.out.println(epsilon);
        //*/
        int b = 50; //[25,50,100,200,300,400,500]
        int s = 20; //[5,10,20,25,35,50,75,100,200]


        //int processedDim = TABLE_SIZE.get(datasetID);
        /*String dataset = "CinC";
        double lbr = .98;
        double epsilon = .2;
        */


        Map<Integer, Double> fftResults;
        Map<Integer, Double> paaResults;
        Map<Integer, Double> rpResults;
        Map<Integer, Double> pcaResults;

        MacroBaseConf conf = new MacroBaseConf();

        SchemalessCSVIngester ingester = new SchemalessCSVIngester(String.format("/Users/meep_me/Desktop/Spring Rotation/workspace/OPTIMIZER/macrobase/contrib/src/test/resources/data/optimizer/raw/%s.csv", dataset));// new CSVIngester(conf);
        List<Datum> data = ingester.getStream().drain();

        PAASkiingDROP paaDrop = new PAASkiingDROP(conf, maxNt, epsilon, lbr, b, s);
        FFTSkiingDROP fftDrop = new FFTSkiingDROP(conf, maxNt, epsilon, lbr, b, s);
        PCASkiingDROP pcaDrop = new PCASkiingDROP(conf, maxNt, epsilon, lbr, b, s);
        ////DROPvPowerIteration drop = new DROPvPowerIteration(conf, maxNt, epsilon, lbr, b, s, rpFlag);


        paaResults = paaDrop.genBasePlots(data);
        fftResults = fftDrop.genBasePlots(data);
        pcaResults = pcaDrop.genBasePlots(data);


        mapDoubleToCSV(paaResults, LBROutFile(dataset,b,s,lbr,epsilon,"PAA"));
        mapDoubleToCSV(fftResults, LBROutFile(dataset,b,s,lbr,epsilon, "FFT"));
        mapDoubleToCSV(pcaResults, LBROutFile(dataset,b,s,lbr,epsilon, "PCASVD"));

    }

}
