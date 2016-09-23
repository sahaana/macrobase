package macrobase.analysis.stats.optimizer.experiments;

import macrobase.analysis.stats.DROP;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.CSVIngester;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BatchDROP {
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


    private static String LBROutFile(int datasetID, int b, int s, int k, int num_Nt, int procD, double lbr, double ep){
        String output = String.format("%s_blbr_b%d_s%d_k%d_procDim%d_lbr%.3f_ep%.3f",TABLE_NAMES.get(datasetID),b, s, k,procD,lbr,ep);
        return String.format("contrib/src/main/java/macrobase/analysis/stats/optimizer/experiments/batch/BLB/Nt/%s.csv", output);
    }

    private static String timeOutFile(int datasetID, int b, int s, int k, int num_Nt, int procD, double lbr, double ep){
        String output = String.format("%s_blbr_b%d_s%d_k%d_procDim%d_lbr%.3f_ep%.3f",TABLE_NAMES.get(datasetID),b, s, k,procD,lbr,ep);
        return String.format("contrib/src/main/java/macrobase/analysis/stats/optimizer/experiments/batch/BLB/time/%s.csv", output);
    }

    public static void main(String[] args) throws Exception{
        int k = 20;
        int num_Nt = 20;
        int processedDim = 1500;
        int datasetID = 1;
        int b = 300; //[25,50,100,200,300,400,500]
        int s = 25 ; //[5,10,20,25,35,50,75,100,200]
        double lbr = .99;
        double epsilon = .2;


        Map<Integer, double[]> LBRResults;
        Map<Integer, Double> timeResults;

        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.CSV_INPUT_FILE, String.format("/Users/meep_me/Desktop/Spring Rotation/workspace/OPTIMIZER/macrobase/contrib/src/test/resources/data/optimizer/%s.csv", TABLE_NAMES.get(datasetID)));
        conf.set(MacroBaseConf.ATTRIBUTES, new ArrayList<>());
        conf.set(MacroBaseConf.METRICS, getMetrics(datasetID));

        CSVIngester ingester = new CSVIngester(conf);
        List<Datum> data = ingester.getStream().drain();

        DROP drop = new DROP(new MacroBaseConf(), k, num_Nt, processedDim, epsilon, lbr, b, s);
        drop.consume(data);

        LBRResults = drop.getBLBR();
        mapArrayToCSV(LBRResults, LBROutFile(datasetID,b,s,k,num_Nt,processedDim,lbr,epsilon));

        timeResults = drop.getTime();
        mapDoubleToCSV(timeResults, timeOutFile(datasetID,b,s,k,num_Nt,processedDim,lbr,epsilon));


    }


}