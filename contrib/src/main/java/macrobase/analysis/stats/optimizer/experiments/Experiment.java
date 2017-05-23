package macrobase.analysis.stats.optimizer.experiments;

import macrobase.datamodel.Datum;
import macrobase.ingest.SchemalessCSVIngester;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by meep_me on 9/1/16.
 */
public abstract class Experiment {

    static void mapIntToCSV(Map<Integer, Integer> dataMap, String file){
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

    static void mapDoubleToCSV(Map<Integer, Double> dataMap, String file){
        File f = new File(file);
        f.getParentFile().mkdirs();
        String eol =  System.getProperty("line.separator");
        try (Writer writer = new FileWriter(f)) {
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

    static void doubleListToCSV(double[] vals, String file){
        File f = new File(file);
        f.getParentFile().mkdirs();
        String eol =  System.getProperty("line.separator");
        try (Writer writer = new FileWriter(f)) {
            for (double val: vals) {
                writer.append(Double.toString(val))
                        .append(eol);
            }
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
        }
    }

    static void mapDoubleLongToCSV(Map<Double, Long> dataMap, String file){
        File f = new File(file);
        f.getParentFile().mkdirs();
        String eol =  System.getProperty("line.separator");
        try (Writer writer = new FileWriter(f)) {
            for (Map.Entry<Double, Long> entry: dataMap.entrySet()) {
                writer.append(Double.toString(entry.getKey()))
                        .append(',')
                        .append(Long.toString(entry.getValue()))
                        .append(eol);
            }
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
        }
    }

    static void mapIntLongToCSV(Map<Integer, Long> dataMap, String file){
        File f = new File(file);
        f.getParentFile().mkdirs();
        String eol =  System.getProperty("line.separator");
        try (Writer writer = new FileWriter(f)) {
            for (Map.Entry<Integer, Long> entry: dataMap.entrySet()) {
                writer.append(Integer.toString(entry.getKey()))
                        .append(',')
                        .append(Long.toString(entry.getValue()))
                        .append(eol);
            }
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
        }
    }

    static void mapDouble2ToCSV(Map<Integer, double[]> dataMap, String file){
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


    static Map<Integer, Integer> scaleIntMap(Map<Integer, Integer> toScale, Map<Integer,Double> counts){
        Map<Integer, Integer> scaled = new HashMap<>();
        for (Map.Entry<Integer, Integer> entry: toScale.entrySet()) {
            int key = entry.getKey();
            int kval = entry.getValue();
            Double newK = new Double(kval)/counts.get(key);
            scaled.put(key, newK.intValue());
        }
        return scaled;
    }

    static Map<Integer, Double> scaleDoubleMap(Map<Integer, Double> toScale, Map<Integer,Double> counts){
        Map<Integer, Double> scaled = new HashMap<>();
        for (Map.Entry<Integer, Double> entry: toScale.entrySet()) {
            int key = entry.getKey();
            double kval = entry.getValue();
            Double newK = new Double(kval)/counts.get(key);
            scaled.put(key, newK);
        }
        return scaled;
    }

    static Map<Integer, double[]> scaleDouble2Map(Map<Integer, double[]> toScale, Map<Integer,Double> counts){
        Map<Integer, double[]> scaled = new HashMap<>();
        for (Map.Entry<Integer, double[]> entry: toScale.entrySet()) {
            int key = entry.getKey();
            double real = entry.getValue()[0];
            double guess = entry.getValue()[1];
            scaled.put(key, new double[] {real/counts.get(key), guess/counts.get(key)});
        }
        return scaled;
    }

    static List<Datum> getData(String dataset) throws Exception{
        SchemalessCSVIngester ingester = new SchemalessCSVIngester(String.format("optimizerData/raw/%s.csv", dataset));
        return ingester.getStream().drain();
    }

}
