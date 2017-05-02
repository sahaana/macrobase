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
import java.util.List;

/**
 * Created by meep_me on 9/1/16.
 */
public class SpectrumDump {
    public static String baseString = "contrib/src/main/java/macrobase/analysis/stats/optimizer/experiments/baseline/spectrum";

    private static void doubleToCSV(double[] vals, String file){
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

    private static String spectrumOutFile(String dataset){
        return String.format(baseString + "/%s_spec.csv", dataset);
    }


    //java ${JAVA_OPTS} -cp "assembly/target/*:core/target/classes:frontend/target/classes:contrib/target/classes" macrobase.analysis.stats.optimizer.experiments.SVDDropExperiments
    public static void main(String[] args) throws Exception{
        String dataset = args[0];
        double lbr = Double.parseDouble(args[1]);
        double qThresh = Double.parseDouble(args[2]);
        System.out.println(dataset);

        double[] spectrum;

        MacroBaseConf conf = new MacroBaseConf();

        SchemalessCSVIngester ingester = new SchemalessCSVIngester(String.format("contrib/src/test/resources/data/optimizer/raw/%s.csv", dataset));
        List<Datum> data = ingester.getStream().drain();

        PCASkiingDROP pcaDrop = new PCASkiingDROP(conf, qThresh, lbr, PCASkiingOptimizer.PCAAlgo.SVD);
        spectrum = pcaDrop.getDataSpectrum(data);

        doubleToCSV(spectrum, spectrumOutFile(dataset));


    }

}
