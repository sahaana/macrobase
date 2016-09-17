#!/usr/bin/env python
import subprocess

datasets = ['50words', 'Adiac', 'ArrowHead', 'Car', 'CBF', 'ChlorineConcentration', 'CinC', 'Computers', 'Cricket', 'Cricket', 'Cricket', 'DiatomSizeReduction', 'DistalPhalanxOutlineAgeGroup', 'DistalPhalanxOutlineCorrect', 'DistalPhalanxTW', 'Earthquakes', 'ECG200', 'ECG5000', 'ECGFiveDays', 'ElectricDevices', 'FaceAll', 'FaceFour', 'FacesUCR', 'FISH', 'FordA', 'FordB', 'Gun', 'Ham', 'HandOutlines', 'Haptics', 'Herring', 'InlineSkate', 'InsectWingbeatSound', 'ItalyPowerDemand', 'LargeKitchenAppliances', 'Lighting2', 'Lighting7', 'MALLAT', 'Meat', 'MedicalImages', 'MiddlePhalanxOutlineAgeGroup', 'MiddlePhalanxOutlineCorrect', 'MiddlePhalanxTW', 'MoteStrain', 'NonInvasiveFatalECG', 'NonInvasiveFatalECG', 'OSULeaf', 'PhalangesOutlinesCorrect', 'Phoneme', 'Plane', 'ProximalPhalanxOutlineAgeGroup', 'ProximalPhalanxOutlineCorrect', 'ProximalPhalanxTW', 'RefrigerationDevices', 'ScreenType', 'ShapeletSim', 'ShapesAll', 'SmallKitchenAppliances', 'SonyAIBORobotSurface', 'SonyAIBORobotSurfaceII', 'StarLightCurves', 'Strawberry', 'SwedishLeaf', 'Symbols', 'synthetic', 'ToeSegmentation1', 'ToeSegmentation2', 'Trace', 'TwoLeadECG', 'Two', 'UWaveGestureLibraryAll', 'uWaveGestureLibrary', 'uWaveGestureLibrary', 'uWaveGestureLibrary', 'wafer', 'Wine', 'WordsSynonyms', 'Worms', 'WormsTwoClass', 'yoga']

lbr = 0.98
ep = 0.2

proc = "java ${JAVA_OPTS} -cp 'assembly/target/*:core/target/classes:frontend/target/classes:contrib/target/classes' macrobase.analysis.stats.optimizer.experiments.SkiingBatchDROP %s %f %f %s"
package = "mvn package -DskipTests"
subprocess.call(package, shell=True)

for dataset in datasets:
    subprocess.call(proc % (dataset, lbr, ep, 'true'), shell=True)
    subprocess.call(proc % (dataset, lbr, ep, 'false'), shell=True)
