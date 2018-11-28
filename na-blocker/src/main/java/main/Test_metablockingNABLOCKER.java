package main;
/**
 * @author @stravanni
 */


import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import BlockBuilding.AbstractBlockingMethod;
import BlockBuilding.MemoryBased.TokenBlocking;
import BlockProcessing.BlockRefinement.BlockFiltering;
import BlockProcessing.BlockRefinement.ComparisonsBasedBlockPurging;
import BlockProcessing.ComparisonRefinement.AbstractDuplicatePropagation;
import DataStructures.AbstractBlock;
import DataStructures.Comparison;
import DataStructures.EntityProfile;
import DataStructures.IdDuplicates;
import MetaBlocking.ThresholdWeightingScheme;
import MetaBlocking.WeightingScheme;
import OnTheFlyMethods.FastImplementations.BlastWeightedNodePruning;
import Utilities.RepresentationModel;

/**
 * @author stravanni
 */

public class Test_metablockingNABLOCKER {

    private static boolean CLEAN = true;
    private static String BASEPATH_CER = "C:/Users/Brasileiro/Bases de Dados/Clean-Clean/Movies/"; ///Users/gio/Desktop/umich/data/data_blockingFramework/";
    private static String BASEPATH_DER = "/Users/gio/Desktop/umich/data/data_blockingFramework/";

    public static void main(String[] args) throws IOException {

        int dataset = 0;
        boolean save = false;
        String blocking_type = "M";
        WeightingScheme ws = WeightingScheme.CHI_ENTRO;
        //WeightingScheme ws = WeightingScheme.FISHER_ENTRO; // For dirty dataset use this test-statistic because of the low number of co-occurrence in the blocks (Fisher exact test vs. Chi-squared ~ approximated)
        ThresholdWeightingScheme th_schme = ThresholdWeightingScheme.AM3;

        List<EntityProfile>[] profiles;
        if (args.length > 0) {
            BASEPATH_CER = args[0] + "/";
            BASEPATH_DER = args[0] + "/";
            save = true;
            profiles = Exp_Util.getEntities(BASEPATH_CER + "profiles/", dataset, CLEAN);
        } else {
            //profiles = Utilities.getEntities(BASEPATH, DATASET, CLEAN);
            profiles = Exp_Util.getEntities(CLEAN ? BASEPATH_CER : BASEPATH_DER + "profiles/", dataset, CLEAN);
            
            //AQUI
//            EntityProfile newProfile = new EntityProfile("");
            //FILMES
//            newProfile.addAttribute("starring", "Malin Larsson"); //Malin Larsson
//        	newProfile.addAttribute("starring", "Joshefin Nelden");//Josefin Neldén
//        	newProfile.addAttribute("starring", "Cecilya Walin");//Cecilia Wallin
//        	newProfile.addAttribute("title", "Tajenare kungin");
//        	newProfile.addAttribute("writer", "Anna Fredriksson");
            
            //PUBLICACOES
//        	newProfile.addAttribute("authors", "Michael Stonebraker"); //Michael Stonebraker, Sunita Sarawagi
//        	newProfile.addAttribute("venue", "VLDB");
//        	newProfile.addAttribute("year", "1996");
//        	newProfile.addAttribute("title", "Reordering Query Execution in Tertiary Memory Databases");
//            newProfile.addAttribute("authors", "Tiago Brasileiro");
//        	newProfile.addAttribute("venue", "Very Large Data Bases");
//        	newProfile.addAttribute("title", "Parallel  Bases");
//        	profiles[0].set(602, newProfile);
//            for (Attribute att : profiles[0].get(602).getAttributes()) {
//				System.out.println(att.getName() + ": " + att.getValue());
//			}
//            System.out.println("------------------");
            
            //end AQUI
        }

        //List<EntityProfile>[] profiles = Utilities.getEntities(BASEPATH, DATASET, CLEAN);
        AbstractBlockingMethod blocking;

        Instant start = Instant.now();
        long init = System.currentTimeMillis();

        if (profiles.length > 1) {
            if (blocking_type == "T") {
                blocking = new TokenBlocking(new List[]{profiles[0], profiles[1]});
            } else {
                //blocking = new TokenBlocking(new List[]{profiles[0]});
                blocking = new BlockBuilding.MemoryBased.AttributeClusteringBlockingEntropy(RepresentationModel.TOKEN_SHINGLING, profiles, 120, 3, true);
                //blocking = new AttributeClusteringBlocking_original(RepresentationModel.TOKEN_UNIGRAMS, Exp_Util.getEntitiesPath(BASEPATH, dataset, CLEAN));
            }
        } else {
            System.out.println("\nok\n");
            //blocking = new TokenBlocking(new List[]{profiles[0]});
            blocking = new BlockBuilding.MemoryBased.AttributeClusteringBlockingEntropy(RepresentationModel.TOKEN_SHINGLING, profiles, 120, 3, true);
            //blocking = new AttributeClusteringBlocking(RepresentationModel.TOKEN_UNIGRAMS, new List[]{profiles[0]});

        }
        List<AbstractBlock> blocks = blocking.buildBlocks();
        long end = System.currentTimeMillis();
        
        double amount = 0.0;
        for (AbstractBlock abstractBlock : blocks) {
        	amount += abstractBlock.getNoOfComparisons();
//			System.out.println(abstractBlock.showBlock());
		}
        
//        HashMap<Integer, Integer> groundtruth2 = extractGroundtruth("dblp_acm/groundtruth");
		HashMap<Integer, Integer> groundtruth2 = extractGroundtruth("movies/groundtruth");
		
		double[] metrics2 = computeMetrics(blocks, groundtruth2);//extract recall, precision and f-measure.
		
		System.out.println("Recall: " + metrics2[0]);
		System.out.println("Precision: " + metrics2[1]);
		System.out.println("F-measure: " + metrics2[2]);
		
		System.out.println("Tempo(cent of sec): " + ((end - init)/100));


        double SMOOTHING_FACTOR = 1.005; // CLEAN
        //double SMOOTHING_FACTOR = 1.0; // CLEAN Dbpedia
        //double SMOOTHING_FACTOR = 1.015; // DIRTY
        double FILTERING_RATIO = 0.8;

        // TODO NOTICE DOWN:
        //FOR CENSUS
//        double SMOOTHING_FACTOR = 1.05; // DIRTY
//        double FILTERING_RATIO = 1; //


        Instant start_purging = Instant.now();
        System.out.println("blocking time: " + Duration.between(start, start_purging));

        ComparisonsBasedBlockPurging cbbp = new ComparisonsBasedBlockPurging(SMOOTHING_FACTOR);
        cbbp.applyProcessing(blocks);

        System.out.println("\n01: " + blocks.get(0).getEntropy() + "\n\n");

        BlockFiltering bf = new BlockFiltering(FILTERING_RATIO);
        bf.applyProcessing(blocks);

        System.out.println("\n02: " + blocks.get(0).getEntropy() + "\n\n");


        System.out.println("n. of blocks: " + blocks.size());

        AbstractDuplicatePropagation adp = Exp_Util.getGroundTruth(CLEAN ? BASEPATH_CER : BASEPATH_DER /*+ "groundTruth/"*/, dataset, CLEAN);


        Instant start_blast = Instant.now();

        System.out.println("block purging_filtering time: " + Duration.between(start_purging, start_blast));

        System.out.println("\nmain: " + blocks.get(0).getEntropy() + "\n\n");
        BlastWeightedNodePruning b_wnp = new BlastWeightedNodePruning(adp, ws, th_schme, blocks.size());

        //OnTheFlyMethods.FastImplementations.RedefinedWeightedNodePruning b_wnp = new OnTheFlyMethods.FastImplementations.RedefinedWeightedNodePruning(adp, ws, th_schme, blocks.size());
        //OnTheFlyMethods.FastImplementations.ReciprocalWeightedNodePruning b_wnp = new OnTheFlyMethods.FastImplementations.ReciprocalWeightedNodePruning(adp, ws, th_schme, blocks.size());
        //BlastWeightedNodePruning bwnp = new BlastWeightedNodePruning(adp, ws, th_schemes[0], blocks.size());
        
        
        b_wnp.applyProcessing(blocks);
        
        //AQUI   Após fazer o pruning
        //Publicacoes: 640 - 1050
        //Movies: 602 - 111
//        System.out.println(b_wnp.getDetectedDuplicates().get(602));
//        for (IdDuplicates dup : b_wnp.getDetectedDuplicates().keySet()) {
//			if ((dup.getEntityId1() + ": " + dup.getEntityId2()).equals("205: 276")) {
//				
//			}
//		}
        
        

        double[] values = b_wnp.getPerformance();

        System.out.println("pc: " + values[0]);
        System.out.println("pq: " + values[1]);
        System.out.println("f1: " + (2 * values[0] * values[1]) / (values[0] + values[1]));

        Instant end_blast = Instant.now();

        System.out.println("blast time: " + Duration.between(start_blast, end_blast));
        System.out.println("Total time: " + Duration.between(start, end_blast));
    }

	private static double[] computeMetrics(List<AbstractBlock> collection,
			HashMap<Integer, Integer> groundtruth) {
		int totalOfDuplicates = groundtruth.size();
		int totalOfDuplicatesFromApproach = 0;
		int totalOfTrueIndentifiedDuplicates = 0;
		
		double[] output = new double[3];
		
		for (AbstractBlock block : collection) {
			
			for (Comparison comp : block.getComparisons()) {
				totalOfDuplicatesFromApproach++;
				Integer correspondent = groundtruth.get(comp.getEntityId1());
				if (correspondent != null) {
					if (correspondent == comp.getEntityId2()) {
						totalOfTrueIndentifiedDuplicates++;
//						groundtruthMyApproach.put(idEnt, groundtruth.get(idEnt));
//						System.out.println(comp.getEntityId1() + " - " + comp.getEntityId2());
						groundtruth.remove(comp.getEntityId1());//to avoid compute more than once.
					}
				}
			}
		}
		
//		for (Integer key : groundtruth.keySet()) {
//			System.out.println(key + " - " + groundtruth.get(key));
//		}

		System.out.println(totalOfTrueIndentifiedDuplicates + " of " + totalOfDuplicates);
		System.out.println("Num of duplicates from approach: " + totalOfDuplicatesFromApproach);
		double recall = ((double)totalOfTrueIndentifiedDuplicates)/totalOfDuplicates;
		double precision = ((double)totalOfTrueIndentifiedDuplicates)/totalOfDuplicatesFromApproach;
		double f1 = 2*((recall * precision)/(recall + precision));
		output[0] = recall;
		output[1] = precision;
		output[2] = f1;
		
		return output;
	}

	private static HashMap<Integer, Integer> extractGroundtruth(String path) {
		HashMap<Integer, Integer> output = new HashMap<Integer, Integer>();
		ObjectInputStream oisGT;
		try {
			oisGT = new ObjectInputStream(new FileInputStream(path));
			HashSet<IdDuplicates> hs = (HashSet<IdDuplicates>) oisGT.readObject();
			for (IdDuplicates dup : hs) {
//				System.out.println(dup.toString());
				output.put(dup.getEntityId1(), dup.getEntityId2());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return output;
	}
    
    
}