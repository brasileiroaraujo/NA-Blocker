/*
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    Copyright (C) 2015 George Antony Papadakis (gpapadis@yahoo.gr)
 */
package OnTheFlyMethods.FastImplementations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import BlockProcessing.ComparisonRefinement.AbstractDuplicatePropagation;
import BlockProcessing.ComparisonRefinement.BilateralDuplicatePropagation;
import DataStructures.IdDuplicates;
import MetaBlocking.ThresholdWeightingScheme;
import MetaBlocking.WeightingScheme;

public class BlastWeightedNodePruning extends RedefinedWeightedNodePruning {

    //private boolean threshold_reiprocal;
    //private ThresholdWeightingScheme threshold_type;

    public BlastWeightedNodePruning(AbstractDuplicatePropagation adp, WeightingScheme scheme, ThresholdWeightingScheme threshold_type) {
        super(adp, "Reciprocal Weighted Node Pruning (" + scheme + ")", scheme, threshold_type);
        //this.threshold_type = threshold_type;
        //this.threshold_reiprocal = threshold_reiprocal;
    }

    public BlastWeightedNodePruning(AbstractDuplicatePropagation adp, WeightingScheme scheme, ThresholdWeightingScheme threshold_type, double totalBlocks) {
        super(adp, "Reciprocal Weighted Node Pruning (" + scheme + ")", scheme, threshold_type);
        //this.threshold_type = threshold_type;
        //this.threshold_reiprocal = threshold_reiprocal;
        this.totalBlocks = totalBlocks;
    }

    @Override
    protected boolean isValidComparison(int entityId, int neighborId) {
        double weight = getWeight(entityId, neighborId);
        boolean inNeighborhood1 = averageWeight[entityId] <= weight;
        boolean inNeighborhood2 = averageWeight[neighborId] <= weight;

        switch (threshold_type) {
            case AVG:
                if (inNeighborhood1 && inNeighborhood2) {
                    return entityId < neighborId;
                }
                break;
            case AM2:
                //System.out.println("AM2");
                double th_12 = (averageWeight[entityId] + averageWeight[neighborId]) / 2;
                //double th_12 = Math.sqrt(Math.pow(averageWeight[entityId], 2) + Math.pow(averageWeight[neighborId], 2)) / 2;

                if (th_12 <= weight) {
                    //if (Math.max(averageWeight[entityId],averageWeight[neighborId]) <= weight) {
                    return entityId < neighborId;
                }
                break;
            case AM3:
//                if ((averageWeight[entityId] + averageWeight[neighborId]) / 4 <= weight) {
//                    //if (Math.max(averageWeight[entityId],averageWeight[neighborId]) <= weight) {
//                    return entityId < neighborId;
//                }
//                break;
                double th12 = Math.sqrt(Math.pow(averageWeight[entityId], 2) + Math.pow(averageWeight[neighborId], 2)) / 4;
                if (th12 <= weight) {
                    //if (Math.max(averageWeight[entityId],averageWeight[neighborId]) <= weight) {
                    return entityId < neighborId;
                }
                break;
        }
        return false;
    }
    
    //AQUI
    public HashMap<Integer, Integer> getDetectedDuplicates(){
    	HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
    	ArrayList<IdDuplicates> dup = ((BilateralDuplicatePropagation)duplicatePropagation).allDetectedDuplicates;
    	for (int i = 0; i < dup.size(); i++) {
			map.put(dup.get(i).getEntityId1(), dup.get(i).getEntityId2());
		}
    	return map;
    }
}
