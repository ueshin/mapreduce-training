/*
 * Copyright 2011 Happy-Camper Street.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package st.happy_camper.hadoop.mapreduce.training.combined.io;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author ueshin
 */
public class CombinedMapReduceGroupingComparator extends WritableComparator {

    /**
     * @param keyClass
     * @param createInstances
     */
    public CombinedMapReduceGroupingComparator() {
        super(CombinedMapReduceMapOutputKeyWritable.class, true);
    }

    /*
     * (non-Javadoc)
     * @see
     * org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.
     * WritableComparable, org.apache.hadoop.io.WritableComparable)
     */
    @Override
    @SuppressWarnings("rawtypes")
    public int compare(WritableComparable a, WritableComparable b) {
        CombinedMapReduceMapOutputKeyWritable aKey = ((CombinedMapReduceMapOutputKeyWritable) a);
        CombinedMapReduceMapOutputKeyWritable bKey = ((CombinedMapReduceMapOutputKeyWritable) b);
        int cmp = aKey.keyType.compareTo(bKey.keyType);
        if(cmp == 0) {
            switch(aKey.keyType) {
                case WORD_COUNT_V1:
                    return aKey.wordCountV1Key.compareTo(bKey.wordCountV1Key);
                case WORD_COUNT_V2:
                    return Integer.valueOf(aKey.wordCountV2Key.charAt(0)).compareTo(bKey.wordCountV2Key.charAt(0));
                case SEMI_JOIN:
                    return aKey.semiJoinKey.compareTo(bKey.semiJoinKey);
                default:
                    throw new IllegalArgumentException("Illegal KeyType: " + aKey.keyType);
            }
        }
        else {
            return cmp;
        }
    }

}
