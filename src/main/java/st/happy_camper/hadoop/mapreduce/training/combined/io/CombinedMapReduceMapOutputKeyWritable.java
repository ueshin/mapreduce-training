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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * @author ueshin
 */
public class CombinedMapReduceMapOutputKeyWritable implements WritableComparable<CombinedMapReduceMapOutputKeyWritable> {

    public static enum KeyType {
        WORD_COUNT_V1, WORD_COUNT_V2, SEMI_JOIN
    }

    public KeyType keyType;

    public final Text wordCountV1Key = new Text();

    public final Text wordCountV2Key = new Text();

    public final Text semiJoinKey = new Text();

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, keyType.ordinal());
        switch(keyType) {
            case WORD_COUNT_V1:
                wordCountV1Key.write(out);
                break;
            case WORD_COUNT_V2:
                wordCountV2Key.write(out);
                break;
            case SEMI_JOIN:
                semiJoinKey.write(out);
                break;
        }
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        keyType = KeyType.values()[WritableUtils.readVInt(in)];
        switch(keyType) {
            case WORD_COUNT_V1:
                wordCountV1Key.readFields(in);
                break;
            case WORD_COUNT_V2:
                wordCountV2Key.readFields(in);
                break;
            case SEMI_JOIN:
                semiJoinKey.readFields(in);
                break;
        }
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(CombinedMapReduceMapOutputKeyWritable o) {
        int cmp = keyType.compareTo(o.keyType);
        if(cmp == 0) {
            switch(keyType) {
                case WORD_COUNT_V1:
                    return wordCountV1Key.compareTo(o.wordCountV1Key);
                case WORD_COUNT_V2:
                    return wordCountV2Key.compareTo(o.wordCountV2Key);
                case SEMI_JOIN:
                    return semiJoinKey.compareTo(o.semiJoinKey);
                default:
                    throw new IllegalArgumentException("Illegal KeyType: " + keyType);
            }
        }
        else {
            return cmp;
        }
    }

}
