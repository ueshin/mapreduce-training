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
package st.happy_camper.hadoop.mapreduce.training.reducesidejoin.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author ueshin
 */
public class ReduceSideJoinOutputKeyWritable implements WritableComparable<ReduceSideJoinOutputKeyWritable> {

    public String word;

    public String url;

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, word);
        if(url != null) {
            out.writeBoolean(true);
            Text.writeString(out, url);
        }
        else {
            out.writeBoolean(false);
        }
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        word = Text.readString(in);
        if(in.readBoolean()) {
            url = Text.readString(in);
        }
        else {
            url = null;
        }
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(ReduceSideJoinOutputKeyWritable o) {
        int cmp = word.compareTo(o.word);
        if(cmp == 0) {
            if(url != null) {
                if(o.url != null) {
                    cmp = url.compareTo(o.url);
                }
                else {
                    cmp = -1;
                }
            }
            else {
                if(o.url != null) {
                    cmp = 1;
                }
                else {
                    cmp = 0;
                }
            }
        }
        return cmp;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return word + "\t" + url;
    }

}
