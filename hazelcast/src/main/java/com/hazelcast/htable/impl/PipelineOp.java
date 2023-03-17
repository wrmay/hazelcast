package com.hazelcast.htable.impl;

import com.hazelcast.internal.tpc.Op;
import com.hazelcast.internal.tpc.OpCodes;

public class PipelineOp extends Op {

    public PipelineOp() {
        super(OpCodes.PIPELINE);
    }

    @Override
    public int run() throws Exception {
        throw new UnsupportedOperationException();
    }
}
