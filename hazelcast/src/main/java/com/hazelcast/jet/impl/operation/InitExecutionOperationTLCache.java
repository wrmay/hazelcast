package com.hazelcast.jet.impl.operation;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;

import java.util.HashMap;
import java.util.Map;

public class InitExecutionOperationTLCache {
    private static ThreadLocal<Map<Data, ExecutionPlan>> cache = ThreadLocal.withInitial(HashMap::new);

    public static void add(Data data, ExecutionPlan executionPlan) {
        cache.get().put(data, executionPlan);
    }

    public static ExecutionPlan get(Data data) {
        Map<Data, ExecutionPlan> localCache = cache.get();
        if (localCache.isEmpty()) {
            cache.remove();
            return null;
        }
        return localCache.get(data);
    }

    public static void clear() {
        cache.remove();
    }
}
