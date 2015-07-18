package org.crustee.raft.storage.btree;

import org.assertj.core.api.Condition;

public class Utils {

    public static Condition<Object[]> numberOfElementsAndNulls(int expectedCount) {
        return new Condition<Object[]>() {
            @Override
            public boolean matches(Object[] value) {
                if(value.length < expectedCount) {
                    return false;
                }
                for (int i = 0; i < expectedCount; i++) {
                    if(value[i] == null) {
                        return false;
                    }
                }
                for (int i = expectedCount; i < value.length; i++) {
                    if(value[i] != null) {
                        return false;
                    }
                }
                return true;
            }
        }.describedAs("should have %s elements", expectedCount);
    }

}
