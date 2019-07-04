package org.yago.yago4.converter.plan;


import java.io.Serializable;
import java.util.function.BiPredicate;

@FunctionalInterface
public interface SerializableBiPredicate<T, U> extends BiPredicate<T, U>, Serializable {
}
