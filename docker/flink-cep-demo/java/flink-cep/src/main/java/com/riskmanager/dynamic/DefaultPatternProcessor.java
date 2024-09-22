package com.riskmanager.dynamic;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.dynamic.impl.json.util.CepJsonUtils;
import org.apache.flink.cep.dynamic.processor.PatternProcessor;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of the {@link PatternProcessor} that is configurable for {@link Pattern},
 * {@link KeySelector} and {@link PatternProcessFunction}.
 *
 * @param <T> Type of the elements appearing in the pattern and produced elements based on found
 *     matches.
 */
@PublicEvolving
public class DefaultPatternProcessor<T> implements PatternProcessor<T> {

    /** The ID of the pattern processor. */
    private final String id;

    /** The version of the pattern processor. */
    private final Integer version;

    /** The pattern of the pattern processor. */
    private final String patternStr;

    private final @Nullable PatternProcessFunction<T, ?> patternProcessFunction;

    public DefaultPatternProcessor(
            final String id,
            final Integer version,
            final String pattern,
            final @Nullable PatternProcessFunction<T, ?> patternProcessFunction,
            final ClassLoader userCodeClassLoader) {
        this.id = checkNotNull(id);
        this.version = checkNotNull(version);
        this.patternStr = checkNotNull(pattern);
        this.patternProcessFunction = patternProcessFunction;
    }

    @Override
    public String toString() {
        return "DefaultPatternProcessor{"
                + "id='"
                + id
                + '\''
                + ", version="
                + version
                + ", pattern="
                + patternStr
                + ", patternProcessFunction="
                + patternProcessFunction
                + '}';
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public Pattern<T, ?> getPattern(ClassLoader classLoader) {
        try {
            return (Pattern<T, ?>) CepJsonUtils.convertJSONStringToPattern(patternStr, classLoader);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the {@link PatternProcessFunction} to collect all the found matches.
     *
     * @return The pattern process function of the pattern processor.
     */
    @Override
    public PatternProcessFunction<T, ?> getPatternProcessFunction() {
        return patternProcessFunction;
    }
}