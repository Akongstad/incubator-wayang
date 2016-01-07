package org.qcri.rheem.core.mapping.test;

import org.qcri.rheem.core.mapping.OperatorMatch;
import org.qcri.rheem.core.mapping.ReplacementSubplanFactory;
import org.qcri.rheem.core.mapping.SubplanMatch;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.plan.test.TestSink;
import org.qcri.rheem.core.plan.test.TestSink2;

/**
 * This factory replaces a {@link org.qcri.rheem.core.plan.test.TestSink} by a
 * {@link org.qcri.rheem.core.plan.test.TestSink2}.
 */
public class TestSinkToTestSink2Factory extends ReplacementSubplanFactory {

    @Override
    protected Operator translate(SubplanMatch subplanMatch) {
        // Retrieve the matched TestSink.
        final OperatorMatch sinkMatch = subplanMatch.getOperatorMatches().get("sink");
        final TestSink testSink = (TestSink) sinkMatch.getOperator();

        // Translate the TestSink to a TestSink2.
        return new TestSink2<>(testSink.getType());
    }

}
