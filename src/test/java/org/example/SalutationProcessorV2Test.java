package org.example;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.apache.nifi.util.TestRunner;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SalutationProcessorV2Test {

    @Test
    public void testProcessorForSalutationBefore(){
        TestRunner testRunner = TestRunners.newTestRunner(new SalutationProcessorV2());
        testRunner.setProperty(SalutationProcessorV2.SALUTATION,"Good Morning!");
        testRunner.setProperty(SalutationProcessorV2.BEFORE_OR_AFTER,"B");
        testRunner.enqueue("Sinchan");
        testRunner.run(1);

        testRunner.assertQueueEmpty();
        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(SalutationProcessorV2.SUCCESS);
        assertTrue("Number of flowfiles in Success Queue is as expected (No of Flowfile = 1) ",results.size()==1);
        MockFlowFile outputFlowfile = results.get(0);
        String outputFlowfileContent = new String(testRunner.getContentAsByteArray(outputFlowfile));
        assertEquals("Good Morning!  Sinchan",outputFlowfileContent);
    }
}
