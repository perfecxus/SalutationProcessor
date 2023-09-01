package org.example;

import com.fasterxml.jackson.core.JsonParseException;
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
        TestRunner testRunner = TestRunners.newTestRunner(new SalutationProcessorV2());//intialize processor for test run
        //set the properties of the processor
        testRunner.setProperty(SalutationProcessorV2.SALUTATION,"Good Morning!");
        testRunner.setProperty(SalutationProcessorV2.BEFORE_OR_AFTER,"B");

        //enqueue a flowfile
        testRunner.enqueue("Sinchan");

        //execute 1 run - similar to NIFI UI's run once
        testRunner.run(1);

        //assert the input Q is empty and the flowfile is processed
        testRunner.assertQueueEmpty();

        //get the flowfiles from Success Q
        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(SalutationProcessorV2.SUCCESS);
        assertTrue("Number of flowfiles in Success Queue is as expected (No of Flowfile = 1) ",results.size()==1);

        MockFlowFile outputFlowfile = results.get(0);
        String outputFlowfileContent = new String(testRunner.getContentAsByteArray(outputFlowfile));
        System.out.println("output flowfile content: " + outputFlowfileContent);
        assertEquals("Good Morning! Sinchan",outputFlowfileContent);
    }
}
