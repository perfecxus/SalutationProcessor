package org.example;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class SalutationProcessor extends AbstractProcessor {

    public static final PropertyDescriptor SALUTATION = new PropertyDescriptor.Builder()
            .name("Salutation")
            .displayName("Salutation")
            .description("The salutation that you want to prepend or append to the name coming in the flowfile")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor BEFORE_OR_AFTER = new PropertyDescriptor.Builder()
            .name("BeforeOrAfter")
            .displayName("Insert Before Or After")
            .description("Whether you cant prepend or append the salutation. For prepend use - B and for Append - use A")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Succes relationship")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Failure relationship")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    public void init(final ProcessorInitializationContext processContext){
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SALUTATION);
        properties.add(BEFORE_OR_AFTER);
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships(){
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors(){
        return properties;
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {

        FlowFile flowFile = processSession.get();
        if(flowFile==null)
            return;
        String beforeOrAfter = processContext.getProperty(BEFORE_OR_AFTER).evaluateAttributeExpressions(flowFile).getValue();
        String salutationStr = processContext.getProperty(SALUTATION).evaluateAttributeExpressions(flowFile).getValue();
        if(!("B".equalsIgnoreCase(beforeOrAfter)||"A".equalsIgnoreCase(beforeOrAfter))){
            processSession.transfer(flowFile,FAILURE);
        }

        final AtomicReference<String> result = new AtomicReference<>();

        processSession.read(flowFile,inputStream -> {
            String flowContent = IOUtils.toString(inputStream,"UTF-8");

            if("B".equalsIgnoreCase(beforeOrAfter)){
                result.getAndSet(salutationStr+" "+flowContent);
            }else if("A".equalsIgnoreCase(beforeOrAfter)){
                result.getAndSet(flowContent+" "+salutationStr);
            }
        });

        processSession.write(flowFile,outputStream -> {
            outputStream.write(result.get().getBytes(StandardCharsets.UTF_8));
        });
        processSession.transfer(flowFile,SUCCESS);

    }
}
