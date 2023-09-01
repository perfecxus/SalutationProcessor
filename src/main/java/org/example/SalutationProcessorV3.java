package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.example.validators.CustomValidators;
import org.apache.nifi.dbcp.DBCPService;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class SalutationProcessorV3 extends AbstractProcessor {

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
            .addValidator(CustomValidators.BEFORE_OR_AFTER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

   public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor LOOKUP_TABLE = new PropertyDescriptor.Builder()
            .name("Lookup Table")
            .displayName("Lookup Table")
            .description("The name of look up table for looking up punctuation mark")
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
    private DBCPService dbcpService;
    private ObjectMapper mapper;

    @Override
    public void init(final ProcessorInitializationContext processContext){
        mapper = new ObjectMapper();
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SALUTATION);
        properties.add(BEFORE_OR_AFTER);
        properties.add(LOOKUP_TABLE);
        properties.add(DBCP_SERVICE);
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

    @OnScheduled
    public void setup(ProcessContext context) {
        dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {

        FlowFile flowFile = processSession.get();
        if(flowFile==null)
            return;
        String beforeOrAfter = processContext.getProperty(BEFORE_OR_AFTER).evaluateAttributeExpressions(flowFile).getValue();
        String salutationStr = processContext.getProperty(SALUTATION).evaluateAttributeExpressions(flowFile).getValue();
        String lookupTable = processContext.getProperty(LOOKUP_TABLE).evaluateAttributeExpressions(flowFile).getValue();
        if(!("B".equalsIgnoreCase(beforeOrAfter)||"A".equalsIgnoreCase(beforeOrAfter))){
            processSession.transfer(flowFile,FAILURE);
        }

        try(final Connection connection = dbcpService.getConnection()) {
            final AtomicReference<String> result = new AtomicReference<>();
            Statement sqlStatement = connection.createStatement();

            processSession.read(flowFile, inputStream -> {
                String flowContent = IOUtils.toString(inputStream, "UTF-8");
                JsonNode flowContentJson = mapper.readTree(flowContent);
                String name = flowContentJson.get("name").asText();
                String ageRange = flowContentJson.get("age_range").asText();
                ResultSet resultSet = null;
                String offer = null;
                try {
                    resultSet = sqlStatement.executeQuery(String.format("SELECT offer FROM %s WHERE age_range='%s'",lookupTable,ageRange));

                    offer = resultSet.next()?resultSet.getString(1):"No offers available now!";
                } catch (SQLException throwables) {
                    offer = "Sorry,unable to fetch your offer at this time";
                }
                if ("B".equalsIgnoreCase(beforeOrAfter)) {
                    result.getAndSet(salutationStr + " " + name+ "!"+offer);
                } else if ("A".equalsIgnoreCase(beforeOrAfter)) {
                    result.getAndSet(name + " " + salutationStr+"!" + offer);
                }
            });

            processSession.write(flowFile, outputStream -> {
                outputStream.write(result.get().getBytes(StandardCharsets.UTF_8));
            });
            processSession.transfer(flowFile, SUCCESS);
        }catch (SQLException ex){

        }

    }
}
