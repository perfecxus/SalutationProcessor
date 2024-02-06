package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.*;
import org.example.validators.CustomValidators;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class SalutationProcessorV4 extends AbstractProcessor {

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

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("Specifies the Controller Service to use for reading incoming data")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();
    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
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
        properties.add(RECORD_READER);
        properties.add(RECORD_WRITER);
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

        final RecordReaderFactory readerFactory = processContext.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = processContext.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        String beforeOrAfter = processContext.getProperty(BEFORE_OR_AFTER).evaluateAttributeExpressions(flowFile).getValue();
        String salutationStr = processContext.getProperty(SALUTATION).evaluateAttributeExpressions(flowFile).getValue();
        String lookupTable = processContext.getProperty(LOOKUP_TABLE).evaluateAttributeExpressions(flowFile).getValue();
        if(!("B".equalsIgnoreCase(beforeOrAfter)||"A".equalsIgnoreCase(beforeOrAfter))){
            processSession.transfer(flowFile,FAILURE);
            return;
        }


        try(final Connection connection = dbcpService.getConnection()) {
            Statement sqlStatement = connection.createStatement();
            processSession.write(flowFile, new StreamCallback() {
                @Override
                public void process(InputStream inputStream, OutputStream outputStream) throws IOException {
                    try (final RecordReader reader = readerFactory.createRecordReader(flowFile.getAttributes(), inputStream, flowFile.getSize(), getLogger())) {

                        // Get the first record and process it before we create the Record Writer. We do this so that if the Processor
                        // updates the Record's schema, we can provide an updated schema to the Record Writer. If there are no records,
                        // then we can simply create the Writer with the Reader's schema and begin & end the Record Set.
                        Record firstRecord = reader.nextRecord();
                        if (firstRecord == null) {
                            final RecordSchema writeSchema = writerFactory.getSchema(flowFile.getAttributes(), reader.getSchema());
                            try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, outputStream, flowFile.getAttributes())) {
                                writer.beginRecordSet();

                                final WriteResult writeResult = writer.finishRecordSet();
                            }

                            return;
                        }

                        firstRecord = processRecord(firstRecord,flowFile, processContext);//AbstractRecordProcessor.this.process(firstRecord, original, context, 1L);

                        final RecordSchema writeSchema = writerFactory.getSchema(flowFile.getAttributes(), firstRecord.getSchema());
                        try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, outputStream, flowFile.getAttributes())) {
                            writer.beginRecordSet();

                            writer.write(firstRecord);

                            Record record;
                            long count = 1L;
                            while ((record = reader.nextRecord()) != null) {
                                final Record processed = processRecord(record,flowFile, processContext);//AbstractRecordProcessor.this.process(record, original, context, ++count);
                                writer.write(processed);
                            }

                            final WriteResult writeResult = writer.finishRecordSet();

                        }
                    } catch (final SchemaNotFoundException e) {
                        throw new ProcessException(e.getLocalizedMessage(), e);
                    } catch (final MalformedRecordException e) {
                        throw new ProcessException("Could not parse incoming data", e);
                    }
                }

                private Record processRecord(Record record, FlowFile flowFile, ProcessContext processContext) {

                    String name = record.getAsString("name");
                    String ageRange = record.getAsString("age_range");
                    ResultSet resultSet = null;
                    String offer = null;
                    try {
                        resultSet = sqlStatement.executeQuery(String.format("SELECT offer FROM %s WHERE age_range='%s'",lookupTable,ageRange));

                        offer = resultSet.next()?resultSet.getString(1):"No offers available now!";
                    } catch (SQLException throwables) {
                        offer = "Sorry,unable to fetch your offer at this time";
                    }
                    String result = null;
                    if ("B".equalsIgnoreCase(beforeOrAfter)) {
                        result= salutationStr + " " + name+ "!"+offer;
                    } else if ("A".equalsIgnoreCase(beforeOrAfter)) {
                        result = name + " " + salutationStr+"!" + offer;
                    }
                    final List<RecordField> fields = Arrays.asList(new RecordField("display_output", RecordFieldType.STRING.getDataType()));
                    final RecordSchema schema = new SimpleRecordSchema(fields);
                    final Record mapRecord = new MapRecord(schema, new HashMap<>());
                    mapRecord.setValue(fields.get(0), result);
                    return mapRecord;
                }

            });

            processSession.transfer(flowFile, SUCCESS);
        }catch (SQLException ex){
            getLogger().error("Error while eshtablishing db connection",ex);
            processSession.transfer(flowFile, FAILURE);

        }catch (Exception ex){
            getLogger().error("Error while fetching offer",ex);
            processSession.transfer(flowFile, FAILURE);
        }

    }
}
