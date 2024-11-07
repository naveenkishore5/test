import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import com.google.cloud.spanner.Mutation;
import java.util.ArrayList;
import java.util.List;

public class GCSCSVToSpannerPipeline {
    
    public interface MyPipelineOptions extends PipelineOptions {
        ValueProvider<String> getInputFile();
        void setInputFile(ValueProvider<String> inputFile);

        ValueProvider<String> getSpannerInstanceId();
        void setSpannerInstanceId(ValueProvider<String> instanceId);

        ValueProvider<String> getSpannerDatabaseId();
        void setSpannerDatabaseId(ValueProvider<String> databaseId);
    }

    public static void main(String[] args) {
        MyPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(MyPipelineOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        // Step 1: Read CSV file from GCS
        PCollection<String> csvData = pipeline
                .apply("Read CSV Data", TextIO.read().from(options.getInputFile()));

        // Step 2: Parse and Validate Data
        PCollection<ClientData> validClientData = csvData
                .apply("Parse and Validate", ParDo.of(new ValidateAndParseFn()));

        // Step 3: Write to Spanner
        validClientData.apply("Write to Spanner", MapElements
                .into(TypeDescriptor.of(Mutation.class))
                .via((ClientData data) -> Mutation.newInsertOrUpdateBuilder("ClientTable")
                        .set("ClientId").to(data.getClientId())
                        .set("Name").to(data.getName())
                        .set("Email").to(data.getEmail())
                        .set("Phone").to(data.getPhone())
                        .build()))
                .apply("Write Mutations", SpannerIO.write()
                        .withInstanceId(options.getSpannerInstanceId())
                        .withDatabaseId(options.getSpannerDatabaseId()));

        pipeline.run().waitUntilFinish();
    }

    // Custom DoFn for parsing and validating CSV rows
    static class ValidateAndParseFn extends DoFn<String, ClientData> {
        @ProcessElement
        public void processElement(@Element String row, OutputReceiver<ClientData> receiver) {
            String[] columns = row.split(",");
            if (columns.length == 4) { // Simple validation: check column count
                ClientData clientData = new ClientData(columns[0], columns[1], columns[2], columns[3]);
                receiver.output(clientData);
            }
        }
    }

    // Define your ClientData class
    static class ClientData {
        private String clientId;
        private String name;
        private String email;
        private String phone;

        // Constructors, Getters and Setters
        public ClientData(String clientId, String name, String email, String phone) {
            this.clientId = clientId;
            this.name = name;
            this.email = email;
            this.phone = phone;
        }

        public String getClientId() {
            return clientId;
        }

        public String getName() {
            return name;
        }

        public String getEmail() {
            return email;
        }

        public String getPhone() {
            return phone;
        }
    }
}
