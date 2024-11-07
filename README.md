Here's a full example workflow to set up and run a Dataflow pipeline locally using Apache Beam's DirectRunner (the local runner). This example shows how to build a Maven project, create the Java classes, set up the pipeline options, and simulate the Dataflow pipeline with local file inputs.

Let's go step-by-step:

Step 1: Set Up the Maven Project
Open Visual Studio Code or your preferred IDE.

Create a new Maven project structure if it’s not already set up. Your folder structure should look something like this:

css
Copy code
my-beam-pipeline/
├── pom.xml
└── src
    └── main
        └── java
            └── com
                └── example
                    ├── MyPipelineOptions.java
                    ├── ClientData.java
                    ├── ValidateAndParseFn.java
                    └── GCSCSVToSpannerPipeline.java
Step 2: Add Dependencies in pom.xml
Your pom.xml file should include the dependencies for Apache Beam SDK, DirectRunner, and any other libraries you need. Here’s an example:

xml
Copy code
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.example</groupId>
    <artifactId>my-beam-pipeline</artifactId>
    <version>1.0-SNAPSHOT</version>

    <dependencies>
        <!-- Apache Beam SDK -->
        <dependency>
            <groupId>org.apache.beam</groupId>
            <artifactId>beam-sdks-java-core</artifactId>
            <version>2.49.0</version>
        </dependency>

        <!-- Apache Beam DirectRunner for local execution -->
        <dependency>
            <groupId>org.apache.beam</groupId>
            <artifactId>beam-runners-direct-java</artifactId>
            <version>2.49.0</version>
        </dependency>
    </dependencies>
</project>
Step 3: Create the MyPipelineOptions Interface
In src/main/java/com/example/MyPipelineOptions.java, define your custom pipeline options interface for handling inputs and other configurations.

java
Copy code
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface MyPipelineOptions extends PipelineOptions {
    ValueProvider<String> getInputFile();
    void setInputFile(ValueProvider<String> inputFile);
}
Step 4: Create the ClientData Class
In src/main/java/com/example/ClientData.java, create a data model class to store parsed CSV records.

java
Copy code
public class ClientData {
    private String clientId;
    private String name;
    private String email;
    private String phone;

    public ClientData(String clientId, String name, String email, String phone) {
        this.clientId = clientId;
        this.name = name;
        this.email = email;
        this.phone = phone;
    }

    // Getters
    public String getClientId() { return clientId; }
    public String getName() { return name; }
    public String getEmail() { return email; }
    public String getPhone() { return phone; }

    @Override
    public String toString() {
        return "ClientData{" + "clientId='" + clientId + '\'' + ", name='" + name + '\'' +
               ", email='" + email + '\'' + ", phone='" + phone + '\'' + '}';
    }
}
Step 5: Create the ValidateAndParseFn Class
In src/main/java/com/example/ValidateAndParseFn.java, define a DoFn to parse and validate each line from the CSV file.

java
Copy code
import org.apache.beam.sdk.transforms.DoFn;

public class ValidateAndParseFn extends DoFn<String, ClientData> {
    @ProcessElement
    public void processElement(@Element String row, OutputReceiver<ClientData> receiver) {
        String[] columns = row.split(",");
        if (columns.length == 4) { // Simple validation: check if the row has 4 columns
            ClientData clientData = new ClientData(columns[0], columns[1], columns[2], columns[3]);
            receiver.output(clientData);
        } else {
            System.out.println("Invalid row: " + row);
        }
    }
}
Step 6: Create the GCSCSVToSpannerPipeline Class
In src/main/java/com/example/GCSCSVToSpannerPipeline.java, create the main pipeline code.

java
Copy code
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptor;

public class GCSCSVToSpannerPipeline {
    public static void main(String[] args) {
        MyPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(MyPipelineOptions.class);

        // Set the runner to DirectRunner for local execution
        options.setRunner(org.apache.beam.runners.direct.DirectRunner.class);

        Pipeline pipeline = Pipeline.create(options);

        // Read CSV data from a local file
        pipeline.apply("Read CSV", TextIO.read().from(options.getInputFile()))
                .apply("Parse and Validate", ParDo.of(new ValidateAndParseFn()))
                .apply("Log Output", MapElements
                    .into(TypeDescriptor.of(String.class))
                    .via((ClientData data) -> {
                        System.out.println("Parsed data: " + data);
                        return data.toString();
                    }));

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }
}
Step 7: Add a Sample CSV File
Create a sample CSV file locally for testing, for example, sample_data.csv:

plaintext
Copy code
1,John Doe,johndoe@example.com,1234567890
2,Jane Smith,janesmith@example.com,0987654321
3,Bob Johnson,bobjohnson@example.com,1231231234
invalid,row,for,test
Step 8: Build the Project
Run the following command to build the project:

bash
Copy code
mvn clean package
Step 9: Run the Pipeline Locally
Run the pipeline with the following command, providing the path to the CSV file as an argument:

bash
Copy code
mvn exec:java -Dexec.mainClass=com.example.GCSCSVToSpannerPipeline \
    -Dexec.args="--inputFile=sample_data.csv"
Step 10: Check Output
The output should display parsed data for each valid row and a message for any invalid rows. The Log Output transform will log each parsed row to the console.

Sample Output:

plaintext
Copy code
Parsed data: ClientData{clientId='1', name='John Doe', email='johndoe@example.com', phone='1234567890'}
Parsed data: ClientData{clientId='2', name='Jane Smith', email='janesmith@example.com', phone='0987654321'}
Parsed data: ClientData{clientId='3', name='Bob Johnson', email='bobjohnson@example.com', phone='1231231234'}
Invalid row: invalid,row,for,test
Summary
Local testing with DirectRunner: This setup uses DirectRunner to run the pipeline locally, which is suitable for testing.
Data validation: The ValidateAndParseFn class parses each CSV row and outputs valid rows while printing invalid rows.
Logging output: The final step logs each ClientData instance to the console, allowing you to verify that data is parsed correctly.
After validating locally, you can replace DirectRunner with DataflowRunner to deploy to Google Cloud Dataflow for a production run.
