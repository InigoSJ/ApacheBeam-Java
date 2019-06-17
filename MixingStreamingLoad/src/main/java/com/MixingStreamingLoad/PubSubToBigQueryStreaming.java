package com.MixingStreamingLoad;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.List;

import org.json.*;

public class PubSubToBigQueryStreaming {

    interface InsertOptions extends PipelineOptions {

        @Description("Pub/Sub topic to read from. Used if --input is empty.")
        @Required
        String getTopic();

        void setTopic(String value);

        @Description("The BigQuery table name. Should not already exist.")
        @Required
        String getOutputTableName();

        void setOutputTableName(String value);
    }

    public static void main(String[] args) throws Exception {
        InsertOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(InsertOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        String table = options.getOutputTableName();

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("age").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("urgency").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
        TableSchema schema = new TableSchema().setFields(fields);


        pipeline.apply("ReadFromPubSub", PubsubIO.readStrings().fromTopic(options.getTopic()))
                .apply("ConvertToTableRow", ParDo.of(new DoFn<String, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        JSONObject json = new JSONObject(c.element());

                        String name = json.getString("Name");
                        String ID = json.getString("ID");
                        Integer urgency = json.getInt("Urgency");
                        Integer age = json.getInt("Age");

                        TableRow row = new TableRow();

                        row.set("Name", name);
                        row.set("Age", age);
                        row.set("ID", ID);
                        row.set("Urgency", urgency);
                        row.set("Timestamp", Instant.now().toString());
                        c.output(row);
                    }
                }))
                .apply("WriteInBigQuery", BigQueryIO.writeTableRows().to(table)
                        .withSchema(schema)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));


        pipeline.run();
    }


}
