package com.training_harder.daily;

import java.util.ArrayList;
import java.util.*;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.Bigquery.Datasets.List;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.training_harder.daily.SamplePipeLine;

public class SamplePipeLine {

	 private static final Logger LOG = LoggerFactory.getLogger(SamplePipeLine.class);

	  public static void main(String[] args) {
		  
		MyOption myOption = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOption.class);
		myOption.setTempLocation("gs://beam_trainings369/input");
		myOption.setStagingLocation("gs://beam_trainings369/input");
		myOption.setProject("beam-training905");
		
	    Pipeline p = Pipeline.create(myOption);
	    
	    helperMethod(p);
	  }

	  public static void helperMethod(Pipeline p) {
		    
		    java.util.List<TableFieldSchema> columns = new java.util.ArrayList<TableFieldSchema>();
		   // ArrayList<TableFieldSchema> columns = new ArrayList<TableFieldSchema>();
		    columns.add(new TableFieldSchema().setName("VideoId").setType("STRING"));
		    columns.add(new TableFieldSchema().setName("Upload_by").setType("STRING"));
		    columns.add(new TableFieldSchema().setName("Category").setType("STRING"));
		    columns.add(new TableFieldSchema().setName("Length").setType("INTEGER"));
		    columns.add(new TableFieldSchema().setName("Views").setType("INTEGER"));
		    columns.add(new TableFieldSchema().setName("Ratings").setType("FLOAT"));
		    columns.add(new TableFieldSchema().setName("Ratings_Given").setType("STRING"));
		    
		    TableSchema tbSchema = new TableSchema().setFields(columns);
		    
		    PCollection<String> pInput = p.apply(TextIO.read().from("gs://beam_trainings369/input/youtubedata.csv"));
		    
		    pInput.apply(ParDo.of(new DoFn<String, TableRow>() {
		    	
		    	@ProcessElement
		    	public void processElement(ProcessContext c) {

		    	String[] arra = c.element().trim().split("\\s+");
	     	
		    		//Eliminate rows with non-null values less than 8 by checking arrays with length greater than 7
		    		if(arra.length > 7) {
		    			
		    		TableRow row = new TableRow();
		    			String add;
		    		if(arra[4].equals("&")) {
		    			add = arra[3]+arra[4]+arra[5];
		    			arra[4] = add;
		    			
		    			row.set("VideoId", arra[0]);
		    			row.set("Upload_by", arra[1]);
		    			row.set("Category", arra[4]);
		    			row.set("Length", arra[6]);
		    			row.set("Views", arra[7]);
		    			row.set("Ratings", arra[8]);
		    			row.set("Ratings_Given", arra[9]);
		    			c.output(row);
		    			} 
		    			else 
		    			{
		    			row.set("VideoId", arra[0]);
			    		row.set("Upload_by", arra[1]);
			    		row.set("Category", arra[3]);
			    		row.set("Length", arra[4]);
			    		row.set("Views", arra[5]);
			    		row.set("Ratings", arra[6]);
			    		row.set("Ratings_Given", arra[7]);
			    		c.output(row);
		    			}
		  
		    		}
		    		
		    	}
		    	
		    	
		    }
		    ))
		    .apply(BigQueryIO.writeTableRows().to("learn_bream_daily.youtubedata")
		    	.withSchema(tbSchema)
		    	.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
		    	.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
		    	    ); 
		    p.run();
		  
	  }

}

/**Column1: Video id -- String
 * Column2: uploader of the Video -- String 
 * Column3: Interval between day of establishment of Youtube and the date of uploading of the video of integer data type.------
 * Column4: Video Category --- String 
 * Column5: Video length --- integer  
 * Column6: Video views -- Integer
 * Column7: Video Ratings --- float
 * Column8: Number of ratings given -- Integer 
 * Column9: Number of video comments --- integer 
8*/
