package com.transport.data.config;

import java.nio.charset.StandardCharsets;

import javax.sql.DataSource;

import com.transport.data.Processor.JobCompletionNotificationListener;
import com.transport.data.Processor.PersonItemProcessor;
import com.transport.data.Reader.CsvItemReader;
import com.transport.data.model.Person;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	// @Bean
	// @StepScope
	// public Resource sampleCsvResource(String filePath) {
	// return new FileSystemResource("c:\\temp\\sample-data.csv");
	// }

	@Bean
	public CsvItemReader<Person> sampleReader() {
		String[] a = { "person_id","lastname", "firstname" };
		CsvItemReader<Person> reader = new CsvItemReader<>();
		reader.setCharset(StandardCharsets.UTF_8);
		reader.setLineSeparator("\r\n");
		reader.setStrict(true);
		reader.setResource(new FileSystemResource("c:\\temp\\sample-data.csv"));
		reader.setLinesToSkip(1);
		reader.setHeaders(a);
		reader.setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {
			{
				setTargetType(Person.class);
			}
		});
		reader.setNumberOfKeyValue(1);
		return reader;
	}

	/*
	 * 
	 * // tag::readerwriterprocessor[]
	 * 
	 * @Bean public FlatFileItemReader<Person> reader() { return new
	 * FlatFileItemReaderBuilder<Person>().name("personItemReader") .resource(new
	 * ClassPathResource("sample-data.csv")).delimited() .names(new String[] {
	 * "firstName", "lastName" }) .fieldSetMapper(new
	 * BeanWrapperFieldSetMapper<Person>() { { setTargetType(Person.class); }
	 * }).build(); }
	 */
	@Bean
	public PersonItemProcessor processor() {
		return new PersonItemProcessor();
	}

	@Bean
	public JdbcBatchItemWriter<Person> writer(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Person>()
				.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
				.sql("INSERT INTO people (person_id,first_name, last_name) "
						+ "VALUES (:person_id, :firstName, :lastName)").dataSource(dataSource)
				.build();
	}
	// end::readerwriterprocessor[]

	// tag::jobstep[]
	@Bean
	public Job importUserJob(JobCompletionNotificationListener listener, Step step1) {
		return jobBuilderFactory.get("importUserJob").incrementer(new RunIdIncrementer()).listener(listener).flow(step1)
				.end().build();
	}

	@Bean
	public Step step1(JdbcBatchItemWriter<Person> writer) {
		return stepBuilderFactory.get("step1").<Person, Person>chunk(10).reader(sampleReader()).processor(processor())
				.writer(writer).build();
	}
}
