package com.app.config;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.app.model.Product;
import com.app.processor.MyItemProcessor;

@Configuration
@EnableBatchProcessing
public class BatchConfig {
	
	@Autowired
	private JobBuilderFactory jf;
	
	@Autowired
	private StepBuilderFactory sf;
	
	@Autowired
	private DataSource datasource;
	
	@Bean
	public DataSource datasource() {
		
		DriverManagerDataSource datasource=new DriverManagerDataSource();
		datasource.setDriverClassName("com.mysql.jdbc.Driver");
		datasource.setUrl("jdbc:mysql://localhost:3306/batch");
		datasource.setUsername("root");
		datasource.setPassword("Sudha@123");
		
		return datasource;
		
	}
	
 @Bean
 
 public JdbcCursorItemReader<Product> reader(){
	 
	 JdbcCursorItemReader<Product> reader=new JdbcCursorItemReader<Product>();
	 reader.setDataSource(datasource);
	 reader.setSql("SELECT id,code,cost,disc,gst from prodstab");
	 reader.setRowMapper(new UserRowMapper());
	 
	 return reader;
 }
 
 
 public class  UserRowMapper implements RowMapper<Product>{

	@Override
	public Product mapRow(ResultSet rs, int rowNum) throws SQLException {
		// TODO Auto-generated method stub
		Product p=new Product();
		p.setId(rs.getInt("id"));
		p.setCode(rs.getString("code"));
		p.setCost(rs.getDouble("cost"));
		p.setDisc(rs.getDouble("disc"));
		p.setGst(rs.getDouble("gst"));
		return p;
	}
	
}
 
 @Bean
	public ItemProcessor<Product, Product> processor(){
		//return new MyProcessor();
		return (p)->{
			p.setDisc(p.getCost()*3/100.0);
			p.setGst(p.getCost()*12/100.0);
			return p;
		};
	}
	 
 @Bean
 public FlatFileItemWriter<Product> writer(){
	 FlatFileItemWriter<Product> writer=new FlatFileItemWriter<Product>();
	 writer.setResource(new ClassPathResource("users.csv"));
	  writer.setLineAggregator(new DelimitedLineAggregator<Product>(){{
		setDelimiter(",");
		setFieldExtractor(new BeanWrapperFieldExtractor<Product>() {{
			setNames(new String[] {"id","code","cost","disc","gst"});
			
		}});
		
		
		  
	  }});
	  
	  
	 return writer;
 }
 
 @Bean
 public Step s1() {
	 return sf.get("s1").<Product,Product>chunk(3).
			 reader(reader())
			 .processor(processor())
			 .writer(writer())
			 .build();
 }
 @Bean
 public Job j1() {
	 return jf.get("j1").incrementer(new RunIdIncrementer()).flow(s1()).end().build();
 }
}
