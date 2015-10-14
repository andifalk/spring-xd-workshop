package info.novatec.xd.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.hateoas.PagedResources;
import org.springframework.xd.rest.client.impl.SpringXDTemplate;
import org.springframework.xd.rest.domain.DetailedContainerResource;
import org.springframework.xd.rest.domain.StreamDefinitionResource;
import org.springframework.xd.rest.domain.metrics.MetricResource;

/**
 * Application demonstrating deploying streams for a simple wordcount to spring xd and 
 * checking field name counter results.
 */
@SpringBootApplication
public class SpringXdClientApplication implements CommandLineRunner {

	private static final Logger LOG = LoggerFactory.getLogger(SpringXdClientApplication.class);
	
	private static final String WORD_STREAM = "words";
	
	private static final String WORDCOUNT_STREAM = "wordcount";
	
	private static final String WORD_STREAM_DEFINITION = "file --dir=%s --outputType=text/plain | "
			+ "splitter --expression=payload.split(' ')  | log";
	
	private static final String WORDCOUNT_STREAM_DEFINITION = "tap:stream:words.splitter > transform "
			+ "--expression=T(org.springframework.xd.tuple.TupleBuilder).tuple().of('word',payload)"
			+ " | field-value-counter --fieldName=word";

	@Value("${spring.xd.admin.url}")
	private String targetXDInstance;

	@Value("${input.directory}")
	private String inputDirectory;
	
	@Autowired
	private SpringXDTemplate xdTemplate;

    public static void main(String[] args) {
        SpringApplication.run(SpringXdClientApplication.class, args);
    }

	@Override
	public void run(String... args) throws Exception {
		
		// Log running spring xd container(s)
		PagedResources<DetailedContainerResource> containers = xdTemplate.runtimeOperations().listContainers();
		for (DetailedContainerResource container : containers) {
		    LOG.info("Container running: {}", container);
		}

		// Check if streams are already existing and undeploy/destroy if applicable
	    LOG.info("Check if streams {} and {} are already existing...", WORD_STREAM, WORDCOUNT_STREAM);
		PagedResources<StreamDefinitionResource> streams = xdTemplate.streamOperations().list();
		for (StreamDefinitionResource stream : streams) {
			if (stream.getName().equals(WORDCOUNT_STREAM)) {
				LOG.info("Undeploy/destroy stream {}...", WORDCOUNT_STREAM);
				xdTemplate.streamOperations().undeploy(WORDCOUNT_STREAM);
				xdTemplate.streamOperations().destroy(WORDCOUNT_STREAM);
			} else if (stream.getName().equals(WORD_STREAM)) {
				LOG.info("Undeploy/destroy stream {}...", WORD_STREAM);
				xdTemplate.streamOperations().undeploy(WORD_STREAM);
				xdTemplate.streamOperations().destroy(WORD_STREAM);
			}
		}
		
		// Also try to delete existing field name value counter
		try {
			xdTemplate.fvcOperations().delete(WORDCOUNT_STREAM);
		} catch (Exception e) {
			LOG.info("Counter {} does not exist", WORDCOUNT_STREAM);
		}
		
		// Now create and deploy all streams
		LOG.info("Creating stream {}", WORD_STREAM);
		xdTemplate.streamOperations().createStream(WORD_STREAM, String.format(WORD_STREAM_DEFINITION, inputDirectory), false);
		LOG.info("Creating stream {}", WORDCOUNT_STREAM);
		xdTemplate.streamOperations().createStream(WORDCOUNT_STREAM, WORDCOUNT_STREAM_DEFINITION, true);
		LOG.info("Deploy stream {}", WORD_STREAM);
		xdTemplate.streamOperations().deploy(WORD_STREAM, new HashMap<>());
		
		// Wait some time to get the words counted
		Thread.sleep(8000);
		
		// Now check the word counter values
		PagedResources<MetricResource> counters = xdTemplate.fvcOperations().list();
		for (MetricResource counter : counters) {
			LOG.info("List values of counter '{}'...", counter.getName());
			
			Map<String, Double> fieldValueCounts = xdTemplate.fvcOperations().retrieve(counter.getName()).getFieldValueCounts();
			
			// With java 8 this is a lot easier than before
			fieldValueCounts.entrySet()
				.stream()
				.sorted(new Comparator<Entry<String,Double>>() {
					@Override
					public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
						return o2.getValue().compareTo(o1.getValue());
					}
				})
				.limit(10)
				.forEach((counterEntry) -> LOG.info("'{}' = {}", counterEntry.getKey(), counterEntry.getValue()));
		}
		
	}
    
	@Bean
	public SpringXDTemplate xdTemplate() throws URISyntaxException {
		return new SpringXDTemplate(new URI(targetXDInstance));
	}
    
}
