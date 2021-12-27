package com.betterreads;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import com.betterreads.author.Author;
import com.betterreads.author.AuthorRepository;
import com.betterreads.book.Book;
import com.betterreads.book.BookRepository;

import connection.DataStaxAstraProperties;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsDataLoaderApplication {

	@Autowired
	AuthorRepository authorRepository;

	@Autowired
	BookRepository bookRepository;

	@Value("${datadump.location.author}")
	private String authorDumpLocation;

	@Value("${datadump.location.works}")
	private String worksDumpLocation;

	public static void main(String[] args) {
		SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
	}

	private void initAuthors() {
		Path path = Paths.get(authorDumpLocation);
		try {
			// Read and Parse the line
			Stream<String> lines = Files.lines(path);
			lines.forEach(line -> {
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObject = new JSONObject(jsonString);

					// Construct author object
					Author author = new Author();
					author.setName(jsonObject.optString("name"));
					author.setPersonalName(jsonObject.optString("personal_name"));
					author.setId(jsonObject.optString("key").replace("/authors/", ""));

					// Persist authors data using repository
					authorRepository.save(author);
				} catch (JSONException e) {
					e.printStackTrace();
				}
			});
			lines.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void initWorks() {
		Path path = Paths.get(worksDumpLocation);
		DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		try {
			// Read and Parse the line
			Stream<String> lines = Files.lines(path);
			lines.forEach(line -> {
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObject = new JSONObject(jsonString);

					// Construct Book object
					Book book = new Book();
					book.setId(jsonObject.optString("key").replace("/works/", ""));

					JSONObject jsonDescription = jsonObject.optJSONObject("description");
					if (jsonDescription != null) {
						book.setDescription(jsonDescription.optString("value"));
					}
					book.setName(jsonObject.optString("title"));

					JSONArray jsonAuthors = jsonObject.optJSONArray("authors");
					if (jsonAuthors != null && jsonAuthors.length() > 0) {
						List<String> authorIds = new ArrayList<>();
						for (int i = 0; i < jsonAuthors.length(); i++) {
							String authorId = jsonAuthors.getJSONObject(i).getJSONObject("author").getString("key")
									.replace("/authors/", "");
							authorIds.add(authorId);
						}
						book.setAuthorIds(authorIds);
						List<String> authorNames = authorIds.stream().map(id -> authorRepository.findById(id))
								.map(optionalAuthor -> {
									if (!optionalAuthor.isPresent())
										return "Unknown Author";
									return optionalAuthor.get().getName();
								}).collect(Collectors.toList());
						book.setAuthorNames(authorNames);
					}
					JSONArray jsonCoverIds = jsonObject.optJSONArray("covers");
					if (jsonCoverIds != null && jsonCoverIds.length() > 0) {
						List<String> coverIds = new ArrayList<>();
						for (int i = 0; i < jsonCoverIds.length(); i++) {
							coverIds.add(jsonCoverIds.getString(i));
						}
						book.setCoverIds(coverIds);
					}
					JSONObject jsonPublishedDate = jsonObject.optJSONObject("created");
					if (jsonPublishedDate != null) {
						book.setPublishedDate(LocalDate.parse(jsonPublishedDate.getString("value"), dateTimeFormatter));
					}
					// Persist books data using repository
					bookRepository.save(book);
				} catch (JSONException e) {
					e.printStackTrace();
				}
			});
			lines.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@PostConstruct
	public void start() {
		initAuthors();
		initWorks();
	}

	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties dataStaxAstraProperties) {
		Path bundle = dataStaxAstraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}

}
