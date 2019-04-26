package www.com.springboot_mongo_iv.entry_point;

import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import www.com.springboot_mongo_iv.document.Movie;

@RestController
public class MovieAPI {

	@Autowired
	private MongoTemplate mongoTemplate;

	@RequestMapping(value = "/movie/{id}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
	public ResponseEntity<Object> findCurrentSignUpLog(@PathVariable("id") String id) {
		Query query = Query.query(Criteria.where("id").is(id));
		List<Movie> movieList = mongoTemplate.find(query, Movie.class);
		if(movieList.size()==1) {
			return new ResponseEntity<Object>(movieList.get(0), HttpStatus.OK);
		}
		return new ResponseEntity<Object>(null, HttpStatus.NOT_FOUND);
	}

	@PostConstruct
	private void initDataSet() {
		mongoTemplate.dropCollection(Movie.class);
		mongoTemplate.save(new Movie("starwars", "Star Wars"));
		mongoTemplate.save(new Movie("indianajones", "Indiana Jones"));
	}
}
