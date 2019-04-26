package movierating

import io.vertx.core.Context
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.http.HttpClientOptions
import io.vertx.core.http.HttpServerOptions
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.core.DeploymentOptions
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import io.vertx.core.http.*
import io.vertx.core.json.*
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import io.vertx.kotlin.coroutines.CoroutineVerticle
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import io.vertx.ext.web.client.WebClient
import movierating.App.Movie
import io.vertx.ext.web.codec.BodyCodec
import io.vertx.ext.web.client.HttpResponse
import io.vertx.core.buffer.Buffer
import io.vertx.ext.unit.Async

@RunWith(VertxUnitRunner::class)
class AppTest {

	@Rule
	@JvmField
	val rule = RunTestOnContext()

	private lateinit var vertx: Vertx
	private lateinit var client: WebClient

	@Before
	fun before() {
		vertx = rule.vertx()
		client = WebClient.create(vertx);
	}

	@Test
	fun `test getMovie API`(testContext: TestContext) {
		val resultBody: String = """{"id":"starwars","title":"Star Wars"}"""
		vertx.deployVerticle("movierating.App",
			DeploymentOptions().setWorker(true),
			testContext.asyncAssertSuccess {
				checkHttpRequest("/movie/starwars", resultBody, testContext.async())
			})
	}

	@Test
	fun `test getRating API`(testContext: TestContext) {
		val resultBody: String = """{"id":"starwars","getRating":6}"""
		vertx.deployVerticle("movierating.App",
			DeploymentOptions().setWorker(true),
			testContext.asyncAssertSuccess {
				checkHttpRequest("/getRating/starwars", resultBody, testContext.async())
			})
	}

	@Test
	fun `test rateMovie API`(testContext: TestContext) {
		val async: Async = testContext.async()
		vertx.deployVerticle("movierating.App",
			DeploymentOptions().setWorker(true),
			testContext.asyncAssertSuccess {
				client.post(8080, "localhost", "/rateMovie/starwars?getRating=50")
					.send({ ar ->
						assertTrue(ar.succeeded())
						var response: HttpResponse<Buffer> = ar.result();
						assertEquals(response.statusCode(), 200)
						async.complete()
					})
			})
	}

	fun checkHttpRequest(url: String, resultBody: String, async: Async) {
		client.get(8080, "localhost", url)
			.send({ ar ->
				assertTrue(ar.succeeded())
				var response: HttpResponse<Buffer> = ar.result();
				assertEquals(response.statusCode(), 200)
				var body = response.bodyAsJsonObject()
				assertEquals(body.toString(), resultBody)
				async.complete()
			})
	}

}