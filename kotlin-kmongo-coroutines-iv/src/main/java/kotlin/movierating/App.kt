package movierating

import io.vertx.ext.web.Route
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.http.listenAwait
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch
import org.litote.kmongo.avg
import com.mongodb.reactivestreams.client.MongoDatabase
import com.mongodb.reactivestreams.client.internal.MongoClientImpl
import io.vertx.core.json.JsonObject
import org.litote.kmongo.coroutine.CoroutineClient
import org.litote.kmongo.coroutine.CoroutineCollection
import org.litote.kmongo.coroutine.CoroutineDatabase
import org.litote.kmongo.coroutine.aggregate
import org.litote.kmongo.coroutine.coroutine
import org.litote.kmongo.eq
import org.litote.kmongo.group
import org.litote.kmongo.json
import org.litote.kmongo.match
import org.litote.kmongo.reactivestreams.KMongo
import org.litote.kmongo.util.KMongoUtil
import io.vertx.ext.mongo.MongoClient


class App : CoroutineVerticle() {


    private lateinit var client: CoroutineClient
    private lateinit var movieCon: CoroutineCollection<Movie>
    private lateinit var rateCon: CoroutineCollection<Rate>

    data class Movie(val id: String, val title: String)
    data class Rate(val value: Int, val movieId: String)

    override suspend fun start() {

        client = KMongo.createClient().coroutine
        val database: CoroutineDatabase = client.getDatabase("test")

        movieCon = database.getCollection<Movie>()
        rateCon = database.getCollection<Rate>()
        movieCon.drop()
        rateCon.drop()
        movieCon.createIndex("{id : 1}")
        rateCon.createIndex("{movieId : 1}")
        movieCon.insertOne(Movie("starwars", "Star Wars"))
        movieCon.insertOne(Movie("indianajones", "Indiana Jones"))
        rateCon.insertOne(Rate(1, "starwars"))
        rateCon.insertOne(Rate(5, "starwars"))
        rateCon.insertOne(Rate(9, "starwars"))
        rateCon.insertOne(Rate(10, "starwars"))
        rateCon.insertOne(Rate(4, "indianajones"))
        rateCon.insertOne(Rate(7, "indianajones"))
        rateCon.insertOne(Rate(3, "indianajones"))
        rateCon.insertOne(Rate(9, "indianajones"))

        // Build Vert.x Web router
        val router = Router.router(vertx)
        router.get("/movie/:id").coroutineHandler { ctx -> getMovie(ctx) }
        router.post("/rateMovie/:id").coroutineHandler { ctx -> rateMovie(ctx) }
        router.get("/getRating/:id").coroutineHandler { ctx -> getRating(ctx) }

        // Start the server
        vertx.createHttpServer()
                .requestHandler(router)
                .listenAwait(config.getInteger("http.port", 8080))

    }

    // Send info about a movie
    suspend fun getMovie(ctx: RoutingContext) {
        val id = ctx.pathParam("id")
        val movie = movieCon.findOne(Movie::id eq id)
        val result = movie?.json
        if (!result.equals(KMongoUtil.EMPTY_JSON)) {
            ctx.response().end(result)
        } else {
            ctx.response().setStatusCode(404).end()
        }
    }

    // Rate a movie
    suspend fun rateMovie(ctx: RoutingContext) {

        val movieId = ctx.pathParam("id")
        val value = Integer.parseInt(ctx.queryParam("getRating")[0])

        val count: Long = movieCon.countDocuments("{id:'$movieId'}")
        if (count == 1L) {
            runCatching { rateCon.insertOne(Rate(value, movieId)) }
                    .onFailure { ctx.response().setStatusCode(404).end() }
                    .onSuccess { ctx.response().setStatusCode(200).end() }
        } else {
            ctx.response().setStatusCode(404).end()
        }

    }

    data class Result(val _id: String, val value: Int)

    // Get the current rating of a movie
    suspend fun getRating(ctx: RoutingContext) {
        val id = ctx.pathParam("id")

        val resultList: List<Result> = rateCon.aggregate<Result>(
                match(
                        Rate::movieId eq id
                ),
                group(
                        Rate::movieId,
                        Result::value avg Rate::value
                )
        ).toList()

        if (resultList.size == 1) {
            ctx.response().end(json {
                obj("id" to resultList.get(0)._id, "getRating" to resultList.get(0).value).encode()
            })
        } else {
            ctx.response().setStatusCode(404).end()
        }
    }

    /**
     * An extension method for simplifying coroutines usage with Vert.x Web routers
     */
    fun Route.coroutineHandler(fn: suspend (RoutingContext) -> Unit) {
        handler { ctx ->
            launch(ctx.vertx().dispatcher()) {
                try {
                    fn(ctx)
                } catch (e: Exception) {
                    ctx.fail(e)
                }
            }
        }
    }
}
