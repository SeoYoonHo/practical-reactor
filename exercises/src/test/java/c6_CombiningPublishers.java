import org.junit.jupiter.api.*;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * In this important chapter we are going to cover different ways of combining publishers.
 * <p>
 * Read first:
 * <p>
 * https://projectreactor.io/docs/core/release/reference/#which.values
 * <p>
 * Useful documentation:
 * <p>
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
public class c6_CombiningPublishers extends CombiningPublishersBase {

    /**
     * Goal of this exercise is to retrieve e-mail of currently logged-in user.
     * `getCurrentUser()` method retrieves currently logged-in user
     * and `getUserEmail()` will return e-mail for given user.
     * <p>
     * No blocking operators, no subscribe operator!
     * You may only use `flatMap()` operator.
     */
    @Test
    public void behold_flatmap() {
        Hooks.enableContextLossTracking(); //used for testing - detects if you are cheating!

        //todo: feel free to change code as you need
        Mono<String> currentUserMono = getCurrentUser();
        Mono<String> currentUserEmail = currentUserMono.flatMap(user -> getUserEmail(user));

        //don't change below this line
        StepVerifier.create(currentUserEmail)
                    .expectNext("user123@gmail.com")
                    .verifyComplete();
    }

    /**
     * `taskExecutor()` returns tasks that should execute important work.
     * Get all the tasks and execute them.
     * <p>
     * Answer:
     * - Is there a difference between Mono.flatMap() and Flux.flatMap()?
     */
    @Test
    public void task_executor() {
        Flux<Void> tasks = taskExecutor().flatMap(voidMono -> voidMono);

        //don't change below this line
        StepVerifier.create(tasks)
                    .verifyComplete();

        Assertions.assertEquals(taskCounter.get(), 10);
    }

    /**
     * `streamingService()` opens a connection to the data provider.
     * Once connection is established you will be able to collect messages from stream.
     * <p>
     * Establish connection and get all messages from data provider stream!
     */
    @Test
    public void streaming_service() {
        Flux<Message> messageFlux = streamingService().flatMapMany(messageFlux1 -> messageFlux1);

        //don't change below this line
        StepVerifier.create(messageFlux)
                    .expectNextCount(10)
                    .verifyComplete();
    }


    /**
     * Join results from services `numberService1()` and `numberService2()` end-to-end.
     * First `numberService1` emits elements and then `numberService2`. (no interleaving)
     * <p>
     * Bonus: There are two ways to do this, check out both!
     */
    @Test
    public void i_am_rubber_you_are_glue() {
        Flux<Integer> numbers = Flux.concat(numberService1(), numberService2());
//        Flux<Integer> numbers = numbers = Flux.mergeSequential(numberService1(), numberService2());

        //don't change below this line
        StepVerifier.create(numbers)
                    .expectNext(1, 2, 3, 4, 5, 6, 7)
                    .verifyComplete();
    }

    /**
     * Similar to previous task:
     * <p>
     * `taskExecutor()` returns tasks that should execute important work.
     * Get all the tasks and execute each of them.
     * <p>
     * Instead of flatMap() use concatMap() operator.
     * <p>
     * Answer:
     * - What is difference between concatMap() and flatMap()?
     * answer : 순서대로, 병렬은 안됨 vs 순서보장X, 병렬처리 가능
     * - What is difference between concatMap() and flatMapSequential()?
     * answer : 순서대로, 병렬은 안됨 vs 순서대로, 병렬처리 가능
     * - Why doesn't Mono have concatMap() operator?
     * answer : 단일이기때문에 순서보장의 의미가 없음
     */
    @Test
    public void task_executor_again() {
        Flux<Void> tasks = taskExecutor().concatMap(voidMono -> voidMono);
//        Flux<Void> tasks = taskExecutor().flatMapSequential(voidMono -> voidMono);

        //don't change below this line
        StepVerifier.create(tasks)
                    .verifyComplete();

        Assertions.assertEquals(taskCounter.get(), 10);
    }

    /**
     * You are writing software for broker house. You can retrieve current stock prices by calling either
     * `getStocksGrpc()` or `getStocksRest()`.
     * Since goal is the best response time, invoke both services but use result only from the one that responds first.
     */
    @Test
    public void need_for_speed() {
        Flux<String> stonks = Flux.merge(getStocksGrpc(), getStocksRest())
                                  .take(5);

        //don't change below this line
        StepVerifier.create(stonks)
                    .expectNextCount(5)
                    .verifyComplete();
    }

    /**
     * As part of your job as software engineer for broker house, you have also introduced quick local cache to retrieve
     * stocks from. But cache may not be formed yet or is empty. If cache is empty, switch to a live source:
     * `getStocksRest()`.
     */
    @Test
    public void plan_b() {
        Flux<String> stonks = getStocksLocalCache().switchIfEmpty(getStocksRest());

        //don't change below this line
        StepVerifier.create(stonks)
                    .expectNextCount(6)
                    .verifyComplete();

        Assertions.assertTrue(localCacheCalled.get());
    }

    /**
     * You are checking mail in your mailboxes. Check first mailbox, and if first message contains spam immediately
     * switch to a second mailbox. Otherwise, read all messages from first mailbox.
     */
    @Test
    public void mail_box_switcher() {
        Flux<Message> myMail = mailBoxPrimary().handle((Message message, SynchronousSink<Message> messageSynchronousSink) -> {
                                                           if (message.metaData.contains("spam")) {
                                                               messageSynchronousSink.complete();
                                                           } else {
                                                               messageSynchronousSink.next(message);
                                                           }}
                                               ).concatWith(mailBoxSecondary());


        //don't change below this line
        StepVerifier.create(myMail)
                    .expectNextMatches(m -> !m.metaData.equals("spam"))
                    .expectNextMatches(m -> !m.metaData.equals("spam"))
                    .verifyComplete();

        Assertions.assertEquals(1, consumedSpamCounter.get());
    }

    /**
     * You are implementing instant search for software company.
     * When user types in a text box results should appear in near real-time with each keystroke.
     * <p>
     * Call `autoComplete()` function for each user input
     * but if newer input arrives, cancel previous `autoComplete()` call and call it for latest input.
     */
    @Test
    public void instant_search() {
        autoComplete(null);
        Flux<String> suggestions = userSearchInput().flatMapSequential(s -> autoComplete(s))
                                                    .takeLast(2);

        //don't change below this line
        StepVerifier.create(suggestions)
                    .expectNext("reactor project", "reactive project")
                    .verifyComplete();
    }


    /**
     * code should work, but it should also be easy to read and understand.
     * orchestrate file writing operations in a self-explanatory way using operators like `when`,`and`,`then`...
     * if all operations have been executed successfully return boolean value `true`.
     */
    @Test
    public void prettify() {
        Mono<Boolean> successful = openFile().then(writeToFile("0x3522285912341"))
                                             .then(closeFile())
                                             .then(Mono.just(true))
                                             .onErrorResume(error -> {
                                                 System.out.println(error);
                                                 return Mono.just(false);
                                             });

        //don't change below this line
        StepVerifier.create(successful)
                    .expectNext(true)
                    .verifyComplete();

        Assertions.assertTrue(fileOpened.get());
        Assertions.assertTrue(writtenToFile.get());
        Assertions.assertTrue(fileClosed.get());
    }

    /**
     * Before reading from a file we need to open file first.
     */
    @Test
    public void one_to_n() {
        Flux<String> fileLines = openFile().thenMany(readFile());

        StepVerifier.create(fileLines)
                    .expectNext("0x1", "0x2", "0x3")
                    .verifyComplete();
    }

    /**
     * Execute all tasks sequentially and after each task have been executed, commit task changes. Don't lose id's of
     * committed tasks, they are needed to further processing!
     */
    @Test
    public void acid_durability() {
        Flux<String> committedTasksIds = tasksToExecute().flatMapSequential(stringMono -> stringMono)
                                                         .doOnNext(s -> commitTask(s));

        //don't change below this line
        StepVerifier.create(committedTasksIds)
                    .expectNext("task#1", "task#2", "task#3")
                    .verifyComplete();

        Assertions.assertEquals(3, committedTasksCounter.get());
    }


    /**
     * News have come that Microsoft is buying Blizzard and there will be a major merger.
     * Merge two companies, so they may still produce titles in individual pace but as a single company.
     */
    @Test
    public void major_merger() {
        Flux<String> microsoftBlizzardCorp = Flux.merge(microsoftTitles().index(), blizzardTitles().index())
                                                 .sort(Comparator.comparingLong(o -> o.getT1()))
                                                 .map(objects -> objects.getT2());

        //don't change below this line
        StepVerifier.create(microsoftBlizzardCorp)
                    .expectNext("windows12", "wow2", "bing2", "overwatch3", "office366", "warcraft4")
                    .verifyComplete();
    }


    /**
     * Your job is to produce cars. To produce car you need chassis and engine that are produced by a different
     * manufacturer. You need both parts before you can produce a car.
     * Also, engine factory is located further away and engines are more complicated to produce, therefore it will be
     * slower part producer.
     * After both parts arrive connect them to a car.
     */
    @Test
    public void car_factory() {
        Flux<Car> producedCars = Flux.zip(carEngineProducer(), carChassisProducer(),
                (engine, chassis) -> new Car(chassis, engine));

        //don't change below this line
        StepVerifier.create(producedCars)
                    .recordWith(ConcurrentLinkedDeque::new)
                    .expectNextCount(3)
                    .expectRecordedMatches(cars -> cars.stream()
                                                       .allMatch(car -> Objects.equals(car.chassis.getSeqNum(),
                                                               car.engine.getSeqNum())))
                    .verifyComplete();
    }

    /**
     * When `chooseSource()` method is used, based on current value of sourceRef, decide which source should be used.
     */

    //only read from sourceRef
    AtomicReference<String> sourceRef = new AtomicReference<>("X");

    public Mono<String> chooseSource() {
        return Mono.defer(() -> {
            if (sourceRef.get()
                         .equals("A")) {
                return sourceA();
            } else if (sourceRef.get()
                                .equals("B")) {
                return sourceB();
            } else {
                return Mono.empty(); //otherwise, return empty
            }
        });
    }

    @Test
    public void deterministic() {
        //don't change below this line
        Mono<String> source = chooseSource();

        sourceRef.set("A");
        StepVerifier.create(source)
                    .expectNext("A")
                    .verifyComplete();

        sourceRef.set("B");
        StepVerifier.create(source)
                    .expectNext("B")
                    .verifyComplete();
    }

    /**
     * Sometimes you need to clean up after your self.
     * Open a connection to a streaming service and after all elements have been consumed,
     * close connection (invoke closeConnection()), without blocking.
     * <p>
     * This may look easy...
     */
    @Test
    public void cleanup() {
        BlockHound.install(); //don't change this line, blocking = cheating!

        Flux<String> stream = Flux.usingWhen(
                StreamingConnection.startStreaming(),       // 스트리밍 시작 (Mono<Flux<String>>)
                Flux::from,                                // 스트리밍 데이터 소비
                connection -> StreamingConnection.closeConnection() // 스트리밍 종료 및 정리
        );

        //don't change below this line
        StepVerifier.create(stream)
                    .then(() -> Assertions.assertTrue(StreamingConnection.isOpen.get()))
                    .expectNextCount(20)
                    .verifyComplete();
        Assertions.assertTrue(StreamingConnection.cleanedUp.get());
    }
}
