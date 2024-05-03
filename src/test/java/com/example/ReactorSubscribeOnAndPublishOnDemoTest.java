package com.example;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

@MicronautTest
class ReactorSubscribeOnAndPublishOnDemoTest {

  // A subscribeOn can be placed anywhere in the chain, and it will
  // affect the ENTIRE chain both upstream and downstream
  @Test
  void oneSubscribeOn(){
    Flux<Integer> flux = Flux.range(1, 4)
            .map(i -> {
              System.out.println("Map 1 = Number " + i + " on Thread " + Thread.currentThread().getName());
              return i;
            })
            .subscribeOn(Schedulers.boundedElastic())
            .map(i -> {
              System.out.println("Map 2 = Number " + i + " on Thread " + Thread.currentThread().getName());
              return i;
            });

    StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1, 2, 3, 4)
            .verifyComplete();
  }

  // The first upstream subscribeOn will take precedence,
  // and the second subscribe on will essentially be ignored
  @Test
  void twoSubscribeOns(){
    Flux<Integer> flux = Flux.range(1, 4)
            .subscribeOn(Schedulers.single())
            .map(i -> {
              System.out.println("Map 1 = Number " + i + " on Thread " + Thread.currentThread().getName());
              return i;
            })
            .subscribeOn(Schedulers.boundedElastic())
            .map(i -> {
              System.out.println("Map 2 = Number " + i + " on Thread " + Thread.currentThread().getName());
              return i;
            });

    StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1, 2, 3, 4)
            .verifyComplete();
  }

  // When flatMapping a Mono/Flux into the reactive chain with a subscribeOn
  // it will take effect unlike what we saw above where the second subscribeOn
  // did nothing.
  @Test
  void nestedSubscribeOns(){
    Flux<Integer> flux = Flux.range(1, 4)
            .subscribeOn(Schedulers.single())
            .map(i -> {
              System.out.println("Map 1 = Number " + i + " on Thread " + Thread.currentThread().getName());
              return i;
            })
            .flatMap(i -> Mono.just(i)
                    .subscribeOn(Schedulers.boundedElastic())
                    .doOnNext(x -> System.out.println("Map 2 = Number " + i + " on Thread " + Thread.currentThread().getName())));

    StepVerifier.create(flux)
            .expectSubscription()
            .expectNextCount(4)
            .verifyComplete();
  }

  // The publishOn will only have an effect downstream. The upstream operations
  // before it will be executed on the initial subscribing test thread.
  @Test
  void onePublishOn(){
    Flux<Integer> flux = Flux.range(1, 4)
            .map(i -> {
              System.out.println("Map 1 = Number " + i + " on Thread " + Thread.currentThread().getName());
              return i;
            })
            .publishOn(Schedulers.boundedElastic())
            .map(i -> {
              System.out.println("Map 2 = Number " + i + " on Thread " + Thread.currentThread().getName());
              return i;
            });

    StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1, 2, 3, 4)
            .verifyComplete();
  }

  // The first upstream publishOn will have a downstream effect on the first map,
  // but will be overwritten by the second publishOn for the second map. Essentially,
  // publishOn affects downstream until another publishOn is encountered.
  @Test
  void twoPublishOns(){
    Flux<Integer> flux = Flux.range(1, 4)
            .publishOn(Schedulers.single())
            .map(i -> {
              System.out.println("Map 1 = Number " + i + " on Thread " + Thread.currentThread().getName());
              return i;
            })
            .publishOn(Schedulers.boundedElastic())
            .map(i -> {
              System.out.println("Map 2 = Number " + i + " on Thread " + Thread.currentThread().getName());
              return i;
            });

    StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1, 2, 3, 4)
            .verifyComplete();
  }

  // The subscribeOn affects everything in the chain UNTIL the publishOn,
  // which then takes precedence for everything else downstream.
  @Test
  void initial_subscribeOn_then_publishOn(){
    Flux<Integer> flux = Flux.range(1, 4)
            .subscribeOn(Schedulers.single())
            .map(i -> {
              System.out.println("Map 1 = Number " + i + " on Thread " + Thread.currentThread().getName());
              return i;
            })
            .publishOn(Schedulers.boundedElastic())
            .map(i -> {
              System.out.println("Map 2 = Number " + i + " on Thread " + Thread.currentThread().getName());
              return i;
            });

    StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1, 2, 3, 4)
            .verifyComplete();
  }

  // The publishOn affects everything downstream, and in this case
  // the subscribeOn has NO effect after the publishOn, but it does
  // affect the initial map.
  @Test
  void initial_publishOn_then_subscribeOn(){
    Flux<Integer> flux = Flux.range(1, 4)
            .map(i -> {
              System.out.println("Map 1 = Number " + i + " on Thread " + Thread.currentThread().getName());
              return i;
            })
            .publishOn(Schedulers.single())
            .map(i -> {
              System.out.println("Map 1 = Number " + i + " on Thread " + Thread.currentThread().getName());
              return i;
            })
            .subscribeOn(Schedulers.boundedElastic())
            .map(i -> {
              System.out.println("Map 2 = Number " + i + " on Thread " + Thread.currentThread().getName());
              return i;
            });

    StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1, 2, 3, 4)
            .verifyComplete();
  }

  // Generally, publishOn only affects downstream, but when publishOn's argument
  // is a callable the callable will be affected as well. This is because reactor
  // detects this, and does a subscribeOn instead.
  // See this reactor code for a better understanding:
  // https://github.com/reactor/reactor-core/blob/d58d344252de96f1b5c47ad18f99fa7066a78f5c/reactor-core/src/main/java/reactor/core/publisher/Mono.java#L3243
  @Test
  void publishOn_after_callable(){

    Mono<String> mono = Mono.fromCallable(() -> {
      System.out.println("Callable is on Thread " + Thread.currentThread().getName());
      return "done";
    })
    .publishOn(Schedulers.boundedElastic())
    .map(s -> {
      System.out.println("Map is on Thread " + Thread.currentThread().getName());
      return s;
    });

    StepVerifier.create(mono)
            .expectSubscription()
            .expectNext("done")
            .verifyComplete();
  }

  // To prevent the callable from being affected by the downstream publishOn you can use
  // the hide() method. As you will see the callable will be executed on the initial
  // calling test thread instead.
  @Test
  void publishOn_after_callable_with_hide(){

    Mono<String> mono = Mono.fromCallable(() -> {
              System.out.println("Callable is on Thread " + Thread.currentThread().getName());
              return "done";
            })
            .hide()
            .publishOn(Schedulers.boundedElastic())
            .map(s -> {
              System.out.println("Map is on Thread " + Thread.currentThread().getName());
              return s;
            });

    StepVerifier.create(mono)
            .expectSubscription()
            .expectNext("done")
            .verifyComplete();
  }

  // SubscribeOn does as expected and affects the execution thread both upstream and downstream,
  // and hide() will have no effect.
  @Test
  void subscribeOn_after_callable(){

    Mono<String> mono = Mono.fromCallable(() -> {
              System.out.println("Callable is on Thread " + Thread.currentThread().getName());
              return "done";
            })
            .subscribeOn(Schedulers.boundedElastic())
            .map(s -> {
              System.out.println("Map is on Thread " + Thread.currentThread().getName());
              return s;
            });

    StepVerifier.create(mono)
            .expectSubscription()
            .expectNext("done")
            .verifyComplete();
  }

  // publishOn affects everything downstream, but ALSO the callable as there is a special case with
  // publish on if a callable is passed to it from directly upstream in the chain. SubscribeOn
  // has NO effect here.
  @Test
  void publishOn_after_callable_then_subscribeOn(){

    Mono<String> mono = Mono.fromCallable(() -> {
              System.out.println("Callable is on Thread " + Thread.currentThread().getName());
              return "done";
            })
            .publishOn(Schedulers.single())
            .map(s -> {
              System.out.println("Map 2 is on Thread " + Thread.currentThread().getName());
              return s;
            }).subscribeOn(Schedulers.boundedElastic());

    StepVerifier.create(mono)
            .expectSubscription()
            .expectNext("done")
            .verifyComplete();
  }

  // Since publishOn is not directly after the callable it will not affect the callable, or
  // anything else upstream. In this case publishOn only affects downstream as is typical,
  // but the subscribeOn now affects the upstream callable and map.
  @Test
  void publishOn_after_callable_and_map_then_subscribeOn(){

    Mono<String> mono = Mono.fromCallable(() -> {
              System.out.println("Callable is on Thread " + Thread.currentThread().getName());
              return "done";
            })
            .map(s -> {
              System.out.println("Map 2 is on Thread " + Thread.currentThread().getName());
              return s;
            })
            .publishOn(Schedulers.single())
            .map(s -> {
              System.out.println("Map 2 is on Thread " + Thread.currentThread().getName());
              return s;
            }).subscribeOn(Schedulers.boundedElastic());

    StepVerifier.create(mono)
            .expectSubscription()
            .expectNext("done")
            .verifyComplete();
  }
}
