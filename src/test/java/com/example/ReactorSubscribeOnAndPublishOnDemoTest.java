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

  // The publishOn will only have an affect downstream. The upstream operations
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
  // the subscribeOn has NO effect.
  @Test
  void initial_publishOn_then_subscribeOn(){
    Flux<Integer> flux = Flux.range(1, 4)
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
}
