package com.example;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;

import reactor.core.publisher.Flux;
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
}
