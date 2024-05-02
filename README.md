This repository contains a set of tests that demonstrate how reactor's subscribeOn and publishOn methods affect the entire reactive chain both upstream and downstream.

You can run each test individually in ReactorSubscribeOnAndPublishDemoTest, and through the logs you can observe,
which threads each mapping operation is executed on dependent on the subscribeOn and publishOn calls used.