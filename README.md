# kafka-view-composer-example 

* The project is part of the [article](https://medium.com/@psaw.mora/kafka-stream-processing-composing-views-by-example-de1a8f031940) on using Kafka Stream DSL and Processor API.

## Instruction To Build
* Clone the project
* Run ` ./gradlew clean buildImage`
* Run ` docker-compose up`
* The docker-compose will setup a one broker Kafka test cluster and provision Kafka-Rest-Proxy and Kafka-Topic-UI along with out sample project.
* The application runs both DSL and Processor API versions at once.
* Processor API version final topic is - composed_view_doctor_and_latest_appointment_processor_api
* DSL version final topic is - composed_view_doctor_and_latest_appointment_processor_api
* Once everything is up and running use the [sample](src/test/java/com/psaw/kafka/stream/app/integration/SampleEventPublisher.java) to publish some events and check the output.

## DSL and Processor API versions
* [Kafka Stream API DSL version](src/main/java/com/psaw/kafka/stream/app/DoctorPatientViewComposerAppWithDsl.java)
* [Kafka Stream Processor API version](src/main/java/com/psaw/kafka/stream/app/DoctorPatientViewComposerAppWithProcessorApi.java)

