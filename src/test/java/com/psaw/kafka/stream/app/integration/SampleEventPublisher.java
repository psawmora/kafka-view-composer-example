package com.psaw.kafka.stream.app.integration;

import com.psaw.kafka.stream.domain.entity.Appointment;
import com.psaw.kafka.stream.domain.entity.Doctor;
import com.psaw.kafka.stream.util.serde.JsonPOJOSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 * <code>{@link SampleEventPublisher}</code> -
 * Showcases publishing Doctor and Appointment events.
 * </p>
 */
public class SampleEventPublisher {

    protected String doctorTopic = "domain_entity_doctor";
    protected String appointmentTopic = "domain_entity_appointment";
    private JsonPOJOSerializer serializer = new JsonPOJOSerializer<>();

    public static void main(String[] args) throws InterruptedException {
        SampleEventPublisher sampleEventPublisher = new SampleEventPublisher();
        sampleEventPublisher.sendDomainSnapshotEvents();
    }

    private void sendDomainSnapshotEvents() throws InterruptedException {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "0"); // Just for testing locally.
        KafkaProducer kafkaProducer = new KafkaProducer(props, Serdes.String().serializer(), serializer);

        CountDownLatch count = new CountDownLatch(3);

        Doctor doctor = createDoctor("doctor-1");
        kafkaProducer.send(new ProducerRecord(doctorTopic, doctor.getId(), doctor), (metadata, exception) -> count.countDown());

        Appointment appointment = createAppointment(doctor.getId(), "appt-id-1");
        kafkaProducer.send(
                new ProducerRecord(appointmentTopic, appointment.getId(), appointment),
                (metadata, exception) -> count.countDown());

        appointment = createAppointment(doctor.getId(), "appt-id-2");
        kafkaProducer.send(
                new ProducerRecord(appointmentTopic, appointment.getId(), appointment),
                (metadata, exception) -> count.countDown());

        System.out.println("Done publishing");
        count.await();
    }

    private Appointment createAppointment(String doctorId, String appointmentId) {
        return Appointment.builder()
                .id(appointmentId)
                .doctorId(doctorId)
                .appointmentDate(Instant.now().plus(7, ChronoUnit.DAYS))
                .createdTimestamp(Instant.now())
                .locationDetail("clinic-1")
                .isCompleted(false)
                .patientId("patient-1").build();
    }

    private Doctor createDoctor(String doctorId) {
        return Doctor.builder()
                    .id(doctorId)
                    .isGeneralPhysician(false)
                    .name("doctor-name-1")
                    .build();
    }
}
