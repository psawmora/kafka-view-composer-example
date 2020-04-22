package com.psaw.kafka.stream.app;

import com.psaw.kafka.stream.domain.entity.Appointment;
import com.psaw.kafka.stream.domain.entity.Doctor;
import com.psaw.kafka.stream.util.serde.JsonPOJOSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p></p>
 */
@ContextConfiguration(classes = {DoctorPatientViewComposerAppWithDslTest.TestConfiguration.class})
public class DoctorPatientViewComposerAppWithDslTest extends AbstractTestNGSpringContextTests {

    @org.springframework.boot.test.context.TestConfiguration
//    @ComponentScan(basePackages = {"com.psaw.kafka.stream.conf"})
//    @Import(value = {DoctorPatientViewComposerAppWithDsl.class})
//    @Import(value = {DoctorPatientViewComposerAppWithProcessorApi.class})
    static class TestConfiguration{}

    @BeforeMethod
    public void setUp() {
    }

    @AfterMethod
    public void tearDown() {
    }

    protected String doctorTopic = "domain_entity_doctor";
    protected String appointmentTopic = "domain_entity_appointment";
    private JsonPOJOSerializer serializer = new JsonPOJOSerializer<>();

    @Test
    public void testOne() throws InterruptedException {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        KafkaProducer kafkaProducer = new KafkaProducer(props, Serdes.String().serializer(), serializer);

        Doctor doctor = Doctor.builder()
                .id("doctor-1")
                .isGeneralPhysician(false)
                .name("doctor-name-1")
                .build();

        ProducerRecord doctorRecord = new ProducerRecord(doctorTopic, doctor.getId(), doctor);
        Future future = kafkaProducer.send(doctorRecord);

        Appointment appointment = Appointment.builder()
                .appointmentDate(Instant.now().plus(7, ChronoUnit.DAYS))
                .createdTimestamp(Instant.now())
                .id("appt-id-3")
                .doctorId(doctor.getId())
                .locationDetail("clinic-1")
                .isCompleted(false)
                .patientId("patient-1").build();

        ProducerRecord appointmentRecord = new ProducerRecord(appointmentTopic, appointment.getId(), appointment);
//        kafkaProducer.send(appointmentRecord);

        Appointment appointment2 = Appointment.builder()
                .appointmentDate(Instant.now().plus(7, ChronoUnit.DAYS))
                .createdTimestamp(Instant.now())
                .id("appt-id-4")
                .doctorId(doctor.getId())
                .locationDetail("clinic-1")
                .isCompleted(false)
                .patientId("patient-1").build();

        ProducerRecord appointmentRecord2 = new ProducerRecord(appointmentTopic, appointment2.getId(), appointment2);
//        ProducerRecord appointmentRecord2 = new ProducerRecord(appointmentTopic, appointment2.getId(), null);
//        kafkaProducer.send(appointmentRecord2);

        AtomicInteger counter = new AtomicInteger(0);
/*
        for (int i = 5; i < 100; i++) {
            appointment2.setId("appt-id-" + i);
            appointment2.setAppointmentDate(Instant.now().plus(6, ChronoUnit.DAYS).minus((7 + i) * 1000, ChronoUnit.MILLIS));
            ProducerRecord appointmentRecordx = new ProducerRecord(appointmentTopic, appointment2.getId(), appointment2);
            kafkaProducer.send(appointmentRecordx, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception != null){
                        System.out.println(exception.toString());
                    }else {
                        System.out.println("Sent Count : " + counter.incrementAndGet());
                    }
                }
            });
        }
*/

//     Thread.sleep(10000);

/*
        for (int i = 91; i < 100; i++) {
            appointment2.setId("appt-id-" + i);
            ProducerRecord appointmentRecordx = new ProducerRecord(appointmentTopic, appointment2.getId(), null);
            kafkaProducer.send(appointmentRecordx, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception != null){
                        System.out.println(exception.toString());
                    }else {
                        System.out.println("Sent Count : " + counter.incrementAndGet());
                    }
                }
            });
        }
*/
        appointment2.setId("appt-1");
        appointment2.setAppointmentDate(Instant.now().plus(5, ChronoUnit.DAYS));
        ProducerRecord appointmentRecordx = new ProducerRecord(appointmentTopic, appointment2.getId(), null);
        kafkaProducer.send(appointmentRecordx, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if(exception != null){
                    System.out.println(exception.toString());
                }else {
                    System.out.println("Sent Count : " + counter.incrementAndGet());
                }
            }
        });

        System.out.println("Done publishing");
        Thread.sleep(100000);
    }

    /*public static void main(String[] args) {
        TreeSet<Appointment> treeSet = new TreeSet<>();

        Appointment appointment1 = Appointment.builder()
                .appointmentDate(Instant.now().plus(7, ChronoUnit.DAYS))
                .createdTimestamp(Instant.now())
                .id("appt-id-1")
                .locationDetail("clinic-1")
                .isCompleted(false).build();


        Appointment appointment2 = Appointment.builder()
                .appointmentDate(Instant.now().plus(7, ChronoUnit.DAYS))
                .createdTimestamp(Instant.now())
                .id("appt-id-2")
                .locationDetail("clinic-1")
                .isCompleted(false).build();

        treeSet.add(appointment1);
        treeSet.add(appointment2);
        System.out.println(treeSet.size());
        treeSet.removeIf((value)-> value.getId().equals(appointment1.getId()));
        System.out.println(treeSet.size());
    }*/
}
