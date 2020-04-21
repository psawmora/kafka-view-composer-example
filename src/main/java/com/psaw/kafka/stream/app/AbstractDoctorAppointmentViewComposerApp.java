package com.psaw.kafka.stream.app;

import com.fasterxml.jackson.core.type.TypeReference;
import com.psaw.kafka.stream.conf.KafkaStreamConfigurationFactory;
import com.psaw.kafka.stream.domain.entity.Appointment;
import com.psaw.kafka.stream.domain.entity.Doctor;
import com.psaw.kafka.stream.util.TopicAndStoreUtil;
import com.psaw.kafka.stream.util.serde.JsonPOJODeserializer;
import com.psaw.kafka.stream.util.serde.JsonPOJOSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Properties;
import java.util.TreeSet;

import static org.apache.kafka.common.serialization.Serdes.serdeFrom;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;

/**
 * <p>
 * <code>{@link AbstractDoctorAppointmentViewComposerApp}</code> -
 * Contains common attributes and behaviors.
 * </p>
 */
public abstract class AbstractDoctorAppointmentViewComposerApp {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${kafka.bootstrap.servers:127.0.0.1:9092}")
    private String bootstrapServers;

    protected String appName = "doctor-appointment-view-composer";

    protected String appointmentTopic = "domain_entity_appointment";

    protected String doctorTopic = "domain_entity_doctor";

    protected String viewOutputTopic = "view_doctor_and_latest_appointment";

    protected int appointmentTopicPartitionCount = 5;

    protected int doctorTopicPartitionCount = 4;

    protected int maximumAppointmentsPerDoctor = 10;

    protected final KafkaStreamConfigurationFactory configurationFactory;

    protected KafkaStreams streams;

    protected Properties appConfiguration;

    public AbstractDoctorAppointmentViewComposerApp(KafkaStreamConfigurationFactory configurationFactory) {
        this.configurationFactory = configurationFactory;
    }

    @PreDestroy
    public void cleanup() {
        logger.info("Stopping the stream application - [{}]", appName);
        streams.close();
    }

    protected JsonPOJOSerializer genericSerializer;
    protected JsonPOJODeserializer<Doctor> doctorDeserializer;
    protected JsonPOJODeserializer<Appointment> appointmentDeserializer;
    protected Serde<Doctor> doctorValueSerde ;
    protected Serde<Appointment> appointmentValueSerde;
    protected JsonPOJODeserializer<TreeSet<Appointment>> appointmentSetDeserializer;

    @PostConstruct
    public void init() {
        logger.info("Initializing the stream application [{}]", appName);
        this.appConfiguration = configurationFactory.builder()
                .configuration(APPLICATION_ID_CONFIG, appName)
                .configuration(NUM_STREAM_THREADS_CONFIG, 10)
                .build();
        try {
            this.createSerdes();
            TopicAndStoreUtil.createTopic(doctorTopic, doctorTopicPartitionCount, (short)1, appConfiguration);
            TopicAndStoreUtil.createTopic(appointmentTopic, appointmentTopicPartitionCount, (short)1, appConfiguration);
            TopicAndStoreUtil.createTopic(viewOutputTopic, 2, (short)1, appConfiguration);
            Topology topology = buildStream();
            logger.info("{} Topology ------------ \n\n", appName);
            logger.info(topology.describe().toString());
            logger.info("------------\n\n\n\n");
            this.streams = new KafkaStreams(topology, appConfiguration);
            this.streams.start();
        } catch (Throwable e) {
            logger.error("Error occurred while starting the Stream Application [{}] - [{}]", appName, e);
            throw e;
        }
    }

    private void createSerdes(){
        this.genericSerializer = new JsonPOJOSerializer<>();
        this.doctorDeserializer = new JsonPOJODeserializer<>(Doctor.class);
        this.appointmentDeserializer = new JsonPOJODeserializer<>(Appointment.class);

        TypeReference<TreeSet<Appointment>> treeSetOfAppointmentType = new TypeReference<TreeSet<Appointment>>(){};
        this.appointmentSetDeserializer = new JsonPOJODeserializer<>(treeSetOfAppointmentType);

        this.doctorValueSerde = serdeFrom(genericSerializer, doctorDeserializer);
        this.appointmentValueSerde = serdeFrom(genericSerializer, appointmentDeserializer);
    }

    protected abstract Topology buildStream();

}
