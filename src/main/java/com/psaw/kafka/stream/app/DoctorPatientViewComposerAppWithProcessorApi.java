package com.psaw.kafka.stream.app;

import com.psaw.kafka.stream.app.key.AppointmentKeyWithDoctorId;
import com.psaw.kafka.stream.app.processor.AppointmentKeyMappingProcessorSupplier;
import com.psaw.kafka.stream.app.processor.DoctorAndAppointmentViewComposerProcessorSupplier;
import com.psaw.kafka.stream.conf.KafkaStreamConfigurationFactory;
import com.psaw.kafka.stream.domain.entity.Appointment;
import com.psaw.kafka.stream.domain.entity.Doctor;
import com.psaw.kafka.stream.domain.view.DoctorAndAppointmentView;
import com.psaw.kafka.stream.util.serde.JsonPOJODeserializer;
import com.psaw.kafka.stream.util.serde.JsonPOJOSerializer;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.*;

import static com.psaw.kafka.stream.app.processor.AppointmentKeyMappingProcessorSupplier.appointmentKeyModifierProcessor;
import static com.psaw.kafka.stream.app.processor.DoctorAndAppointmentViewComposerProcessorSupplier.appointmentsAndDoctorViewComposerProcessor;
import static com.psaw.kafka.stream.util.TopicAndStoreUtil.createTopic;
import static org.apache.kafka.common.utils.Utils.murmur2;
import static org.apache.kafka.common.utils.Utils.toPositive;
import static org.apache.kafka.streams.state.Stores.inMemoryKeyValueStore;

/**
 * <p>
 * <code>{@link DoctorPatientViewComposerAppWithProcessorApi}</code> -
 * Implementation using the Processor API.
 * </p>
 */
@Service
public class DoctorPatientViewComposerAppWithProcessorApi extends AbstractDoctorAppointmentViewComposerApp {

    public DoctorPatientViewComposerAppWithProcessorApi(
            @Qualifier("application-configuration-factory") KafkaStreamConfigurationFactory configurationFactory) {
        super(configurationFactory);
        this.appName = "doctor-appointment-view-composer-processor-api";
        this.viewOutputTopic = "composed_view_doctor_and_latest_appointment_processor_api";
    }

    @Override
    protected Topology buildStream() {
        String internalTopicName = "internal_appointment_repartitioned";
        createTopic(internalTopicName, doctorTopicPartitionCount, (short)1, appConfiguration);

        Topology topology = new Topology();
        topology.addSource("external-topic-source:" + appointmentTopic, Serdes.String().deserializer(), appointmentDeserializer, appointmentTopic)
                .addProcessor("appointment-key-modifier-with-doctor-id-processor",
                        () -> appointmentKeyModifierProcessor("temp_appointment_key_store"), "external-topic-source:" + appointmentTopic)
                .addSink("internal-appointment-repartitioning-sink", internalTopicName,
                        appointmentKeySerde.serializer(), appointmentValueSerde.serializer(), (topic, key, value, numPartitions) -> {
                            byte[] keyBytes = key.getDoctorId().getBytes();
                            return toPositive(murmur2(keyBytes)) % numPartitions;
                        }, "appointment-key-modifier-with-doctor-id-processor")
                .addSource("internal-topic-source:internal_appointment_repartitioned_topic", appointmentKeyDeserializer, appointmentDeserializer, internalTopicName)
                .addSource("external-topic-source:" + doctorTopic, Serdes.String().deserializer(), doctorDeserializer, doctorTopic)
                .addProcessor(
                        "appointments-of-doctor-calculating-processor",
                        () -> appointmentsAndDoctorViewComposerProcessor("doctor-appointment-view-store", maximumAppointmentsPerDoctor),
                        "internal-topic-source:internal_appointment_repartitioned_topic",
                        "external-topic-source:" + doctorTopic)
                .addSink(
                        "external-topic-sink:" + viewOutputTopic, viewOutputTopic,
                        Serdes.String().serializer(), viewSerializer, "appointments-of-doctor-calculating-processor");

        addStateStores(topology);
        return topology;
    }

    private void addStateStores(Topology topology) {
        final StoreBuilder<KeyValueStore<String, AppointmentKeyWithDoctorId>> tempAppointmentKeyStore =
                Stores.keyValueStoreBuilder(
                        inMemoryKeyValueStore("temp_appointment_key_store"), Serdes.String(), appointmentKeySerde);

        final StoreBuilder<KeyValueStore<String, DoctorAndAppointmentView>> doctorAppointmentViewStore =
                Stores.keyValueStoreBuilder(
                        inMemoryKeyValueStore("doctor-appointment-view-store"), Serdes.String(), viewValueSerde);

        topology.addStateStore(doctorAppointmentViewStore, "appointments-of-doctor-calculating-processor")
                .addStateStore(tempAppointmentKeyStore, "appointment-key-modifier-with-doctor-id-processor");
    }
}
