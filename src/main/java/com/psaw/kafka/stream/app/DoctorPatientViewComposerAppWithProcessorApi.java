package com.psaw.kafka.stream.app;

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
    }

    protected JsonPOJOSerializer<TreeSet<Appointment>> appointmentSetSerializer= new JsonPOJOSerializer<>();
    protected JsonPOJODeserializer<AppointmentKeyWithDoctorId>
            appointmentKeyDeserializer = new JsonPOJODeserializer<>(AppointmentKeyWithDoctorId.class);
    private Serde<AppointmentKeyWithDoctorId>
            appointmentKeySerde = Serdes.serdeFrom(new JsonPOJOSerializer<>(), appointmentKeyDeserializer);

    protected JsonPOJODeserializer<DoctorAndAppointmentView> viewDeserializer = new JsonPOJODeserializer<>(DoctorAndAppointmentView.class);
    protected JsonPOJOSerializer<DoctorAndAppointmentView> viewSerializer = new JsonPOJOSerializer<>();
    private Serde<DoctorAndAppointmentView>
            viewValueSerde = Serdes.serdeFrom(viewSerializer, viewDeserializer);

    @Override
    protected Topology buildStream() {
        String internalTopicName = "internal_appointment_repartitioned";
        createTopic(internalTopicName, doctorTopicPartitionCount, (short)1, appConfiguration);

        final StoreBuilder<KeyValueStore<String, AppointmentKeyWithDoctorId>> tempAppointmentKeyStore =
                Stores.keyValueStoreBuilder(
                        inMemoryKeyValueStore("temp_appointment_key_store"), Serdes.String(), appointmentKeySerde);

        final StoreBuilder<KeyValueStore<String, DoctorAndAppointmentView>> doctorAppointmentViewStore =
                Stores.keyValueStoreBuilder(
                        inMemoryKeyValueStore("doctor-appointment-view-store"), Serdes.String(), viewValueSerde);

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
                        () -> appointmentsAndDoctorViewComposerProcessor("doctor-appointment-view-store"),
                        "internal-topic-source:internal_appointment_repartitioned_topic",
                        "external-topic-source:" + doctorTopic)
                .addSink(
                        "external-topic-sink:" + viewOutputTopic, viewOutputTopic,
                        Serdes.String().serializer(), viewSerializer, "appointments-of-doctor-calculating-processor");

        topology.addStateStore(doctorAppointmentViewStore, "appointments-of-doctor-calculating-processor")
                .addStateStore(tempAppointmentKeyStore, "appointment-key-modifier-with-doctor-id-processor");

        return topology;
    }

    private Processor<Object, Object> appointmentsAndDoctorViewComposerProcessor(String composedViewStoreName){
        return new Processor<Object, Object>() {

            private ProcessorContext context;
            private KeyValueStore<String, DoctorAndAppointmentView> composedViewStore;

            @Override
            public void init(ProcessorContext context) {
                this.composedViewStore = (KeyValueStore<String, DoctorAndAppointmentView>) context.getStateStore(composedViewStoreName);
                this.context = context;
            }

            @Override
            public void process(Object key, Object value) {
                if(value instanceof Appointment) {
                    updateViewWithAppointment((AppointmentKeyWithDoctorId)key, (Appointment)value);
                }
                if(value instanceof Doctor){
                    updateViewWithDoctor((String) key, (Doctor) value);
                }
            }

            private void updateViewWithDoctor(String key, Doctor doctor) {
                DoctorAndAppointmentView doctorAndAppointmentView = composedViewStore.get(key);
                if (doctorAndAppointmentView == null) {
                    doctorAndAppointmentView = new DoctorAndAppointmentView();
                    doctorAndAppointmentView.setActiveAppointments(new TreeSet<>());
                }
                doctorAndAppointmentView.setDoctor(doctor);
                composedViewStore.put(key, doctorAndAppointmentView);
            }

            private void updateViewWithAppointment(AppointmentKeyWithDoctorId key, Appointment appointment) {
                DoctorAndAppointmentView doctorAndAppointmentView = composedViewStore.get(key.getDoctorId());
                if (doctorAndAppointmentView == null) {
                    doctorAndAppointmentView = new DoctorAndAppointmentView();
                    doctorAndAppointmentView.setActiveAppointments(new TreeSet<>());
                }
                SortedSet<Appointment> activeAppointments = doctorAndAppointmentView.getActiveAppointments();
                if (appointment.getAppointmentDate() == null) {
                    activeAppointments.remove(appointment);
                } else {
                    activeAppointments.add(appointment);
                }
                if (activeAppointments.size() >= maximumAppointmentsPerDoctor) {
                    activeAppointments.remove(activeAppointments.last());
                }
                doctorAndAppointmentView.setActiveAppointments(activeAppointments);
                composedViewStore.put(key.getDoctorId(), doctorAndAppointmentView);
                context.forward(key.getDoctorId(), doctorAndAppointmentView);
            }

            @Override
            public void close() {

            }
        };
    }

    private Processor<String, Appointment> appointmentKeyModifierProcessor(String storeName){
        return new Processor<String, Appointment>() {

            private ProcessorContext context;
            private KeyValueStore<String, AppointmentKeyWithDoctorId> tempStore;

            @Override
            public void init(ProcessorContext context) {
                this.tempStore = (KeyValueStore<String, AppointmentKeyWithDoctorId>) context.getStateStore(storeName);
                this.context = context;
            }

            @Override
            public void process(String key, Appointment newAppointment) {
                if (newAppointment == null) {
                    handleEntityDeletion(key);
                } else {
                    handleEntityUpdate(key, newAppointment);
                }
            }

            private void handleEntityUpdate(String key, Appointment newAppointment) {
                AppointmentKeyWithDoctorId keyWithDoctorId = tempStore.get(key);
                if (keyWithDoctorId == null) {
                    keyWithDoctorId = new AppointmentKeyWithDoctorId(newAppointment.getId(), newAppointment.getDoctorId());
                    tempStore.put(key, keyWithDoctorId);
                }
                context.forward(keyWithDoctorId, newAppointment);
            }

            private void handleEntityDeletion(String key) {
                AppointmentKeyWithDoctorId keyWithDoctorId = tempStore.delete(key);
                if (keyWithDoctorId != null) {
                    context.forward(keyWithDoctorId, Appointment.builder().id(key)); // Delete marker
                }
            }

            @Override
            public void close() {

            }
        };
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class AppointmentKeyWithDoctorId implements Serializable{
        String appointmentId;
        String doctorId;
    }
}
