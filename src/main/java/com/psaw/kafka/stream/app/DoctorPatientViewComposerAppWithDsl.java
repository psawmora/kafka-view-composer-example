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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.*;

import static com.psaw.kafka.stream.util.TopicAndStoreUtil.*;
import static org.apache.kafka.common.utils.Utils.murmur2;
import static org.apache.kafka.common.utils.Utils.toPositive;
import static org.apache.kafka.streams.state.Stores.inMemoryKeyValueStore;

/**
 * <p>
 * <code>{@link DoctorPatientViewComposerAppWithDsl}</code> -
 * Implementation with KStream and KTable.
 * </p>
 */
@Service
public class DoctorPatientViewComposerAppWithDsl extends AbstractDoctorAppointmentViewComposerApp {

    public DoctorPatientViewComposerAppWithDsl(
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
        StreamsBuilder builder = new StreamsBuilder();
        String internalTopicName = "internal_repartitioned_domain_entity_appointment";
        createTopic(internalTopicName, doctorTopicPartitionCount, (short)1, appConfiguration);

        final StoreBuilder<KeyValueStore<String, AppointmentKeyWithDoctorId>> tempAppointmentKeyStore =
                Stores.keyValueStoreBuilder(
                        inMemoryKeyValueStore("temp_appointment_key_store"), Serdes.String(), appointmentKeySerde);
        builder.addStateStore(tempAppointmentKeyStore);

        KTable<String, Doctor> doctorKTable = builder.table(doctorTopic,
                Consumed.with(Serdes.String(), doctorValueSerde),
                getStateStoreMaterialized("doctor-store", doctorValueSerde));

        Serde<TreeSet<Appointment>> appointmentSetSerde = Serdes.serdeFrom(appointmentSetSerializer, appointmentSetDeserializer);

        KTable<String, TreeSet<Appointment>> groupedAppointmentsByDoctorId = builder
                .stream(appointmentTopic, Consumed.with(Serdes.String(), appointmentValueSerde))
                .transform(getTransformerSupplier("temp_appointment_key_store"), "temp_appointment_key_store")
                .through(internalTopicName,
                        Produced.with(appointmentKeySerde, appointmentValueSerde)
                                .withStreamPartitioner((topic, key, value, numPartitions) -> {
                                    byte[] keyBytes = key.getDoctorId().getBytes();
                                    return toPositive(murmur2(keyBytes)) % numPartitions;
                                }))
                .map(new KeyValueMapper<AppointmentKeyWithDoctorId, Appointment, KeyValue<AppointmentKeyWithDoctorId,
                        Appointment>>() {
                    @Override
                    public KeyValue<AppointmentKeyWithDoctorId, Appointment> apply(AppointmentKeyWithDoctorId key,
                                                                                   Appointment value) {
                        return mapNullAppointmentsToDummyValue(key, value);
                    }
                })
                .groupBy(new KeyValueMapper<AppointmentKeyWithDoctorId, Appointment, String>() {
                    @Override
                    public String apply(AppointmentKeyWithDoctorId key, Appointment value) {
                        return key.getDoctorId();
                    }
                }, Grouped.with(Serdes.String(), appointmentValueSerde))
                .aggregate(
                        () -> new TreeSet<>(),
                        new Aggregator<String, Appointment, TreeSet<Appointment>>() {
                            @Override
                            public TreeSet<Appointment> apply(String key, Appointment value, TreeSet<Appointment> aggregate) {
                                return updateAppointmentCollection(value, aggregate);
                            }
                        }, getStateStoreMaterialized("appointment-aggregate-store", appointmentSetSerde));

        doctorKTable
                .outerJoin(groupedAppointmentsByDoctorId,
                        new ValueJoiner<Doctor, TreeSet<Appointment>, DoctorAndAppointmentView>() {
                            @Override
                            public DoctorAndAppointmentView apply(Doctor doctor, TreeSet<Appointment> appointments) {
                                DoctorAndAppointmentView doctorAndAppointmentView = new DoctorAndAppointmentView();
                                if (doctor != null) {
                                    doctorAndAppointmentView.setDoctor(doctor);
                                }
                                if (appointments != null) {
                                    doctorAndAppointmentView.setActiveAppointments(appointments);
                                }
                                return doctorAndAppointmentView;
                            }
                        }, getStateStoreMaterialized("doctor-appointment-view-store", viewValueSerde ))
                .toStream()
                .to(viewOutputTopic, Produced.with(Serdes.String(), viewValueSerde));


        return builder.build();
    }

    private KeyValue<AppointmentKeyWithDoctorId, Appointment> mapNullAppointmentsToDummyValue(AppointmentKeyWithDoctorId key, Appointment value) {
        Appointment appointment = value;
        if (appointment == null) {
            // Null values aren't aggregated. Thus we'll send a dummy appointment.
            appointment = new Appointment();
            appointment.setId(key.getAppointmentId());
        }
        return KeyValue.pair(key, appointment);
    }

    private TreeSet<Appointment> updateAppointmentCollection(Appointment value, TreeSet<Appointment> aggregate) {
        if (value.getAppointmentDate() == null) {
            aggregate.removeIf(appointment -> appointment.getId().equals(value.getId()));
        } else {
            aggregate.add(value);
        }
        if(aggregate.size() == maximumAppointmentsPerDoctor){
            Appointment last = aggregate.last();
            aggregate.remove(last);
        }
        return aggregate;
    }

    private TransformerSupplier<String, Appointment, KeyValue<AppointmentKeyWithDoctorId, Appointment>> getTransformerSupplier(String tempStoreName) {
        return new TransformerSupplier<String, Appointment, KeyValue<AppointmentKeyWithDoctorId, Appointment>>() {
            @Override
            public Transformer<String, Appointment, KeyValue<AppointmentKeyWithDoctorId, Appointment>> get() {
                return new Transformer<String, Appointment, KeyValue<AppointmentKeyWithDoctorId, Appointment>>() {
                    KeyValueStore<String, AppointmentKeyWithDoctorId> tempStore;

                    @Override
                    public void init(ProcessorContext context) {
                        tempStore = (KeyValueStore<String, AppointmentKeyWithDoctorId>) context.getStateStore(tempStoreName);
                    }

                    @Override
                    public KeyValue<AppointmentKeyWithDoctorId, Appointment> transform(String key, Appointment newAppointment) {
                        if (newAppointment == null) {
                            // Handling change-log event indicating a delete.
                            return handleEntityDeletion(key);
                        }
                        return handleEntityUpdate(key, newAppointment);
                    }

                    private KeyValue<AppointmentKeyWithDoctorId, Appointment> handleEntityUpdate(String key,
                                                                                                 Appointment newAppointment) {
                        AppointmentKeyWithDoctorId keyWithDoctorId = tempStore.get(key);
                        if(keyWithDoctorId == null){
                            keyWithDoctorId = new AppointmentKeyWithDoctorId(newAppointment.getId(), newAppointment.getDoctorId());
                            tempStore.put(key, keyWithDoctorId);
                        }
                        return KeyValue.pair(keyWithDoctorId, newAppointment);
                    }

                    private KeyValue<AppointmentKeyWithDoctorId, Appointment> handleEntityDeletion(String key) {
                        AppointmentKeyWithDoctorId keyWithDoctorId = tempStore.delete(key);
                        if (keyWithDoctorId == null) {
                            // No need to forward this as it cannot be repartitioned. Need to send to a dead-letter topic.
                            // For now just not forwarding.
                            return null;
                        }
                        return KeyValue.pair(keyWithDoctorId, null);
                    }

                    @Override
                    public void close() {

                    }
                };
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
