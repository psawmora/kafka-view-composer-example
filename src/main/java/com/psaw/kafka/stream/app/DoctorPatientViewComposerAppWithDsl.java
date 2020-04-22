package com.psaw.kafka.stream.app;

import com.psaw.kafka.stream.app.key.AppointmentKeyWithDoctorId;
import com.psaw.kafka.stream.app.transformer.AppointmentKeyToCompositeKeyTransformerSupplier;
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
import java.util.function.Predicate;

import static com.psaw.kafka.stream.app.transformer.AppointmentKeyToCompositeKeyTransformerSupplier.getTransformerSupplier;
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
        this.appName = "doctor-appointment-view-composer-dsl";
        this.viewOutputTopic = "composed_view_doctor_and_latest_appointment_dsl";
    }

    @Override
    protected Topology buildStream() {
        StreamsBuilder builder = new StreamsBuilder();
        String internalTopicName = "internal_repartitioned_domain_entity_appointment";
        createTopic(internalTopicName, doctorTopicPartitionCount, (short)1, appConfiguration);
        createStateStore(builder);

        KTable<String, Doctor> doctorKTable =
                builder.table(
                        doctorTopic,
                        Consumed.with(Serdes.String(), doctorValueSerde),
                        getStateStoreMaterialized("doctor-store", doctorValueSerde));

        KStream<String, Appointment> appointmentOriginalStream =
                builder.stream(appointmentTopic, Consumed.with(Serdes.String(), appointmentValueSerde));

        KStream<AppointmentKeyWithDoctorId, Appointment> appointmentStreamWithCompositeKey =
                appointmentOriginalStream.transform(
                        getTransformerSupplier("temp_appointment_key_store"), "temp_appointment_key_store");

        KStream<AppointmentKeyWithDoctorId, Appointment> repartitionedAppointmentStreamByDoctorId =
                appointmentStreamWithCompositeKey.through(
                        internalTopicName,
                        Produced.with(appointmentKeySerde, appointmentValueSerde)
                                .withStreamPartitioner((topic, key, value, numPartitions) -> {
                                    byte[] keyBytes = key.getDoctorId().getBytes();
                                    return toPositive(murmur2(keyBytes)) % numPartitions;
                                }));

        KStream<AppointmentKeyWithDoctorId, Appointment> deletedAppointmentMappedToDummyInstanceStream =
                repartitionedAppointmentStreamByDoctorId
                        .map(this::mapNullAppointmentsToDummyValue);

        Serde<TreeSet<Appointment>> appointmentSetSerde = Serdes.serdeFrom(genericSerializer, appointmentSetDeserializer);
        KTable<String, TreeSet<Appointment>> groupedAppointmentsByDoctorId =
                deletedAppointmentMappedToDummyInstanceStream
                        .groupBy((key, value) -> key.getDoctorId(), Grouped.with(Serdes.String(), appointmentValueSerde))
                        .aggregate(TreeSet::new,
                                (key, value, aggregate) -> updateAppointmentCollection(value, aggregate),
                                getStateStoreMaterialized("appointment-aggregate-store", appointmentSetSerde));

        doctorKTable
                .outerJoin(groupedAppointmentsByDoctorId,
                        (doctor, appointments) -> {
                            DoctorAndAppointmentView doctorAndAppointmentView = new DoctorAndAppointmentView();
                            if (doctor != null) {
                                doctorAndAppointmentView.setDoctor(doctor);
                            }
                            if (appointments != null) {
                                doctorAndAppointmentView.setActiveAppointments(appointments);
                            }
                            return doctorAndAppointmentView;
                        }, getStateStoreMaterialized("doctor-appointment-view-store", viewValueSerde ))
                .toStream()
                .to(viewOutputTopic, Produced.with(Serdes.String(), viewValueSerde));


        return builder.build();
    }

    private void createStateStore(StreamsBuilder builder) {
        final StoreBuilder<KeyValueStore<String, AppointmentKeyWithDoctorId>> tempAppointmentKeyStore =
                Stores.keyValueStoreBuilder(
                        inMemoryKeyValueStore("temp_appointment_key_store"), Serdes.String(), appointmentKeySerde);
        builder.addStateStore(tempAppointmentKeyStore);
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
        if (aggregate.size() > maximumAppointmentsPerDoctor) {
            Appointment last = aggregate.last();
            System.out.println("Size exceeded. Removing appointment : " + last + " | max size : " + maximumAppointmentsPerDoctor);
            aggregate.remove(last);
        }
        return aggregate;
    }
}
