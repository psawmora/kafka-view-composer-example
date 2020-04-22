package com.psaw.kafka.stream.app.transformer;

import com.psaw.kafka.stream.app.DoctorPatientViewComposerAppWithDsl;
import com.psaw.kafka.stream.app.key.AppointmentKeyWithDoctorId;
import com.psaw.kafka.stream.domain.entity.Appointment;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * <p></p>
 */
public class AppointmentKeyToCompositeKeyTransformerSupplier {

    public static TransformerSupplier<String, Appointment, KeyValue<AppointmentKeyWithDoctorId, Appointment>> getTransformerSupplier(String tempStoreName) {
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
}
