package com.psaw.kafka.stream.app.processor;

import com.psaw.kafka.stream.app.key.AppointmentKeyWithDoctorId;
import com.psaw.kafka.stream.domain.entity.Appointment;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * <p>
 * <code>{@link AppointmentKeyMappingProcessorSupplier}</code> -
 * Provides a utility method to create the Processor to map Appointment key to a composite key - {appointment_id, doctor_id}
 * </p>
 */
public class AppointmentKeyMappingProcessorSupplier {

    public static Processor<String, Appointment> appointmentKeyModifierProcessor(String storeName) {
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
                    context.forward(keyWithDoctorId, Appointment.builder().id(key).build());
                }
            }

            @Override
            public void close() {}
        };
    }


}
