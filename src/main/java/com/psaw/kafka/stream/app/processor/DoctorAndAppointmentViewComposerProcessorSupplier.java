package com.psaw.kafka.stream.app.processor;

import com.psaw.kafka.stream.app.key.AppointmentKeyWithDoctorId;
import com.psaw.kafka.stream.domain.entity.Appointment;
import com.psaw.kafka.stream.domain.entity.Doctor;
import com.psaw.kafka.stream.domain.view.DoctorAndAppointmentView;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.SortedSet;
import java.util.TreeSet;

/**
 * <p>
 * <code>{@link DoctorAndAppointmentViewComposerProcessorSupplier}</code> -
 * Provides a utility method to create a Processor to compose a view of a Doctor and relevant Appointments.
 * </p>
 */
public class DoctorAndAppointmentViewComposerProcessorSupplier {

    public static Processor<Object, Object> appointmentsAndDoctorViewComposerProcessor(String composedViewStoreName,
                                                                                       int maximumAppointmentsPerDoctor) {
        return new Processor<Object, Object>() {
            private ProcessorContext context;
            private KeyValueStore<String, DoctorAndAppointmentView> composedViewStore;

            @Override
            public void init(ProcessorContext context) {
                this.composedViewStore =
                        (KeyValueStore<String, DoctorAndAppointmentView>) context.getStateStore(composedViewStoreName);
                this.context = context;
            }

            @Override
            public void process(Object key, Object value) {
                if (value instanceof Appointment) {
                    updateViewWithAppointment((AppointmentKeyWithDoctorId) key, (Appointment) value);
                }
                if (value instanceof Doctor) {
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
                context.forward(doctor.getId(), doctorAndAppointmentView);
            }

            private void updateViewWithAppointment(AppointmentKeyWithDoctorId key, Appointment newAppointment) {
                DoctorAndAppointmentView doctorAndAppointmentView = composedViewStore.get(key.getDoctorId());
                if (doctorAndAppointmentView == null) {
                    doctorAndAppointmentView = new DoctorAndAppointmentView();
                    doctorAndAppointmentView.setActiveAppointments(new TreeSet<>());
                }
                SortedSet<Appointment> activeAppointments = doctorAndAppointmentView.getActiveAppointments();
                if (newAppointment.getAppointmentDate() == null) {
                    activeAppointments.removeIf(appointment -> appointment.getId().equals(newAppointment.getId()));
                } else {
                    activeAppointments.add(newAppointment);
                }
                if (activeAppointments.size() > maximumAppointmentsPerDoctor) {
                    activeAppointments.remove(activeAppointments.last());
                }
                doctorAndAppointmentView.setActiveAppointments(activeAppointments);
                composedViewStore.put(key.getDoctorId(), doctorAndAppointmentView);
                context.forward(key.getDoctorId(), doctorAndAppointmentView);
            }

            @Override
            public void close() { }
        };
    }
}
