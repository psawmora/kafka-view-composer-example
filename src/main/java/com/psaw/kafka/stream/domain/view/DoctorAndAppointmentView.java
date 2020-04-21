package com.psaw.kafka.stream.domain.view;

import com.psaw.kafka.stream.domain.entity.Appointment;
import com.psaw.kafka.stream.domain.entity.Doctor;
import lombok.*;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

/**
 * <p>
 * <code>{@link DoctorAndAppointmentView}</code> -
 * A view of Doctor and his/her latest 10 active appointments.
 * </p>
 */
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class DoctorAndAppointmentView implements Serializable {
    private String id;
    private Doctor doctor;
    private SortedSet<Appointment> activeAppointments;
    private Instant createdTime;
}
