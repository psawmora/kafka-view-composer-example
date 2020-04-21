package com.psaw.kafka.stream.domain.entity;

import lombok.*;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * <p>
 * <code>{@link Appointment}</code> -
 * Basic domain entity definition representing an Appointment.
 * </p>
 */
@Getter
@Setter
@ToString
@Builder
@AllArgsConstructor
public class Appointment implements Serializable, Comparable<Appointment> {

    public Appointment() {
    }

    private String id;
    private boolean isCompleted;
    private Instant appointmentDate;
    private String patientId;
    private String doctorId;
    private String locationDetail;
    private Instant createdTimestamp;

    @Override
    public int compareTo(Appointment other) {
        if (other.id.equals(this.id)) {
            return 0;
        }
        if(other.appointmentDate != null && this.appointmentDate != null){
            return this.appointmentDate.compareTo(other.appointmentDate);
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Appointment that = (Appointment) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
