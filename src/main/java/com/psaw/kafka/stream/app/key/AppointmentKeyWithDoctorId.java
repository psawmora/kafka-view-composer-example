package com.psaw.kafka.stream.app.key;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * <p>
 * <code>{@link AppointmentKeyWithDoctorId}</code> -
 * Composite key
 * </p>
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
public class AppointmentKeyWithDoctorId implements Serializable {
    String appointmentId;
    String doctorId;
}
