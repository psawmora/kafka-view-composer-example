package com.psaw.kafka.stream.domain.entity;

import lombok.*;

import java.io.Serializable;

/**
 * <p>
 * <code>{@link Doctor}</code> -
 * Basic domain entity definition representing a Doctor.
 * </p>
 */
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Doctor implements Serializable {
    private String id;
    private String name;
    private boolean isGeneralPhysician;
}
