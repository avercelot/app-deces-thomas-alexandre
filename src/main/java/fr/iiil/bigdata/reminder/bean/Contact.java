package fr.iiil.bigdata.reminder.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@NoArgsConstructor
@Data
@Builder
@AllArgsConstructor
public class Contact implements Serializable {
    private String raison;
    private String word;
    private long nb;
}
