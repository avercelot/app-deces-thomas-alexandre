package fr.iiil.bigdata.actedeces.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ActeDeces implements Serializable {
    private String nomPrenom;
    private Integer sexe;
    private Integer dateNaissance;
    private String codeLieuNaissance;
    private String commune;
    private String pays;
    private Integer dateDeces;
    private String codeLieuDeces;
    private String numActe;

}


