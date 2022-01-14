package fr.iiil.bigdata.actedeces.functions.mapper;

import fr.iiil.bigdata.actedeces.bean.ActeDeces;

import java.io.Serializable;
import java.util.function.Function;

public class LineToActeDeces implements Function<String, ActeDeces>, Serializable {
    @Override
    public ActeDeces apply(String line) {
        if (line == null || line.length() < 167) {
            return null;
        }
        ActeDeces acteDeces = ActeDeces.builder()
                .nomPrenom(line.substring(0,80).trim())
                .sexe(Integer.parseInt(line.substring(80,81)))
                .dateNaissance(Integer.parseInt(line.substring(81,89)))
                .codeLieuNaissance(line.substring(89,94))
                .commune(line.substring(94,124).trim())
                .pays(line.substring(124,154).trim())
                .dateDeces(Integer.parseInt(line.substring(154,162)))
                .codeLieuDeces(line.substring(162, 167))
                .numActe(line.substring(167, line.length()))
                .build();
        return acteDeces;
    }
}
