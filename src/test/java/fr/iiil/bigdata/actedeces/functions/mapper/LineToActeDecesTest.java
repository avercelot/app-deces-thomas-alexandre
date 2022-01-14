package fr.iiil.bigdata.actedeces.functions.mapper;

import fr.iiil.bigdata.actedeces.bean.ActeDeces;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LineToActeDecesTest {
    private final String sampleLine = "BENAMEUR*HABIB/                                                                 11941020892352DEPARTEMENT D'ORAN                                          2021122401004262";
    private final String sampleLineIncomplete = "BENAMEUR*HABIB/                                                                 11941020892352                                          2021122401004262";

    @Test
    public void testConvertOneLine() {
        LineToActeDeces lineToActeDeces = new LineToActeDeces();
        ActeDeces acteDeces = lineToActeDeces.apply(sampleLine);

        assertThat(acteDeces.getNomPrenom()).isEqualTo("BENAMEUR*HABIB/");
        assertThat(acteDeces.getSexe()).isEqualTo(1);
        assertThat(acteDeces.getDateNaissance()).isEqualTo(19410208);
        assertThat(acteDeces.getCodeLieuNaissance()).isEqualTo("92352");
        assertThat(acteDeces.getCommune()).isEqualTo("DEPARTEMENT D'ORAN");
        assertThat(acteDeces.getPays()).isEqualTo("");
        assertThat(acteDeces.getDateDeces()).isEqualTo(20211224);
        assertThat(acteDeces.getCodeLieuDeces()).isEqualTo("01004");
        assertThat(acteDeces.getNumActe()).isEqualTo("262");
        assertThat(acteDeces).isEqualTo(
                ActeDeces.builder()
                        .nomPrenom("BENAMEUR*HABIB/")
                        .sexe(1)
                        .dateNaissance(19410208)
                        .codeLieuNaissance("92352")
                        .commune("DEPARTEMENT D'ORAN")
                        .pays("")
                        .dateDeces(20211224)
                        .codeLieuDeces("01004")
                        .numActe("262")
                        .build()
        );
    }

    @Test
    public void testConvertOneLineIncomplete() {
        LineToActeDeces lineToActeDeces = new LineToActeDeces();
        ActeDeces acteDeces = lineToActeDeces.apply(sampleLineIncomplete);

        assertThat(acteDeces).isEqualTo(
                ActeDeces.builder()
                        .nomPrenom("BENAMEUR*HABIB/")
                        .sexe(1)
                        .dateNaissance(19410208)
                        .codeLieuNaissance("92352")
                        .commune("")
                        .pays("")
                        .dateDeces(20211224)
                        .codeLieuDeces("01004")
                        .numActe("262")
                        .build()
        );
    }
}
