package mu.semte.ch.harvesting.filtering.utils;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.atlas.lib.DateTimeUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.UUID;


public interface ModelUtils {

    Logger LOG = LoggerFactory.getLogger(ModelUtils.class);
    String CONTENT_TYPE_TURTLE = "text/turtle";

    static Model toModel(String value, String lang) {
        if (StringUtils.isEmpty(value)) throw new RuntimeException("model cannot be empty");
        return toModel(IOUtils.toInputStream(value, StandardCharsets.UTF_8), lang);
    }

    static String uuid() {
        return StringUtils.substring(UUID.randomUUID().toString(), 0, 32);
    }

    static String escapeDateTime(Date date) {
        String d = DateTimeUtils.calendarToXSDDateTimeString(dateToCalendar(date));
        return "\"%s\"^^xsd:dateTime ".formatted(d);
    }

    static String escapeNumber(long number) {
        return "\"%s\"^^xsd:integer".formatted(number);
    }

    static Calendar dateToCalendar(Date date) {
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        return calendar;
    }

    static boolean equals(Model firstModel, Model secondModel) {
        return firstModel.isIsomorphicWith(secondModel);
    }

    static Model difference(Model firstModel, Model secondModel) {
        return firstModel.difference(secondModel);
    }

    static Model intersection(Model firstModel, Model secondModel) {
        return firstModel.intersection(secondModel);
    }

    static Model toModel(InputStream is, String lang) {
        try (var stream = is) {
            Model graph = ModelFactory.createDefaultModel();
            graph.read(stream, "", lang);
            return graph;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Model toModel(InputStream is, Lang lang) {
        return toModel(is, lang.getName());
    }

    static Lang filenameToLang(String filename) {
        return RDFLanguages.filenameToLang(filename);
    }

    static Lang filenameToLang(String filename, Lang fallback) {
        return RDFLanguages.filenameToLang(filename, fallback);
    }

    static String getContentType(String lang) {
        return getRdfLanguage(lang).getContentType().getContentTypeStr();
    }

    static String getExtension(String lang) {
        return getRdfLanguage(lang).getFileExtensions().stream().findFirst().orElse("txt");
    }

    static Lang getRdfLanguage(String lang) {
        return RDFLanguages.nameToLang(lang);
    }

    static String toString(Model model, Lang lang) {
        StringWriter writer = new StringWriter();
        model.write(writer, lang.getName());
        return writer.toString();
    }

    static byte[] toBytes(Model model, Lang lang) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        model.write(bos, lang.getName());
        return bos.toByteArray();
    }
}
