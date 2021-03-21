package mu.semte.ch.harvesting.filtering.lib.utils;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.impl.PropertyImpl;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.system.RiotLib;
import org.apache.jena.shacl.vocabulary.SHACLM;
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

  static Model replaceAnonNodes(Model model, String prefix){
    Model m = ModelFactory.createDefaultModel();
    model.listStatements().toList()
         .stream()
         .map(statement -> {
           var subject = statement.getSubject();
           var predicate = statement.getPredicate();
           var object = statement.getObject();
           if (subject.isAnon()) {
             subject = ResourceFactory.createResource(blankNodeToIriString(subject.asNode(), prefix));
           }
           if (predicate.isAnon()) {
             predicate = ResourceFactory.createProperty(blankNodeToIriString(predicate.asNode(), prefix));
           }
           if (object.isResource() && object.isAnon()) {
             object = ResourceFactory.createProperty(blankNodeToIriString(object.asNode(), prefix));
           }
           return ResourceFactory.createStatement(subject, predicate, object);
         })
         .forEach(m::add);
    return m;
  }

  static String blankNodeToIriString(Node node, String prefix) {
      if ( node.isBlank() ) {
        String x = node.getBlankNodeLabel();
        return prefix + x;
      }
    if (node.isURI())
      return node.getURI();
    throw new RiotException("Not a blank node or URI");
  }

}
