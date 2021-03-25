package mu.semte.ch.harvesting.valdiator.service;

import com.google.common.collect.ImmutableMap;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.harvesting.valdiator.lib.dto.DataContainer;
import mu.semte.ch.harvesting.valdiator.lib.dto.Task;
import mu.semte.ch.harvesting.valdiator.lib.utils.SparqlClient;
import mu.semte.ch.harvesting.valdiator.lib.utils.SparqlQueryStore;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.shacl.ValidationReport;
import org.apache.jena.shacl.engine.ShaclPaths;
import org.apache.jena.shacl.lib.ShLib;
import org.apache.jena.shacl.validation.ReportEntry;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.WorkbookUtil;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileOutputStream;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;
import static mu.semte.ch.harvesting.valdiator.Constants.LOGICAL_FILE_PREFIX;
import static mu.semte.ch.harvesting.valdiator.lib.utils.ModelUtils.formattedDate;
import static mu.semte.ch.harvesting.valdiator.lib.utils.ModelUtils.uuid;
import static org.apache.jena.shacl.engine.ShaclPaths.pathNode;

@Service
@Slf4j
public class XlsReportService {
  private final ShaclService shaclService;
  private final SparqlQueryStore queryStore;
  private final SparqlClient sparqlClient;
  private final TaskService taskService;

  @Value("${share-folder.path}")
  private String shareFolderPath;

  public XlsReportService(ShaclService shaclService,
                          SparqlQueryStore queryStore,
                          SparqlClient sparqlClient, TaskService taskService) {
    this.shaclService = shaclService;
    this.queryStore = queryStore;
    this.sparqlClient = sparqlClient;
    this.taskService = taskService;
  }

  public void writeReport(Task task, Model report, DataContainer fileContainer) {
    ValidationReport validationReport = shaclService.fromModel(report);
    Workbook workbook = new XSSFWorkbook();

    var groupedByPath = validationReport.getEntries()
                                        .stream()
                                        .collect(Collectors.groupingBy(reportEntry -> pathNode(reportEntry.resultPath()).getLocalName()));

    Sheet sheet = workbook.createSheet("Statistics");

    Row header = sheet.createRow(0);

    header.createCell(0).setCellValue("Property");
    header.createCell(1).setCellValue("Number of Occurences");

    var counter = new AtomicInteger(1);

    groupedByPath.forEach((key, list) -> {
      Row row = sheet.createRow(counter.getAndAdd(1));
      row.createCell(0).setCellValue(key);
      row.createCell(1).setCellValue(list.size());
      Sheet detailSheet = workbook.createSheet(WorkbookUtil.createSafeSheetName(key));

      Row detailHeader = detailSheet.createRow(0);
      detailHeader.createCell(0).setCellValue("Focus Node");
      detailHeader.createCell(1).setCellValue("Value");
      detailHeader.createCell(2).setCellValue("Result Message");
      detailHeader.createCell(3).setCellValue("Path");

      for (int i = 0; i < list.size(); i++) {
        Row detailRow = detailSheet.createRow(i + 1);
        ReportEntry reportEntry = list.get(i);

        detailRow.createCell(0).setCellValue(ShLib.displayStr(reportEntry.focusNode()));
        detailRow.createCell(1).setCellValue(ofNullable(reportEntry.value()).map(ShLib::displayStr).orElse(""));
        detailRow.createCell(2).setCellValue(ofNullable(reportEntry.message()).orElse(""));
        detailRow.createCell(3).setCellValue(ofNullable(reportEntry.resultPath())
                                                     .map(ShaclPaths::pathNode)
                                                     .map(ShLib::displayStr).orElse(""));

      }

    });
    autoSizeColumns(workbook);
    var dataContainer = fileContainer.toBuilder()
                                     .graphUri(writeFile(task.getGraph(), workbook))
                                     .build();
    taskService.appendTaskResultFile(task, dataContainer);
  }

  private void autoSizeColumns(Workbook workbook) {
    int numberOfSheets = workbook.getNumberOfSheets();
    for (int i = 0; i < numberOfSheets; i++) {
      Sheet sheet = workbook.getSheetAt(i);
      if (sheet.getPhysicalNumberOfRows() > 0) {
        Row row = sheet.getRow(sheet.getFirstRowNum());
        Iterator<Cell> cellIterator = row.cellIterator();
        while (cellIterator.hasNext()) {
          Cell cell = cellIterator.next();
          int columnIndex = cell.getColumnIndex();
          sheet.autoSizeColumn(columnIndex);
        }
      }
    }
  }

  @SneakyThrows
  private String writeFile(String graph, Workbook workbook) {
    var fileExtension = "xlsx";
    var logicalFileName = "report-statistics";
    var contentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
    var phyId = uuid();
    var phyFilename = "%s.%s".formatted(phyId, fileExtension);
    var path = "%s/%s".formatted(shareFolderPath, phyFilename);
    var physicalFile = "share://%s".formatted(phyFilename);
    var loId = uuid();
    var logicalFile = "%s/%s".formatted(LOGICAL_FILE_PREFIX, loId);
    var now = formattedDate(LocalDateTime.now());
    var file = new File(path);
    FileOutputStream outputStream = new FileOutputStream(file);
    workbook.write(outputStream);
    workbook.close();
    var fileSize = file.length();
    var queryParameters = ImmutableMap.<String, Object>builder()
            .put("graph", graph)
            .put("physicalFile", physicalFile)
            .put("logicalFile", logicalFile)
            .put("phyId", phyId)
            .put("phyFilename", phyFilename)
            .put("now", now)
            .put("fileSize", fileSize)
            .put("loId", loId)
            .put("logicalFileName", logicalFileName)
            .put("fileExtension", "nt")
            .put("contentType", contentType).build();

    var queryStr = queryStore.getQueryWithParameters("writeTtlFile", queryParameters);
    sparqlClient.executeUpdateQuery(queryStr);
    return logicalFile;
  }
}
