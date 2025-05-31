package binder.io;

import binder.runner.Config;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Deprecated
public class RelationalFileInput implements AutoCloseable{

    protected static final String DEFAULT_HEADER_STRING = "column";
    public List<String> headerLine;
    protected CSVReader CSVReader;
    protected List<String> nextLine;
    protected String relationName;
    protected int numberOfColumns = 0;
    // Initialized to -1 because of lookahead
    protected int currentLineNumber = -1;
    protected int numberOfSkippedLines = 0;

    protected boolean hasHeader;
    protected boolean skipDifferingLines;
    protected String nullValue;

    public RelationalFileInput(String relationName, Reader reader, Config setting) throws IOException {
        this.relationName = relationName;
        this.hasHeader = setting.hasHeader;
        this.skipDifferingLines = setting.skipDifferingLines;
        this.nullValue = setting.nullString;

        this.CSVReader = new CSVReaderBuilder(reader).withCSVParser(
                new CSVParserBuilder()
                        .withSeparator(setting.separator)
                        .withIgnoreLeadingWhiteSpace(setting.ignoreLeadingWhiteSpaces)
                        .withQuoteChar(setting.quoteChar)
                        .withStrictQuotes(setting.strictQuotes)
                        .withEscapeChar(setting.escapeChar)
                        .build()
        ).build();

        // read the first line
        this.nextLine = readNextLine();
        if (this.nextLine != null) {
            this.numberOfColumns = this.nextLine.size();
        }

        if (hasHeader) {
            this.headerLine = this.nextLine;
            next();
        }

        // If the header is still null generate a standard header the size of number of columns.
        if (this.headerLine == null) {
            this.headerLine = generateHeaderLine();
        }
    }

    public RelationalFileInput(String relationName, FileReader reader, Config config) throws IOException {
        this.relationName = relationName;

        this.hasHeader = config.hasHeader;
        this.skipDifferingLines = config.skipDifferingLines;
        this.nullValue = config.nullString;

        this.CSVReader = new CSVReaderBuilder(reader).withCSVParser(new CSVParserBuilder().withSeparator(config.separator).build()).build();

        // read the first line
        this.nextLine = readNextLine();
        if (this.nextLine != null) {
            this.numberOfColumns = this.nextLine.size();
        }

        if (hasHeader) {
            this.headerLine = this.nextLine;
            next();
        }

        // If the header is still null generate a standard header the size of number of columns.
        if (this.headerLine == null) {
            this.headerLine = generateHeaderLine();
        }
    }

    public boolean hasNext() {
        return !(this.nextLine == null);
    }

    public List<String> next() throws IOException {
        List<String> currentLine = this.nextLine;

        if (currentLine == null) {
            return null;
        }
        this.nextLine = readNextLine();

        if (this.skipDifferingLines) {
            readToNextValidLine();
        } else {
            failDifferingLine(currentLine);
        }

        return currentLine;
    }

    protected void failDifferingLine(List<String> currentLine) throws IOException {
        if (currentLine.size() != this.numberOfColumns()) {
            throw new IOException("Csv line length did not match on line " + currentLineNumber);
        }
    }

    protected void readToNextValidLine() throws IOException {
        if (!hasNext()) {
            return;
        }

        while (this.nextLine.size() != this.numberOfColumns()) {
            this.nextLine = readNextLine();
            this.numberOfSkippedLines++;
            if (!hasNext()) {
                break;
            }
        }
    }

    protected List<String> generateHeaderLine() {
        List<String> headerList = new ArrayList<>();
        for (int i = 1; i <= this.numberOfColumns; i++) {
            headerList.add(DEFAULT_HEADER_STRING + i);
        }
        return Collections.unmodifiableList(headerList);
    }

    protected List<String> readNextLine() throws IOException {
        String[] lineArray;
        try {
            lineArray = this.CSVReader.readNext();
            currentLineNumber++;
        } catch (IOException | CsvValidationException e) {
            throw new IOException("Could not read next line in file input", e);
        }
        if (lineArray == null) {
            return null;
        }
        // Convert empty Strings to null
        List<String> list = new ArrayList<>();
        for (String val : lineArray) {
            if (val.equals(this.nullValue)) {
                list.add(null);
            } else {
                list.add(val);
            }
        }
        // Return an immutable list
        return Collections.unmodifiableList(list);
    }

    @Override
    public void close() throws Exception {
        CSVReader.close();
    }

    public int numberOfColumns() {
        return numberOfColumns;
    }

    public String relationName() {
        return relationName;
    }

    public List<String> columnNames() {
        return headerLine;
    }

}