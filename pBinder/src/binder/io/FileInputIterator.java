package binder.io;

import binder.runner.Config;
import de.metanome.algorithm_integration.input.InputIterationException;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class FileInputIterator implements InputIterator {

    private final RelationalFileInput inputGenerator;
    private final int inputRowLimit;
    private List<String> record = null;
    private int rowsRead = 0;

    public FileInputIterator(String relationName, Config config, int inputRowLimit) throws InputIterationException, FileNotFoundException {
        this.inputGenerator = new RelationalFileInput(relationName, new BufferedReader(new FileReader(config.inputFolderPath + config.databaseName + relationName)), config);
        this.inputRowLimit = inputRowLimit;
    }

    @Override
    public boolean next() throws InputIterationException {
        if (this.inputGenerator.hasNext() && ((this.inputRowLimit <= 0) || (this.rowsRead < this.inputRowLimit))) {
            List<String> input = this.inputGenerator.next();
            this.record = new ArrayList<>(input.size());
            for (String value : input) {
                // Replace line breaks with the zero-character, because these line breaks would otherwise split values when later written to plane-text buckets
                if (value != null) {
                    value = value.replaceAll("\n", "\0");
                }
                this.record.add(value);
            }

            this.rowsRead++;
            return true;
        }
        return false;
    }

    @Override
    public String getValue(int columnIndex) {
        return this.record.get(columnIndex);
    }

    @Override
    public List<String> getValues() {
        return this.record;
    }

    @Override
    public void close() throws Exception {
        this.inputGenerator.close();
    }
}
