package binder.io;

import binder.runner.Config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileInputIterator {

    private final RelationalFileInput inputGenerator;
    private final int inputRowLimit;
    private List<String> record = null;
    private int rowsRead = 0;

    public FileInputIterator(String relationName, Config config, int inputRowLimit) throws IOException {
        this.inputGenerator = new RelationalFileInput(relationName, new BufferedReader(new FileReader(config.datasetFolder + config.datasetName + File.separator + relationName)), config);
        this.inputRowLimit = inputRowLimit;
    }

    public boolean next() throws IOException {
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

    public String getValue(int columnIndex) {
        return this.record.get(columnIndex);
    }

    public List<String> getValues() {
        return this.record;
    }

    public void close() throws IOException {
        this.inputGenerator.close();
    }
}
