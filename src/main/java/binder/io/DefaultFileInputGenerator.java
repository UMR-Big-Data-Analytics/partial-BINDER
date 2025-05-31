package binder.io;

import binder.runner.Config;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

@Deprecated
public class DefaultFileInputGenerator {

    protected Config config;
    File inputFile;

    /**
     * @param setting the settings to construct new {@link de.metanome.algorithm_integration.input.RelationalInput}s
     *                with
     * @throws IOException thrown if the file cannot be found
     */
    public DefaultFileInputGenerator(Config config, int tableId) throws IOException {
        this.setInputFile(new File(config.datasetFolder + config.datasetName + File.separator + config.relationNames[tableId] + config.fileEnding));
        this.config = config;
    }


    public RelationalFileInput generateNewCopy() throws IOException {
        return new RelationalFileInput(inputFile.getName(), new FileReader(inputFile), config);
    }

    private void setInputFile(File inputFile) throws FileNotFoundException {
        if (inputFile.isFile()) {
            this.inputFile = inputFile;
        } else {
            throw new FileNotFoundException();
        }
    }

}
