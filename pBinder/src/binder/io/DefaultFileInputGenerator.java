package binder.io;

import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class DefaultFileInputGenerator {

    protected ConfigurationSettingFileInput setting;
    File inputFile;

    /**
     * @param setting the settings to construct new {@link de.metanome.algorithm_integration.input.RelationalInput}s
     *                with
     * @throws IOException thrown if the file cannot be found
     */
    public DefaultFileInputGenerator(ConfigurationSettingFileInput setting) throws IOException {
        this.setInputFile(new File(setting.getFileName()));
        this.setting = setting;
    }


    public RelationalFileInput generateNewCopy() throws IOException {
        return new RelationalFileInput(inputFile.getName(), new FileReader(inputFile), setting);
    }

    private void setInputFile(File inputFile) throws FileNotFoundException {
        if (inputFile.isFile()) {
            this.inputFile = inputFile;
        } else {
            throw new FileNotFoundException();
        }
    }

}
