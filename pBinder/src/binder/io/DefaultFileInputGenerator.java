package binder.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.FileInputGenerator;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;

public class DefaultFileInputGenerator implements FileInputGenerator {

    File inputFile;
    protected ConfigurationSettingFileInput setting;

    /**
     * @param setting the settings to construct new {@link de.metanome.algorithm_integration.input.RelationalInput}s
     *                with
     * @throws AlgorithmConfigurationException thrown if the file cannot be found
     */
    public DefaultFileInputGenerator(ConfigurationSettingFileInput setting)
            throws AlgorithmConfigurationException {
        try {
            this.setInputFile(new File(setting.getFileName()));
        } catch (FileNotFoundException e) {
            System.out.println(setting.getFileName());
            throw new AlgorithmConfigurationException("File not found!", e);
        }
        this.setting = setting;
    }

    @Override
    public RelationalFileInput generateNewCopy() throws InputGenerationException {
        try {
            return new RelationalFileInput(inputFile.getName(), new FileReader(inputFile), setting);
        } catch (FileNotFoundException e) {
            throw new InputGenerationException("File not found!", e);
        } catch (InputIterationException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * @return inputFile
     */
    @Override
    public File getInputFile() {
        return inputFile;
    }

    private void setInputFile(File inputFile) throws FileNotFoundException {
        if (inputFile.isFile()) {
            this.inputFile = inputFile;
        } else {
            throw new FileNotFoundException();
        }
    }

    /**
     * @return the setting
     */
    public ConfigurationSettingFileInput getSetting() {
        return this.setting;
    }

    @Override
    public void close() throws Exception {
        // Nothing to close
    }

}
