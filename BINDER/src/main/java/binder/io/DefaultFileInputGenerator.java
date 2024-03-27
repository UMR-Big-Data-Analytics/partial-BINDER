package binder.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.FileInputGenerator;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;

/**
 * Generator for {@link de.metanome.algorithm_integration.input.RelationalInput}s based on file
 * files. The setting holds all parameters to construct new {@link de.metanome.algorithm_integration.input.RelationalInput}s.
 * To adapt the parameters you have to adapt the setting.
 *
 * @author Jakob Zwiener
 */
public class DefaultFileInputGenerator implements FileInputGenerator {

    File inputFile;
    protected ConfigurationSettingFileInput setting;

    protected DefaultFileInputGenerator() {
    }

    /**
     * Creates a DefaultFileInputGenerator with default settings. The default setting is used to
     * construct a new {@link de.metanome.algorithm_integration.input.RelationalInput}.
     *
     * @param inputFile the file input file
     * @throws java.io.FileNotFoundException if the input file is not found
     */
    public DefaultFileInputGenerator(File inputFile) throws FileNotFoundException {
        this.setInputFile(inputFile);
        this.setting = new ConfigurationSettingFileInput(inputFile.getPath());
    }

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
            throw new AlgorithmConfigurationException("File not found!", e);
        }
        this.setting = setting;
    }

    public DefaultFileInputGenerator(File inputFile, ConfigurationSettingFileInput setting)
            throws AlgorithmConfigurationException, FileNotFoundException {
        try {
            this.setInputFile(inputFile);
        } catch (FileNotFoundException e) {
            throw new AlgorithmConfigurationException("File not found!", e);
        }
        this.setting = setting;
    }
    @Override
    public RelationalInput generateNewCopy() throws InputGenerationException {
        try {
            return new FileIterator(inputFile.getName(), new FileReader(inputFile), setting);
        } catch (FileNotFoundException e) {
            throw new InputGenerationException("File not found!", e);
        } catch (InputIterationException e) {
            throw new InputGenerationException("Could not iterate over the first line of the file input", e);
        }
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
