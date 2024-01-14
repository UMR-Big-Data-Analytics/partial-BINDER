package binder.io;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import binder.runner.Config;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import binder.io.RelationalFileInput;

public class FileInputIterator implements InputIterator {

	private RelationalFileInput inputGenerator;
	private List<String> record = null;
	
	private int rowsRead = 0;
	private final int inputRowLimit;
	
	public FileInputIterator(String relationName, Config config, int inputRowLimit) throws InputGenerationException, AlgorithmConfigurationException, InputIterationException, FileNotFoundException {
		this.inputGenerator = new RelationalFileInput(relationName, new BufferedReader(new FileReader(config.inputFolderPath + config.databaseName + relationName)), config);
		this.inputRowLimit = inputRowLimit;
	}

	@Override
	public boolean next() throws InputIterationException {
		if (this.inputGenerator.hasNext() && ((this.inputRowLimit <= 0) || (this.rowsRead < this.inputRowLimit))) {
			List<String> input = this.inputGenerator.next();
			this.record = new ArrayList<>(input.size());
			System.out.println(input);
			
			for (String value : input) {
				// Replace line breaks with the zero-character, because these line breaks would otherwise split values when later written to plane-text buckets
				if (value != null)
					value = value.replaceAll("\n", "\0");
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
