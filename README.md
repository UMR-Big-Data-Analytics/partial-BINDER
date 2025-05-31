### 🧠 Partial BINDER – Partial Bucketing INclusion Dependency ExtractoR

The `pBINDER` algorithm builds on the [BINDER algorithm](https://hpi.de/fileadmin/user_upload/fachgebiete/naumann/publications/2015/p559-papenbrock.pdf), extending it to support **partial** IND discovery.  

**Partial IND Discovery:**
  Uses a threshold parameter `ρ` to detect inclusion dependencies that hold for at least `ρ%` of the values in the dependent attribute.

## 🚀 Installation

`pBINDER` is implemented in Java and built using Maven. To build the project, ensure the following tools are installed:

- Java JDK 21 or later
- Maven
- Git

### Steps to Build

1. Clone the repository and initialize submodules:


```bash
git submodule init
git submodule update
```

2. If not already done, clone and build the [Metanome repository](https://github.com/HPI-Information-Systems/Metanome). This provides required dependencies for the algorithms.

3. Build the algorithms using Maven to generate the JAR files.

## 🧪 Usage & Examples

You can run the algorithms in two ways:

1. **Via Metanome:**  
   Follow the instructions in the [Metanome repository](https://github.com/HPI-Information-Systems/Metanome) to run the algorithms within the Metanome framework.

2. **Standalone Execution:**  
   Each algorithm includes a `Runner` class that supports standalone execution either via an IDE or directly from the JAR file.  
   For experiments, we used shell scripts to execute the JAR files. These scripts are located in the `Utils` folder.

> **Note**: An older, non-Metanome version of this code exists, but it is deprecated and not recommended for use.
