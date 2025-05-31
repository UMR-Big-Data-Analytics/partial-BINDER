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