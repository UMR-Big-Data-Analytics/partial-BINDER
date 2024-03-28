# <ins>P</ins>artial <ins>B</ins>ucketing <ins>IN</ins>clusion <ins>D</ins>ependency <ins>E</ins>xtracto<ins>R</ins>

This repository is part of my [partial inclusion dependency repository](https://github.com/Jakob-L-M/partial-inclusion-dependencies). The code published here is based on Thorsten Papenbrock's [BINDER Algorithm](https://hpi.de/fileadmin/user_upload/fachgebiete/naumann/publications/2015/p559-papenbrock.pdf). In the directory [`📂BINDER`](./BINDER/) you will find the original implementation of BINDER. The directory [`📂pBINDER`](./pBINDER/) includes my rewritten version of BINDER, which also supports partial IND discovery.

## Key differences between BINDER and pBINDER
The **architecture** of pBINDER does not relay on [Metanome](https://github.com/HPI-Information-Systems/Metanome). While this detachment offers a more independent implementation, we lose the capability of easily executing the algorithm through a fort-end application, like the one [Metanome offers](https://github.com/HPI-Information-Systems/Metanome-Frontend).

As the name implies, pBINDER is capable of discovering **partial** INDs. Using a threshold $\rho$ it will find all pINDs where at least $\rho\%$ of values of the dependant side also occur in the referenced side.

**Null-Handling** is more flexible in the pBINDER implementation. The original version treated _NULL_ as a subset of everything else, with the option to detect foreign key INDs, being INDs where we require the referenced side to not have any _NULL_ entries. The new implementation expands these options by also offering an _EQUALITY_ and _INEQUALITY_ mode. In the _EQUALITY_ mode, all _NULL_ are equal to each other but unequal to all other values. In _INEQUALITY_ mode, a _NULL_ entries in unequal to all other entries, including other _NULL_ entries. For more information regarding these modes, refer to be thesis in the main repo.

Even though pBINDER solves a more complex problem and offers additional customization, it is **computationally more efficient** than the original version.

The **readability and quality** of the code has been improved by added vast amounts of comments, removing dead code, introducing new classes and splitting complex methods into smaller chunks.

Partial BINDER also uses an improved candidate generation and fixes the incorrect string concatenation, that BINDER uses in n-ary layers.

## Experimental results