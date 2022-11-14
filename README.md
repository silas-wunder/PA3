# PA3

PA3 is a basic page rank implementation designed for Apache Spark. Initially we tried a matrix multiplication approach before hitting memory issues with our Spark cluster, these files are called `PageRank.java` and `TaxedPageRank.java`.

We then switched to a random walk implementation that is slower but uses far less memory to run, these files are called `RWPageRank.java` and `RWTaxedPageRank.java`.

The taxed version of each implementation uses the formula $\beta Mv + (1 - \beta)e/n$, where $\beta = 0.85$, $M$ is the link matrix, $v$ is the ranks vector, $e$ is a vector of length $n$ of all $1$'s, and $n$ is the number of total number of pages.

This project also includes a wiki-bomb, similar in idea to a [google bomb](https://en.wikipedia.org/wiki/Google_bombing), written using python and found in `wikibomb.py`. The output can be found in `input/bombed_links.txt`. This link bomb was written in a manner to have the wikipedia page for "Rocky Mountain National Park" return the highest rank in a search for "surfing," counterintuitively.
