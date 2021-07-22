# ESTester Module
This module is used to run tests in parallel/serial which make requests to elasticsearch.

-	The tests in this module are meant to be run with the testRunner classes.
-	To run the tests, open the project in an IDE and run the testRunner classes after setting the mode for **ParallelComputer** to `(true, false)` or `(false, false)`. *Note: Running with `(true, true)` is not supported*.
- Some of the tests use a large amount of memory. So, you need to change the maximum allowed memory settings in `config/jvm.options` to `-Xmx4g` or `-Xmx8g` as you require.
