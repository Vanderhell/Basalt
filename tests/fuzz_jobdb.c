#include "jobdb.h"
#include <stdio.h>

/* Harness entry point for external mutational runners. The verifier is
   deliberately read-only and treats every byte in the supplied store as
   untrusted input. */
int main(int argc, char **argv) {
    if (argc != 2) return 2;
    return jobdb_verify(argv[1]) == JOBDB_OK ? 0 : 1;
}
