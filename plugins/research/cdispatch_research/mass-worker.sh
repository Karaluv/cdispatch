#!/bin/bash
set -euo pipefail
root=$1
manifest=$2
index=$3
: "${CDISPATCH_RESEARCH_ENV:?Set the reviewed environment setup script}"
: "${CDISPATCH_MASS_DRIVER:?Set the application driver path}"
: "${CDISPATCH_MASS_PYTHON:?Set the application Python interpreter}"
source "$CDISPATCH_RESEARCH_ENV"
export OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_DYNAMIC=false OMP_DYNAMIC=false
export I_MPI_PIN=1 I_MPI_PIN_DOMAIN=core I_MPI_FABRICS=shm
cd "$root"
exec "$CDISPATCH_MASS_PYTHON" "$CDISPATCH_MASS_DRIVER" --root "$root" --manifest "$manifest" --index "$index"
