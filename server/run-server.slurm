#!/bin/bash
# SLURM version of the long run. Submit with:  sbatch run-server.slurm
# When the time limit ends the job, submit it again: it continues where it stopped.
#SBATCH --job-name=wayang-profiling
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --time=48:00:00
#SBATCH --output=slurm-%j.out

# module load java/17        # uncomment / adapt if the cluster provides Java as a module
cd "$SLURM_SUBMIT_DIR"
# Stop starting new configs 30 minutes before the job's time limit, so the last one can finish.
# Add --postgres local to include PostgreSQL (needs PostgreSQL installed on the node: ./setup-vm.sh).
./run-server.sh --hours 47.5
