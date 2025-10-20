# UDP Blast File Transfer Project

## Step 1: Build
$ make

## Step 2: Prepare Files
$ dd if=/dev/urandom of=test_100kb.bin bs=1024 count=100
$ dd if=/dev/urandom of=test_1000kb.bin bs=1024 count=1000

## Step 3: Run Receiver
$ mkdir -p outdir
$ ./receiver 9000 outdir

## Step 4: Run Manually (Single Run Example)
### Without Garbler:
$ ./sender 127.0.0.1 9000 test_100kb.bin 1024 500

### With Garbler:
(terminal 1)
$ ./receiver 9000 outdir
(terminal 2)
$ ./garbler 10000 127.0.0.1 9000 10
(terminal 3)
$ ./sender 127.0.0.1 10000 test_100kb.bin 1024 500

## Step 5: Run Automated Experiment
Run all loss values automatically and store results in CSV:
$ python3 experiment.py

Output: results/results.csv containing throughput, loss events, and retransmits.

## Step 6: Plot Throughput vs Loss
You can easily plot with Excel or matplotlib using the CSV.
