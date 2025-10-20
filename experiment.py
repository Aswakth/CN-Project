import os
import subprocess
import time
import csv
import pandas as pd
import matplotlib.pyplot as plt

# ----------------- Configuration -----------------
loss_rates = [0, 1, 5, 10, 20]

# Ask user for file size
size_input = input("Enter file size to test (100 for 100KB, 1000 for 1000KB): ").strip()
if size_input not in ["100", "1000"]:
    print("Invalid input. Choose 100 or 1000.")
    exit(1)
file = f"test_{size_input}kb.bin"
print(f"Running experiment for file: {file}")

os.makedirs("results", exist_ok=True)
csv_path = "results/results.csv"

# ----------------- Run Experiments -----------------
with open(csv_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["File", "Loss(%)", "Throughput(Mbps)", "LossEvents", "Retransmits"])

    for loss in loss_rates:
        print(f"\n=== Running for loss={loss}% ===")

        # Start garbler
        garbler = subprocess.Popen(
            ["./garbler", "10000", "127.0.0.1", "9000", str(loss)],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        time.sleep(1)  # give garbler time to start

        # Run sender
        try:
            res = subprocess.run(
                ["./sender", "127.0.0.1", "10000", file, "1024", "500"],
                capture_output=True, text=True, timeout=300  # increased timeout for large files
            )
        except subprocess.TimeoutExpired:
            print(f"⚠️ Sender timed out for loss={loss}%")
            garbler.terminate()
            continue

        # Flush garbler output (optional)
        garbler.terminate()
        time.sleep(0.5)

        # Process sender output
        lines = [line.strip() for line in res.stdout.strip().splitlines() if line.strip()]
        csv_lines = [line for line in lines if ',' in line]  # filter only CSV lines
        if not csv_lines:
            print(f"⚠️ Could not parse sender output:\n{res.stdout}")
            continue

        csv_line = csv_lines[-1]  # last CSV line
        parts = csv_line.split(",")
        if len(parts) >= 4:
            writer.writerow([file, loss, parts[1], parts[2], parts[3]])
            print(f"  -> Throughput: {parts[1]} Mbps, Retransmits: {parts[3]}")
        else:
            print("⚠️ Could not parse CSV line:", csv_line)

print("\n✅ All experiments completed! Results saved to results/results.csv")

# ----------------- Generate Graphs -----------------
df = pd.read_csv(csv_path)

# Throughput vs Loss
plt.figure()
plt.plot(df["Loss(%)"], df["Throughput(Mbps)"], marker="o", linewidth=2, label=file)
plt.title(f"Throughput vs Loss (%) for {file}")
plt.xlabel("Packet Loss (%)")
plt.ylabel("Throughput (Mbps)")
plt.grid(True)
plt.savefig(f"results/throughput_{file}.png", dpi=200)
plt.close()

# Retransmits vs Loss
plt.figure()
plt.plot(df["Loss(%)"], df["Retransmits"], marker="s", color="red", linewidth=2, label=file)
plt.title(f"Retransmits vs Loss (%) for {file}")
plt.xlabel("Packet Loss (%)")
plt.ylabel("Retransmissions")
plt.grid(True)
plt.savefig(f"results/retransmits_{file}.png", dpi=200)
plt.close()

print("✅ Graphs generated in 'results/' folder")
