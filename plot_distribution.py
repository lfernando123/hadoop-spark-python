import matplotlib.pyplot as plt

def load_data(path):
    degrees = []
    counts = []
    with open(path) as f:
        for line in f:
            d, c = line.strip().split()
            degrees.append(int(d))
            counts.append(int(c))
    return degrees, counts

# Example
degrees, counts = load_data("results/livejournal2/part-r-00000")

plt.figure()
plt.scatter(degrees, counts, s=5)
plt.xscale("log")
plt.yscale("log")
plt.xlabel("In-degree")
plt.ylabel("Number of nodes")
plt.title("In-degree Distribution (LiveJournal2)")
plt.grid(True)
plt.show()