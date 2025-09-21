.PHONY: all fetch analyze clean

# Default target (runs when you just type `make`)
all: analyze

fetch:
	@echo "=== Fetching METAR data for all airports... ==="
	./fetch_metars.sh
	@echo "=== Fetch step complete. Data saved in raw_metars/ ==="

analyze: fetch
	@echo "=== Analyzing METAR data and creating weather_report.csv ==="
	./analyze_data.sh
	@echo "=== Analysis complete. weather_report.csv generated successfully. ==="
	@echo "=== Pipeline completed successfully. ==="

clean:
	@echo "=== Cleaning up generated files and directories... ==="
	rm -rf raw_metars weather_report.csv
	@echo "=== Cleanup complete. Workspace is clean. ==="

